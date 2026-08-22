//! The NBD server qemu attaches to.
//!
//! Fixed-newstyle handshake over a unix socket, simple replies only. NBD was chosen over
//! iSCSI-to-localhost (a whole target stack for no gain) and over ublk (a kernel
//! dependency): it is qemu-native, needs no kernel module, and keeps the entire data
//! path in userspace where it can be killed and restarted without touching the host.
//!
//! The rule that keeps this from desynchronising: **a WRITE's payload is drained from the
//! socket before any error is reported.** Replying early leaves the guest's bytes sitting
//! in the stream to be parsed as the next request header, which produces a protocol
//! failure attributed to whatever came after it.

use std::io::{BufReader, BufWriter, Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};

use crate::err::{Error, Result};
use crate::vdisk::Vdisk;

const NBDMAGIC: u64 = 0x4e42_444d_4147_4943;
const IHAVEOPT: u64 = 0x4948_4156_454f_5054;
const REP_MAGIC: u64 = 0x0003_e889_0455_65a9;
const REQUEST_MAGIC: u32 = 0x2560_9513;
const SIMPLE_REPLY_MAGIC: u32 = 0x6744_6698;

const FLAG_FIXED_NEWSTYLE: u16 = 1;
const FLAG_NO_ZEROES: u16 = 2;

const OPT_EXPORT_NAME: u32 = 1;
const OPT_ABORT: u32 = 2;
const OPT_LIST: u32 = 3;
const OPT_INFO: u32 = 6;
const OPT_GO: u32 = 7;

const REP_ACK: u32 = 1;
const REP_SERVER: u32 = 2;
const REP_INFO: u32 = 3;
const REP_ERR_UNSUP: u32 = 0x8000_0001;
const REP_ERR_INVALID: u32 = 0x8000_0003;

const INFO_EXPORT: u16 = 0;
const INFO_BLOCK_SIZE: u16 = 3;

const TX_HAS_FLAGS: u16 = 1;
const TX_READ_ONLY: u16 = 2;
const TX_SEND_FLUSH: u16 = 4;
const TX_SEND_FUA: u16 = 8;
const TX_SEND_TRIM: u16 = 32;
const TX_SEND_WRITE_ZEROES: u16 = 64;

const CMD_READ: u16 = 0;
const CMD_WRITE: u16 = 1;
const CMD_DISC: u16 = 2;
const CMD_FLUSH: u16 = 3;
const CMD_TRIM: u16 = 4;
const CMD_CACHE: u16 = 5;
const CMD_WRITE_ZEROES: u16 = 6;

const CMD_FLAG_FUA: u16 = 1;

/// Refuse absurd read lengths before allocating for them. NBD's own recommended maximum
/// is 32 MiB; a client asking for more is confused or hostile, and either way this
/// process should not try to satisfy it.
const MAX_IO: u32 = 64 << 20;

/// What an NBD export reads and writes.
///
/// Two implementations: a vdisk this node owns, and a forwarder relaying to the node that
/// does. The NBD layer cannot tell them apart, which is the point -- the guest always
/// talks to its local Sidon, and whether that Sidon happens to own the disk is not the
/// guest's problem and not this file's either.
pub trait Backend: Send + Sync {
    fn size(&self) -> u64;
    fn read_only(&self) -> bool;
    fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>>;
    fn write(&self, offset: u64, data: &[u8]) -> Result<()>;
    fn flush(&self) -> Result<()>;
    fn write_zeroes(&self, offset: u64, len: u64) -> Result<()>;
}

/// A vdisk served by the node that owns it.
pub struct LocalVdisk(pub Arc<Mutex<Vdisk>>);

impl Backend for LocalVdisk {
    fn size(&self) -> u64 {
        self.0.lock().expect("vdisk mutex poisoned").size
    }
    fn read_only(&self) -> bool {
        self.0.lock().expect("vdisk mutex poisoned").class == "immutable"
    }
    fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        self.0.lock().expect("vdisk mutex poisoned").read(offset, len)
    }
    fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        self.0.lock().expect("vdisk mutex poisoned").write(offset, data)
    }
    fn flush(&self) -> Result<()> {
        self.0.lock().expect("vdisk mutex poisoned").flush()
    }
    fn write_zeroes(&self, offset: u64, len: u64) -> Result<()> {
        self.0.lock().expect("vdisk mutex poisoned").write_zeroes(offset, len)
    }
}

pub struct Export {
    pub backend: Arc<dyn Backend>,
    pub name: String,
}

fn read_exact<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<()> {
    r.read_exact(buf).map_err(|e| Error::io(format!("nbd read: {e}")))
}

fn read_u16<R: Read>(r: &mut R) -> Result<u16> {
    let mut b = [0u8; 2];
    read_exact(r, &mut b)?;
    Ok(u16::from_be_bytes(b))
}

fn read_u32<R: Read>(r: &mut R) -> Result<u32> {
    let mut b = [0u8; 4];
    read_exact(r, &mut b)?;
    Ok(u32::from_be_bytes(b))
}

fn read_u64<R: Read>(r: &mut R) -> Result<u64> {
    let mut b = [0u8; 8];
    read_exact(r, &mut b)?;
    Ok(u64::from_be_bytes(b))
}

fn transmission_flags(read_only: bool) -> u16 {
    let mut f = TX_HAS_FLAGS | TX_SEND_FLUSH | TX_SEND_FUA | TX_SEND_TRIM | TX_SEND_WRITE_ZEROES;
    if read_only {
        f |= TX_READ_ONLY;
    }
    f
}

fn send_option_reply<W: Write>(
    w: &mut W,
    option: u32,
    rep_type: u32,
    payload: &[u8],
) -> Result<()> {
    let mut head = Vec::with_capacity(20 + payload.len());
    head.extend_from_slice(&REP_MAGIC.to_be_bytes());
    head.extend_from_slice(&option.to_be_bytes());
    head.extend_from_slice(&rep_type.to_be_bytes());
    head.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    head.extend_from_slice(payload);
    w.write_all(&head).map_err(|e| Error::io(format!("nbd write: {e}")))?;
    Ok(())
}

fn export_info_payload(size: u64, read_only: bool) -> Vec<u8> {
    let mut p = Vec::with_capacity(12);
    p.extend_from_slice(&INFO_EXPORT.to_be_bytes());
    p.extend_from_slice(&size.to_be_bytes());
    p.extend_from_slice(&transmission_flags(read_only).to_be_bytes());
    p
}

fn block_size_payload() -> Vec<u8> {
    // minimum / preferred / maximum. The minimum is 1 because the journal records byte
    // ranges, not sectors; the preferred 4 KiB matches what guests actually issue.
    let mut p = Vec::with_capacity(14);
    p.extend_from_slice(&INFO_BLOCK_SIZE.to_be_bytes());
    p.extend_from_slice(&1u32.to_be_bytes());
    p.extend_from_slice(&4096u32.to_be_bytes());
    p.extend_from_slice(&(MAX_IO).to_be_bytes());
    p
}

/// Serve one client connection to completion.
pub fn serve(stream: UnixStream, export: &Export) -> Result<()> {
    // Two handles on one socket: the reader blocks on the next request while the writer
    // is still flushing the previous reply. `try_clone` shares the underlying descriptor,
    // which is what makes that safe rather than merely convenient.
    let peer = stream.try_clone().map_err(|e| Error::io(format!("nbd socket clone: {e}")))?;
    let mut reader = BufReader::new(peer);
    let mut writer = BufWriter::new(stream);

    let (size, read_only) = (export.backend.size(), export.backend.read_only());

    // ---- handshake ---------------------------------------------------------------
    let mut hello = Vec::with_capacity(18);
    hello.extend_from_slice(&NBDMAGIC.to_be_bytes());
    hello.extend_from_slice(&IHAVEOPT.to_be_bytes());
    hello.extend_from_slice(&(FLAG_FIXED_NEWSTYLE | FLAG_NO_ZEROES).to_be_bytes());
    writer.write_all(&hello)?;
    writer.flush()?;

    let client_flags = read_u32(&mut reader)?;
    let no_zeroes = client_flags & FLAG_NO_ZEROES as u32 != 0;

    loop {
        let magic = read_u64(&mut reader)?;
        if magic != IHAVEOPT {
            return Err(Error::refused(format!(
                "nbd client sent option magic {magic:#x}, expected IHAVEOPT"
            )));
        }
        let option = read_u32(&mut reader)?;
        let len = read_u32(&mut reader)?;
        if len > (1 << 20) {
            return Err(Error::refused(format!("nbd option {option} payload of {len} bytes")));
        }
        let mut data = vec![0u8; len as usize];
        read_exact(&mut reader, &mut data)?;

        match option {
            OPT_EXPORT_NAME => {
                // Old-style: no reply header, straight to the export tuple.
                let mut resp = Vec::with_capacity(10 + 124);
                resp.extend_from_slice(&size.to_be_bytes());
                resp.extend_from_slice(&transmission_flags(read_only).to_be_bytes());
                if !no_zeroes {
                    resp.extend_from_slice(&[0u8; 124]);
                }
                writer.write_all(&resp)?;
                writer.flush()?;
                break;
            }
            OPT_GO | OPT_INFO => {
                // Payload: u32 name length, name, u16 count, then that many u16 requests.
                // The requests are advisory; sending EXPORT and BLOCK_SIZE unconditionally
                // is permitted and saves parsing a list we would answer the same way.
                if data.len() < 4 {
                    send_option_reply(&mut writer, option, REP_ERR_INVALID, b"short payload")?;
                    writer.flush()?;
                    continue;
                }
                send_option_reply(
                    &mut writer,
                    option,
                    REP_INFO,
                    &export_info_payload(size, read_only),
                )?;
                send_option_reply(&mut writer, option, REP_INFO, &block_size_payload())?;
                send_option_reply(&mut writer, option, REP_ACK, &[])?;
                writer.flush()?;
                if option == OPT_GO {
                    break;
                }
            }
            OPT_LIST => {
                let name = export.name.as_bytes();
                let mut p = Vec::with_capacity(4 + name.len());
                p.extend_from_slice(&(name.len() as u32).to_be_bytes());
                p.extend_from_slice(name);
                send_option_reply(&mut writer, option, REP_SERVER, &p)?;
                send_option_reply(&mut writer, option, REP_ACK, &[])?;
                writer.flush()?;
            }
            OPT_ABORT => {
                send_option_reply(&mut writer, option, REP_ACK, &[])?;
                writer.flush()?;
                return Ok(());
            }
            _ => {
                // Structured replies and metadata contexts land here. Declining them is
                // a supported answer; the client falls back to simple replies.
                send_option_reply(&mut writer, option, REP_ERR_UNSUP, &[])?;
                writer.flush()?;
            }
        }
    }

    // ---- transmission ------------------------------------------------------------
    loop {
        let magic = read_u32(&mut reader)?;
        if magic != REQUEST_MAGIC {
            return Err(Error::refused(format!(
                "nbd request magic {magic:#x} is not a request; the stream is desynchronised"
            )));
        }
        let cmd_flags = read_u16(&mut reader)?;
        let cmd_type = read_u16(&mut reader)?;
        let handle = read_u64(&mut reader)?;
        let offset = read_u64(&mut reader)?;
        let length = read_u32(&mut reader)?;

        if cmd_type == CMD_DISC {
            writer.flush().ok();
            return Ok(());
        }

        // A WRITE's payload belongs to this request whatever happens next.
        let payload = if cmd_type == CMD_WRITE {
            if length > MAX_IO {
                return Err(Error::refused(format!("nbd write of {length} bytes is out of range")));
            }
            let mut buf = vec![0u8; length as usize];
            read_exact(&mut reader, &mut buf)?;
            Some(buf)
        } else {
            None
        };

        let result: Result<Option<Vec<u8>>> = (|| {
            let backend = export.backend.as_ref();
            match cmd_type {
                CMD_READ => {
                    if length > MAX_IO {
                        return Err(Error::refused(format!(
                            "nbd read of {length} bytes is out of range"
                        )));
                    }
                    backend.read(offset, length).map(Some)
                }
                CMD_WRITE => {
                    let data = payload.as_ref().expect("read above");
                    backend.write(offset, data)?;
                    if cmd_flags & CMD_FLAG_FUA != 0 {
                        backend.flush()?;
                    }
                    Ok(None)
                }
                CMD_FLUSH => backend.flush().map(|_| None),
                CMD_TRIM | CMD_WRITE_ZEROES => {
                    backend.write_zeroes(offset, length as u64)?;
                    Ok(None)
                }
                // A cache hint we honour by doing nothing, which is a complete
                // implementation of a hint.
                CMD_CACHE => Ok(None),
                other => Err(Error::refused(format!("nbd command {other} is not supported"))),
            }
        })();

        match result {
            Ok(data) => {
                let mut head = Vec::with_capacity(16);
                head.extend_from_slice(&SIMPLE_REPLY_MAGIC.to_be_bytes());
                head.extend_from_slice(&0u32.to_be_bytes());
                head.extend_from_slice(&handle.to_be_bytes());
                writer.write_all(&head)?;
                if let Some(d) = data {
                    writer.write_all(&d)?;
                }
                writer.flush()?;
            }
            Err(e) => {
                eprintln!(
                    "sidon: nbd {} cmd={cmd_type} off={offset} len={length}: {e}",
                    export.name
                );
                let mut head = Vec::with_capacity(16);
                head.extend_from_slice(&SIMPLE_REPLY_MAGIC.to_be_bytes());
                head.extend_from_slice(&e.errno().to_be_bytes());
                head.extend_from_slice(&handle.to_be_bytes());
                writer.write_all(&head)?;
                writer.flush()?;
                // No payload follows an error reply, so the stream stays in sync and the
                // guest sees an I/O error on this request only.
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transmission_flags_advertise_what_is_implemented() {
        let f = transmission_flags(false);
        assert!(f & TX_HAS_FLAGS != 0);
        assert!(f & TX_SEND_FLUSH != 0);
        assert!(f & TX_SEND_TRIM != 0);
        assert!(f & TX_SEND_WRITE_ZEROES != 0);
        assert_eq!(f & TX_READ_ONLY, 0);
        // An immutable vdisk must announce itself read-only, or a guest will try to
        // write and only discover the refusal one EPERM at a time.
        assert!(transmission_flags(true) & TX_READ_ONLY != 0);
    }

    #[test]
    fn export_info_is_twelve_bytes_big_endian() {
        let p = export_info_payload(0x1000, false);
        assert_eq!(p.len(), 12);
        assert_eq!(u16::from_be_bytes([p[0], p[1]]), INFO_EXPORT);
        assert_eq!(
            u64::from_be_bytes([p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9]]),
            0x1000
        );
    }

    #[test]
    fn block_size_info_is_ordered_min_preferred_max() {
        let p = block_size_payload();
        assert_eq!(p.len(), 14);
        let min = u32::from_be_bytes([p[2], p[3], p[4], p[5]]);
        let pref = u32::from_be_bytes([p[6], p[7], p[8], p[9]]);
        let max = u32::from_be_bytes([p[10], p[11], p[12], p[13]]);
        assert!(min <= pref && pref <= max, "{min} {pref} {max}");
    }
}
