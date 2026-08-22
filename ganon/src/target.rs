//! What Ganon attacks: a block device or an NBD export, behind one trait.
//!
//! Scenarios never name their substrate. The same kill-and-verify run points at a DRBD
//! device on one invocation and a Sidon socket on the next, which is the only way a
//! verdict about Sidon means anything -- the harness has to be calibrated against an
//! implementation two decades of production says is correct before it is allowed to
//! judge new code.
//!
//! The NBD client here is written from the protocol specification, not from Sidon's
//! server. If both were derived from one shared module, a shared misreading of the spec
//! would be invisible to both.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

pub trait Block {
    fn size(&self) -> u64;
    fn read_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>, String>;
    fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<(), String>;
    fn flush(&mut self) -> Result<(), String>;
    fn describe(&self) -> String;
}

// ---------------------------------------------------------------------------------
// A plain device or file: the DRBD adapter.
// ---------------------------------------------------------------------------------

pub struct DeviceTarget {
    file: File,
    size: u64,
    path: String,
}

impl DeviceTarget {
    pub fn open(path: &Path) -> Result<Self, String> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| format!("cannot open {}: {e}", path.display()))?;
        // Block devices report zero length through metadata, so seek to the end instead:
        // it works for both a regular file and a device node.
        let mut probe = file.try_clone().map_err(|e| e.to_string())?;
        let size = probe.seek(SeekFrom::End(0)).map_err(|e| e.to_string())?;
        Ok(DeviceTarget { file, size, path: path.display().to_string() })
    }
}

impl Block for DeviceTarget {
    fn size(&self) -> u64 {
        self.size
    }
    fn read_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>, String> {
        let mut buf = vec![0u8; len];
        self.file.seek(SeekFrom::Start(offset)).map_err(|e| e.to_string())?;
        self.file.read_exact(&mut buf).map_err(|e| format!("read at {offset}: {e}"))?;
        Ok(buf)
    }
    fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<(), String> {
        self.file.seek(SeekFrom::Start(offset)).map_err(|e| e.to_string())?;
        self.file.write_all(data).map_err(|e| format!("write at {offset}: {e}"))
    }
    fn flush(&mut self) -> Result<(), String> {
        self.file.sync_data().map_err(|e| e.to_string())
    }
    fn describe(&self) -> String {
        format!("device {}", self.path)
    }
}

// ---------------------------------------------------------------------------------
// An NBD export over a unix socket: the Sidon adapter.
// ---------------------------------------------------------------------------------

const NBDMAGIC: u64 = 0x4e42_444d_4147_4943;
const IHAVEOPT: u64 = 0x4948_4156_454f_5054;
const REP_MAGIC: u64 = 0x0003_e889_0455_65a9;
const REQUEST_MAGIC: u32 = 0x2560_9513;
const SIMPLE_REPLY_MAGIC: u32 = 0x6744_6698;
const OPT_GO: u32 = 7;
const REP_ACK: u32 = 1;
const REP_INFO: u32 = 3;
const INFO_EXPORT: u16 = 0;

pub struct NbdTarget {
    reader: BufReader<UnixStream>,
    writer: BufWriter<UnixStream>,
    size: u64,
    handle: u64,
    label: String,
}

fn rd<R: Read>(r: &mut R, n: usize) -> Result<Vec<u8>, String> {
    let mut b = vec![0u8; n];
    r.read_exact(&mut b).map_err(|e| format!("nbd read: {e}"))?;
    Ok(b)
}

fn be32(b: &[u8]) -> u32 {
    u32::from_be_bytes(b[0..4].try_into().unwrap())
}
fn be64(b: &[u8]) -> u64 {
    u64::from_be_bytes(b[0..8].try_into().unwrap())
}

impl NbdTarget {
    pub fn connect(socket: &Path, export: &str) -> Result<Self, String> {
        let stream = UnixStream::connect(socket)
            .map_err(|e| format!("cannot connect {}: {e}", socket.display()))?;
        let peer = stream.try_clone().map_err(|e| e.to_string())?;
        let mut reader = BufReader::new(peer);
        let mut writer = BufWriter::new(stream);

        let hello = rd(&mut reader, 18)?;
        if be64(&hello[0..8]) != NBDMAGIC || be64(&hello[8..16]) != IHAVEOPT {
            return Err("server did not send a newstyle NBD greeting".to_string());
        }
        // Client flags: fixed newstyle + no zeroes.
        writer.write_all(&3u32.to_be_bytes()).map_err(|e| e.to_string())?;

        // NBD_OPT_GO with the export name and no info requests.
        let name = export.as_bytes();
        let mut payload = Vec::new();
        payload.extend_from_slice(&(name.len() as u32).to_be_bytes());
        payload.extend_from_slice(name);
        payload.extend_from_slice(&0u16.to_be_bytes());
        writer.write_all(&IHAVEOPT.to_be_bytes()).map_err(|e| e.to_string())?;
        writer.write_all(&OPT_GO.to_be_bytes()).map_err(|e| e.to_string())?;
        writer.write_all(&(payload.len() as u32).to_be_bytes()).map_err(|e| e.to_string())?;
        writer.write_all(&payload).map_err(|e| e.to_string())?;
        writer.flush().map_err(|e| e.to_string())?;

        let mut size = None;
        loop {
            let head = rd(&mut reader, 20)?;
            if be64(&head[0..8]) != REP_MAGIC {
                return Err("bad option reply magic".to_string());
            }
            let rep_type = be32(&head[12..16]);
            let len = be32(&head[16..20]) as usize;
            let data = rd(&mut reader, len)?;
            if rep_type == REP_INFO && data.len() >= 10 {
                let info_type = u16::from_be_bytes(data[0..2].try_into().unwrap());
                if info_type == INFO_EXPORT {
                    size = Some(be64(&data[2..10]));
                }
            } else if rep_type == REP_ACK {
                break;
            } else if rep_type & 0x8000_0000 != 0 {
                return Err(format!("server refused NBD_OPT_GO: reply type {rep_type:#x}"));
            }
        }
        let size = size.ok_or_else(|| "server never sent NBD_INFO_EXPORT".to_string())?;
        Ok(NbdTarget {
            reader,
            writer,
            size,
            handle: 1,
            label: format!("nbd {} via {}", export, socket.display()),
        })
    }

    fn request(&mut self, cmd: u16, offset: u64, len: u32, data: Option<&[u8]>) -> Result<Vec<u8>, String> {
        let handle = self.handle;
        self.handle = self.handle.wrapping_add(1);
        let mut head = Vec::with_capacity(28);
        head.extend_from_slice(&REQUEST_MAGIC.to_be_bytes());
        head.extend_from_slice(&0u16.to_be_bytes());
        head.extend_from_slice(&cmd.to_be_bytes());
        head.extend_from_slice(&handle.to_be_bytes());
        head.extend_from_slice(&offset.to_be_bytes());
        head.extend_from_slice(&len.to_be_bytes());
        self.writer.write_all(&head).map_err(|e| format!("nbd write: {e}"))?;
        if let Some(d) = data {
            self.writer.write_all(d).map_err(|e| format!("nbd payload: {e}"))?;
        }
        self.writer.flush().map_err(|e| format!("nbd flush: {e}"))?;

        let reply = rd(&mut self.reader, 16)?;
        if be32(&reply[0..4]) != SIMPLE_REPLY_MAGIC {
            return Err("bad reply magic; the stream is desynchronised".to_string());
        }
        let errno = be32(&reply[4..8]);
        let got_handle = be64(&reply[8..16]);
        if got_handle != handle {
            return Err(format!("reply handle {got_handle} does not match request {handle}"));
        }
        if errno != 0 {
            return Err(format!("server returned errno {errno}"));
        }
        if cmd == 0 {
            return rd(&mut self.reader, len as usize);
        }
        Ok(Vec::new())
    }
}

impl Block for NbdTarget {
    fn size(&self) -> u64 {
        self.size
    }
    fn read_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>, String> {
        self.request(0, offset, len as u32, None)
    }
    fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<(), String> {
        self.request(1, offset, data.len() as u32, Some(data)).map(|_| ())
    }
    fn flush(&mut self) -> Result<(), String> {
        self.request(3, 0, 0, None).map(|_| ())
    }
    fn describe(&self) -> String {
        self.label.clone()
    }
}

pub fn open(spec: &str) -> Result<Box<dyn Block>, String> {
    // "nbd:<socket>:<export>" or a bare path.
    if let Some(rest) = spec.strip_prefix("nbd:") {
        let (socket, export) = rest
            .rsplit_once(':')
            .ok_or_else(|| "nbd target needs nbd:<socket>:<export>".to_string())?;
        Ok(Box::new(NbdTarget::connect(Path::new(socket), export)?))
    } else {
        Ok(Box::new(DeviceTarget::open(Path::new(spec))?))
    }
}
