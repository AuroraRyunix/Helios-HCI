//! How two sites authenticate each other: a pinned certificate key, not a chain
//! (`docs/dfs/replication.md` section 2, D-28).
//!
//! **Status: exercised only over a loopback socket between two throwaway certificates.** Nothing
//! in the daemon constructs these configs and no listener exists.
//!
//! A site is identified by the SHA-256 of its leaf certificate's SubjectPublicKeyInfo. The
//! verifier ignores the chain and the name and accepts a peer if and only if that hash is in the
//! pin set, so a certificate renewed on the same key needs no action and a new key is a second
//! pin. Expiry is checked with a one-day leeway either side; a skew beyond that is reported *as*
//! skew, because "expired" would send an operator to renew a certificate that is fine.
//!
//! The handshake signature is still verified by rustls's default implementation: a pin proves
//! which key is expected, and the signature proves the peer holds it. This module only replaces
//! the question "is this certificate trusted".
//!
//! The same verifier serves both directions, because both are required: a site that did not
//! check the dialler would let anyone who knows the port speak the replication operations.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ring::digest;
use rustls::client::{ServerCertVerified, ServerCertVerifier};
use rustls::server::{ClientCertVerified, ClientCertVerifier};
use rustls::{Certificate, ClientConfig, DistinguishedName, Error as TlsError,
             PrivateKey, ServerConfig, ServerName};

use crate::err::{Error, Result};

pub const LEEWAY_SECONDS: i64 = 86_400;

// ---- a just-enough DER reader ---------------------------------------------------------------

/// One tag-length-value at the start of `b`: (tag, whole element, content, rest).
fn tlv(b: &[u8]) -> Option<(u8, &[u8], &[u8], &[u8])> {
    let tag = *b.first()?;
    let first = *b.get(1)?;
    let (len, header) = if first < 0x80 {
        (first as usize, 2)
    } else {
        let n = (first & 0x7f) as usize;
        if n == 0 || n > 4 || b.len() < 2 + n {
            return None;
        }
        let mut len = 0usize;
        for byte in &b[2..2 + n] {
            len = (len << 8) | *byte as usize;
        }
        (len, 2 + n)
    };
    let end = header.checked_add(len)?;
    if b.len() < end {
        return None;
    }
    Some((tag, &b[..end], &b[header..end], &b[end..]))
}

struct Parsed<'a> {
    spki: &'a [u8],
    not_before: i64,
    not_after: i64,
}

/// A SEQUENCE at the start of `b`: (whole, content, rest).
fn seq(b: &[u8]) -> Result<(&[u8], &[u8], &[u8])> {
    match tlv(b) {
        Some((0x30, whole, content, rest)) => Ok((whole, content, rest)),
        _ => Err(Error::refused("the peer's certificate is not valid DER".to_string())),
    }
}

fn parse(der: &[u8]) -> Result<Parsed<'_>> {
    let bad = || Error::refused("the peer's certificate is not valid DER".to_string());
    let (_, cert, _) = seq(der)?;
    let (_, tbs, _) = seq(cert)?;
    let mut rest = tbs;
    // The version is optional and context-tagged; the serial number follows it either way.
    let (tag, _, _, next) = tlv(rest).ok_or_else(bad)?;
    rest = next;
    if tag == 0xA0 {
        rest = tlv(rest).ok_or_else(bad)?.3; // the serial number
    }
    rest = tlv(rest).ok_or_else(bad)?.3; // signature algorithm
    rest = tlv(rest).ok_or_else(bad)?.3; // issuer
    let (_, validity, after) = seq(rest)?;
    rest = tlv(after).ok_or_else(bad)?.3; // subject
    let (spki, _, _) = seq(rest)?;
    let (nb_tag, _, nb, v_rest) = tlv(validity).ok_or_else(bad)?;
    let (na_tag, _, na, _) = tlv(v_rest).ok_or_else(bad)?;
    Ok(Parsed { spki, not_before: parse_time(nb_tag, nb)?, not_after: parse_time(na_tag, na)? })
}

fn parse_time(tag: u8, content: &[u8]) -> Result<i64> {
    let bad = || Error::refused("a certificate date is not understood".to_string());
    let s = std::str::from_utf8(content).map_err(|_| bad())?;
    let digits = s.strip_suffix('Z').ok_or_else(bad)?;
    let (year, rest) = match tag {
        0x17 if digits.len() == 12 => {
            let yy: i64 = digits[..2].parse().map_err(|_| bad())?;
            (if yy >= 50 { 1900 + yy } else { 2000 + yy }, &digits[2..])
        }
        0x18 if digits.len() == 14 => (digits[..4].parse().map_err(|_| bad())?, &digits[4..]),
        _ => return Err(bad()),
    };
    let f = |a: usize| -> Result<i64> { rest[a..a + 2].parse().map_err(|_| bad()) };
    Ok(days_from_civil(year, f(0)?, f(2)?) * 86_400 + f(4)? * 3_600 + f(6)? * 60 + f(8)?)
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The SHA-256 of the certificate's SubjectPublicKeyInfo, lowercase hex: the site's identity.
pub fn spki_fingerprint(cert_der: &[u8]) -> Result<String> {
    let p = parse(cert_der)?;
    Ok(digest::digest(&digest::SHA256, p.spki)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

// ---- the pin set --------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct PinSet {
    pins: HashSet<String>,
}

impl PinSet {
    pub fn new<I: IntoIterator<Item = S>, S: AsRef<str>>(pins: I) -> Self {
        PinSet {
            pins: pins.into_iter().map(|p| p.as_ref().trim().to_ascii_lowercase()).collect(),
        }
    }

    /// Accept or refuse a leaf certificate, with a reason an operator can act on.
    pub fn check(&self, cert_der: &[u8], now_secs: i64) -> Result<()> {
        let p = parse(cert_der)?;
        let fp = spki_fingerprint(cert_der)?;
        if !self.pins.contains(&fp) {
            return Err(Error::refused(format!(
                "the peer's key {fp} is not a pinned site key (revoked, never paired, or a new \
                 key that was not imported first)")));
        }
        if now_secs < p.not_before - LEEWAY_SECONDS {
            return Err(Error::refused(format!(
                "the peer's certificate becomes valid {} s from now: the clocks of the two sites \
                 differ by more than the {} s allowed (check time synchronisation; the \
                 certificate is not at fault)", p.not_before - now_secs, LEEWAY_SECONDS)));
        }
        if now_secs > p.not_after + LEEWAY_SECONDS {
            return Err(Error::refused(format!(
                "the peer's certificate expired {} s ago; renew it on the same key and no pin \
                 changes", now_secs - p.not_after)));
        }
        Ok(())
    }
}

fn secs(now: SystemTime) -> i64 {
    now.duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

/// Plugs a [`PinSet`] into rustls, for either end of the connection.
pub struct PinnedVerifier {
    pins: PinSet,
    hint: Vec<DistinguishedName>,
}

impl PinnedVerifier {
    pub fn new(pins: PinSet) -> Arc<Self> {
        Arc::new(PinnedVerifier { pins, hint: Vec::new() })
    }
}

fn to_tls(e: Error) -> TlsError {
    // Carried in the message so the operator's log names the cause, not just "bad certificate".
    TlsError::General(format!("{e}"))
}

impl ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &Certificate,
        _intermediates: &[Certificate],
        _server_name: &ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp: &[u8],
        now: SystemTime,
    ) -> std::result::Result<ServerCertVerified, TlsError> {
        self.pins.check(&end_entity.0, secs(now)).map_err(to_tls)?;
        Ok(ServerCertVerified::assertion())
    }
}

impl ClientCertVerifier for PinnedVerifier {
    fn client_auth_root_subjects(&self) -> &[DistinguishedName] {
        // Only a hint to the dialler about which certificate to send. Empty narrows nothing;
        // the certificate request is still made (the anonymous-dialler test pins that), and the
        // pin is what decides.
        &self.hint
    }

    fn verify_client_cert(
        &self,
        end_entity: &Certificate,
        _intermediates: &[Certificate],
        now: SystemTime,
    ) -> std::result::Result<ClientCertVerified, TlsError> {
        self.pins.check(&end_entity.0, secs(now)).map_err(to_tls)?;
        Ok(ClientCertVerified::assertion())
    }
}

/// The dialling side: refuses any server whose key is not pinned.
pub fn client_config(pins: PinSet, chain: Vec<Certificate>, key: PrivateKey) -> Result<ClientConfig> {
    ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(PinnedVerifier::new(pins))
        .with_client_auth_cert(chain, key)
        .map_err(|e| Error::refused(format!("site client certificate rejected: {e}")))
}

/// The listening side: requires a client certificate and refuses any whose key is not pinned.
pub fn server_config(pins: PinSet, chain: Vec<Certificate>, key: PrivateKey) -> Result<ServerConfig> {
    ServerConfig::builder()
        .with_safe_defaults()
        .with_client_cert_verifier(PinnedVerifier::new(pins))
        .with_single_cert(chain, key)
        .map_err(|e| Error::refused(format!("site server certificate rejected: {e}")))
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;

    struct Site {
        cert: Certificate,
        key_pem: PathBuf,
        cert_pem: PathBuf,
        key: PrivateKey,
    }

    fn dir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("sidon-sitetls-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn openssl(args: &[&str]) {
        let out = Command::new("openssl").args(args).stdin(std::process::Stdio::null()).output()
            .expect("these tests make their throwaway certificates with the openssl command");
        assert!(out.status.success(), "openssl {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    }

    fn load(cert_pem: &Path, key_pem: &Path) -> Site {
        let cert = rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(cert_pem).unwrap()))
            .unwrap().remove(0);
        let key = rustls_pemfile::pkcs8_private_keys(&mut BufReader::new(std::fs::File::open(key_pem).unwrap()))
            .unwrap().remove(0);
        Site { cert: Certificate(cert), key: PrivateKey(key),
               key_pem: key_pem.to_path_buf(), cert_pem: cert_pem.to_path_buf() }
    }

    /// A fresh key and self-signed certificate, the shape a site certificate has.
    fn site(d: &Path, name: &str) -> Site {
        let (k, c) = (d.join(format!("{name}.key")), d.join(format!("{name}.crt")));
        openssl(&["req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
                  "-keyout", k.to_str().unwrap(), "-out", c.to_str().unwrap(),
                  "-subj", &format!("/CN=HCI-Site-{name}"), "-days", "30"]);
        // rustls reads PKCS#8; make that explicit whatever openssl wrote.
        let k8 = d.join(format!("{name}.p8"));
        openssl(&["pkcs8", "-topk8", "-nocrypt", "-in", k.to_str().unwrap(), "-out", k8.to_str().unwrap()]);
        load(&c, &k8)
    }

    /// The same key, a new certificate: what a renewal is.
    fn renewed(d: &Path, of: &Site, name: &str) -> Site {
        let c = d.join(format!("{name}.crt"));
        openssl(&["req", "-x509", "-new", "-key", of.key_pem.to_str().unwrap(), "-out", c.to_str().unwrap(),
                  "-subj", &format!("/CN=HCI-Site-{name}-renewed"), "-days", "30"]);
        load(&c, &of.key_pem)
    }

    fn pins(sites: &[&Site]) -> PinSet {
        PinSet::new(sites.iter().map(|s| spki_fingerprint(&s.cert.0).unwrap()))
    }

    /// One connection, both ends in this process. Returns (what the server saw, what the client saw).
    fn dial(
        server: &Site, server_pins: PinSet, client: &Site, client_pins: PinSet,
    ) -> (std::result::Result<String, String>, std::result::Result<String, String>) {
        let sc = Arc::new(server_config(server_pins, vec![server.cert.clone()], server.key.clone()).unwrap());
        let cc = Arc::new(client_config(client_pins, vec![client.cert.clone()], client.key.clone()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let (sock, _) = listener.accept().unwrap();
            let conn = rustls::ServerConnection::new(sc).unwrap();
            let mut tls = rustls::StreamOwned::new(conn, sock);
            let mut buf = [0u8; 4];
            tls.read_exact(&mut buf).map_err(|e| e.to_string())?;
            tls.write_all(b"pong").map_err(|e| e.to_string())?;
            tls.flush().map_err(|e| e.to_string())?;
            Ok(String::from_utf8_lossy(&buf).to_string())
        });
        let sock = TcpStream::connect(addr).unwrap();
        let name = ServerName::try_from("site.invalid").unwrap(); // the name is ignored on purpose
        let conn = rustls::ClientConnection::new(cc, name).unwrap();
        let mut tls = rustls::StreamOwned::new(conn, sock);
        let client_side = (|| {
            tls.write_all(b"ping").map_err(|e| e.to_string())?;
            tls.flush().map_err(|e| e.to_string())?;
            let mut buf = [0u8; 4];
            tls.read_exact(&mut buf).map_err(|e| e.to_string())?;
            Ok(String::from_utf8_lossy(&buf).to_string())
        })();
        (handle.join().unwrap(), client_side)
    }

    #[test]
    fn the_fingerprint_is_the_sha256_of_the_public_key_as_openssl_computes_it() {
        let d = dir("fp");
        let a = site(&d, "a");
        let pubkey = d.join("a.pub");
        openssl(&["pkey", "-in", a.key_pem.to_str().unwrap(), "-pubout", "-outform", "DER", "-out", pubkey.to_str().unwrap()]);
        let expected: String = digest::digest(&digest::SHA256, &std::fs::read(&pubkey).unwrap())
            .as_ref().iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(spki_fingerprint(&a.cert.0).unwrap(), expected);
        let _ = a.cert_pem;
    }

    #[test]
    fn two_sites_that_pin_each_other_talk() {
        let d = dir("ok");
        let (a, b) = (site(&d, "a"), site(&d, "b"));
        let (server_saw, client_saw) = dial(&a, pins(&[&b]), &b, pins(&[&a]));
        assert_eq!(server_saw.unwrap(), "ping");
        assert_eq!(client_saw.unwrap(), "pong");
    }

    #[test]
    fn a_server_whose_key_is_not_pinned_is_refused_by_the_dialler() {
        let d = dir("srv");
        let (a, b, stranger) = (site(&d, "a"), site(&d, "b"), site(&d, "x"));
        let (_, client_saw) = dial(&stranger, pins(&[&b]), &b, pins(&[&a]));
        assert!(client_saw.is_err(), "the client must not talk to a key it did not pin");
    }

    #[test]
    fn a_client_whose_key_is_not_pinned_is_refused_by_the_listener() {
        let d = dir("cli");
        let (a, b, stranger) = (site(&d, "a"), site(&d, "b"), site(&d, "x"));
        let (server_saw, client_saw) = dial(&a, pins(&[&b]), &stranger, pins(&[&a]));
        assert!(server_saw.is_err(), "the listener served an unpinned client");
        assert!(client_saw.is_err());
    }

    #[test]
    fn a_dialler_with_no_certificate_at_all_is_refused() {
        let d = dir("anon");
        let a = site(&d, "a");
        let sc = Arc::new(server_config(pins(&[&a]), vec![a.cert.clone()], a.key.clone()).unwrap());
        let cc = Arc::new(
            ClientConfig::builder().with_safe_defaults()
                .with_custom_certificate_verifier(PinnedVerifier::new(pins(&[&a])))
                .with_no_client_auth());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let (sock, _) = listener.accept().unwrap();
            let mut tls = rustls::StreamOwned::new(rustls::ServerConnection::new(sc).unwrap(), sock);
            let mut buf = [0u8; 4];
            tls.read_exact(&mut buf).map(|_| ()).map_err(|e| e.to_string())
        });
        let mut tls = rustls::StreamOwned::new(
            rustls::ClientConnection::new(cc, ServerName::try_from("x.invalid").unwrap()).unwrap(),
            TcpStream::connect(addr).unwrap());
        let _ = tls.write_all(b"ping");
        let _ = tls.flush();
        let mut b = [0u8; 4];
        let _ = tls.read_exact(&mut b);
        assert!(handle.join().unwrap().is_err(), "anonymous clients must not reach the operations");
    }

    #[test]
    fn a_renewed_certificate_on_the_same_key_needs_no_new_pin_and_a_new_key_does() {
        let d = dir("renew");
        let (a, b) = (site(&d, "a"), site(&d, "b"));
        let a2 = renewed(&d, &a, "a2");
        assert_ne!(a.cert.0, a2.cert.0);
        assert_eq!(spki_fingerprint(&a.cert.0).unwrap(), spki_fingerprint(&a2.cert.0).unwrap());
        let (server_saw, client_saw) = dial(&a2, pins(&[&b]), &b, pins(&[&a]));
        assert_eq!((server_saw.unwrap(), client_saw.unwrap()), ("ping".into(), "pong".into()));
        // The new key is introduced as a second pin and both work during the overlap.
        let a3 = site(&d, "a3");
        let both = pins(&[&a, &a3]);
        assert!(dial(&a3, pins(&[&b]), &b, both.clone()).1.is_ok());
        assert!(dial(&a, pins(&[&b]), &b, both).1.is_ok());
    }

    #[test]
    fn revoking_a_pin_refuses_the_next_connection() {
        let d = dir("revoke");
        let (a, b) = (site(&d, "a"), site(&d, "b"));
        assert!(dial(&a, pins(&[&b]), &b, pins(&[&a])).1.is_ok());
        let nobody = PinSet::new(Vec::<String>::new());
        assert!(dial(&a, pins(&[&b]), &b, nobody).1.is_err());
    }

    #[test]
    fn expiry_has_a_day_of_leeway_and_a_larger_skew_is_called_skew() {
        let d = dir("time");
        let a = site(&d, "a");
        let p = parse(&a.cert.0).unwrap();
        let set = pins(&[&a]);
        let (nb, na) = (p.not_before, p.not_after);
        assert!(set.check(&a.cert.0, nb + 60).is_ok());
        assert!(set.check(&a.cert.0, nb - LEEWAY_SECONDS + 60).is_ok(), "a day early is within the leeway");
        assert!(set.check(&a.cert.0, na + LEEWAY_SECONDS - 60).is_ok(), "a day late is within the leeway");
        let early = format!("{}", set.check(&a.cert.0, nb - 3 * LEEWAY_SECONDS).unwrap_err());
        assert!(early.contains("clocks of the two sites differ"), "{early}");
        assert!(early.contains("not at fault"), "{early}");
        let late = format!("{}", set.check(&a.cert.0, na + 3 * LEEWAY_SECONDS).unwrap_err());
        assert!(late.contains("expired"), "{late}");
        assert!(late.contains("same key"), "{late}");
    }

    #[test]
    fn a_pin_refusal_names_the_key_so_it_can_be_compared_with_the_pairing_bundle() {
        let d = dir("name");
        let (a, b) = (site(&d, "a"), site(&d, "b"));
        let err = format!("{}", pins(&[&a]).check(&b.cert.0, 0).unwrap_err());
        assert!(err.contains(&spki_fingerprint(&b.cert.0).unwrap()), "{err}");
    }

    #[test]
    fn garbage_and_truncated_certificates_are_refused_not_a_panic() {
        let d = dir("garbage");
        let a = site(&d, "a");
        let set = pins(&[&a]);
        for bad in [vec![], vec![0x30], vec![0x30, 0x82, 0xff, 0xff], b"not a certificate".to_vec(),
                    a.cert.0[..a.cert.0.len() / 2].to_vec()] {
            assert!(set.check(&bad, 0).is_err());
        }
    }

    #[test]
    fn dates_convert_the_way_the_calendar_does() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
        assert_eq!(parse_time(0x17, b"700101000000Z").unwrap(), 0);
        assert_eq!(parse_time(0x18, b"20240229120000Z").unwrap(), 1_709_208_000);
        assert_eq!(parse_time(0x17, b"491231235959Z").unwrap(), 2_524_607_999);
    }
}
