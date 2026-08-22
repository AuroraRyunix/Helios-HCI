//! Mutual TLS for the replication port, against the cluster CA.
//!
//! Port 9105 carries guest data. An `APPEND` payload is the literal bytes a VM just
//! wrote, and the same connection carries `FENCE`, `TRUNCATE` and `EGROUP_PUT`. Without
//! authentication anyone who can reach the port can read every guest's writes *and*
//! raise the epoch on a vdisk, which makes every replica refuse the real owner's
//! appends. The fencing proof assumes only cluster members can speak this protocol; this
//! module is what makes that true.
//!
//! Both directions are verified. Server-only TLS would encrypt the bytes and still let
//! anyone who trusts the CA -- which is anyone at all, since the CA certificate is
//! public -- connect and issue commands. What matters here is *authentication*, and it
//! has to be mutual because both ends are peers rather than a client and a service.
//!
//! The material is the same the rest of the cluster already uses: `/etc/hci/spark/certs`,
//! issued by Impa, renewed by Impa. Sidon deliberately introduces no second credential
//! for an operator to discover has expired.
//!
//! ## Why this is synchronous
//!
//! Agahnim, the other Rust service here, uses `tokio-rustls`. Sidon's whole byte path is
//! blocking threads and std, so it uses plain `rustls` over the `TcpStream` that was
//! already there. Pulling tokio in for the transport would restructure the data path to
//! solve a problem the data path does not have.

use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::server::AllowAnyAuthenticatedClient;
use rustls::{Certificate, ClientConfig, ClientConnection, PrivateKey, RootCertStore,
             ServerConfig, ServerConnection, ServerName, StreamOwned};

use crate::err::{Error, Result};

/// Where Impa puts the node's own identity and the CA that signed it.
pub const CERT_DIR: &str = "/etc/hci/spark/certs";

/// Where to look, honouring `SIDON_CERT_DIR`.
///
/// The override exists for tests, which need a directory with nothing in it to exercise
/// the refusal, and it costs nothing to let an operator relocate the material with it.
pub fn cert_dir() -> PathBuf {
    match std::env::var("SIDON_CERT_DIR") {
        Ok(v) if !v.trim().is_empty() => PathBuf::from(v),
        _ => PathBuf::from(CERT_DIR),
    }
}

/// Whether a connection to or from `addr` must be encrypted, given whether the material
/// is available. `Ok(true)` means wrap it in TLS.
///
/// The rule in one place, as a function of two facts, so it can be checked without
/// binding a port or holding a certificate. Both ends consult it: a listener deciding
/// what to accept and a client deciding what to dial have to agree, and the way they stop
/// agreeing is by each implementing the rule separately.
pub fn wire_policy(addr: &str, have_material: bool) -> Result<bool> {
    if is_loopback(addr) {
        return Ok(false);
    }
    if have_material {
        return Ok(true);
    }
    Err(Error::refused(format!(
        "refusing plaintext replication on {addr}: it carries guest data, and the \
         mutual-TLS material in {} could not be loaded. Fix the certificates, or use \
         127.0.0.1 for single-host testing.",
        cert_dir().display()
    )))
}

/// A peer connection, with or without TLS underneath.
///
/// Boxed rather than generic so the framing code stays one function. The alternative --
/// making `serve_connection` and `PeerClient::call` generic over `Read + Write` -- pushes
/// a type parameter through every caller to save one vtable lookup per *frame*, on a path
/// whose per-frame cost is an fdatasync.
pub trait Wire: Read + Write + Send {}
impl<T: Read + Write + Send> Wire for T {}

pub struct TlsMaterial {
    server: Arc<ServerConfig>,
    client: Arc<ClientConfig>,
}

fn read_certs(path: &Path) -> Result<Vec<Certificate>> {
    let file = File::open(path)
        .map_err(|e| Error::io(format!("cannot read certificate {}: {e}", path.display())))?;
    let certs = rustls_pemfile::certs(&mut BufReader::new(file))
        .map_err(|e| Error::io(format!("{} is not a PEM certificate: {e}", path.display())))?;
    if certs.is_empty() {
        return Err(Error::io(format!("{} contains no certificate", path.display())));
    }
    Ok(certs.into_iter().map(Certificate).collect())
}

fn read_key(path: &Path) -> Result<PrivateKey> {
    // PKCS#8 first, then the older forms. Impa writes PKCS#8, but a cluster provisioned
    // by an earlier build may hold an RSA or SEC1 key and refusing to start on one would
    // be a storage outage caused by a key format.
    let open = || {
        File::open(path)
            .map_err(|e| Error::io(format!("cannot read key {}: {e}", path.display())))
    };
    if let Ok(keys) = rustls_pemfile::pkcs8_private_keys(&mut BufReader::new(open()?)) {
        if let Some(k) = keys.into_iter().next() {
            return Ok(PrivateKey(k));
        }
    }
    if let Ok(keys) = rustls_pemfile::rsa_private_keys(&mut BufReader::new(open()?)) {
        if let Some(k) = keys.into_iter().next() {
            return Ok(PrivateKey(k));
        }
    }
    if let Ok(keys) = rustls_pemfile::ec_private_keys(&mut BufReader::new(open()?)) {
        if let Some(k) = keys.into_iter().next() {
            return Ok(PrivateKey(k));
        }
    }
    Err(Error::io(format!("{} holds no private key this build can read", path.display())))
}

impl TlsMaterial {
    /// Load the node identity and the cluster CA from `dir`.
    ///
    /// Every failure is fatal rather than a fallback to plaintext. A daemon that quietly
    /// serves guest data unencrypted because a file was missing is worse than one that
    /// does not start, and the second is the failure an operator can see.
    pub fn load(dir: &Path) -> Result<TlsMaterial> {
        let ca = read_certs(&dir.join("ca.crt"))?;
        let node = read_certs(&dir.join("node.crt"))?;
        let key = read_key(&dir.join("node.key"))?;

        let mut roots = RootCertStore::empty();
        for cert in &ca {
            roots
                .add(cert)
                .map_err(|e| Error::io(format!("cluster CA is not usable as a root: {e}")))?;
        }

        // Client certificates must be signed by the cluster CA. `AllowAnyAuthenticated`
        // means "any identity this CA vouches for", which is exactly the set of cluster
        // nodes -- the CA signs nothing else, and Impa is the only thing that can ask it
        // to.
        let verifier = AllowAnyAuthenticatedClient::new(roots.clone());
        let server = ServerConfig::builder()
            .with_safe_defaults()
            .with_client_cert_verifier(Arc::new(verifier))
            .with_single_cert(node.clone(), key.clone())
            .map_err(|e| Error::io(format!("node certificate and key do not match: {e}")))?;

        let client = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(roots)
            .with_client_auth_cert(node, key)
            .map_err(|e| Error::io(format!("node certificate and key do not match: {e}")))?;

        Ok(TlsMaterial { server: Arc::new(server), client: Arc::new(client) })
    }

    /// Load from the standard location, or `None` when the material is not there.
    ///
    /// `None` is not "carry on without TLS": it is what `listen` and `dial` turn into a
    /// refusal for any address that is not loopback.
    pub fn load_default() -> Option<TlsMaterial> {
        let dir = cert_dir();
        match TlsMaterial::load(&dir) {
            Ok(m) => Some(m),
            Err(e) => {
                eprintln!("sidon: no TLS material in {}: {e}", dir.display());
                None
            }
        }
    }

    /// Wrap an accepted connection. The handshake runs on first read or write.
    pub fn accept(&self, sock: TcpStream) -> Result<Box<dyn Wire>> {
        let conn = ServerConnection::new(Arc::clone(&self.server))
            .map_err(|e| Error::io(format!("tls server setup: {e}")))?;
        Ok(Box::new(StreamOwned::new(conn, sock)))
    }

    /// Wrap an outgoing connection, verifying the peer is `name`.
    pub fn connect(&self, name: ServerName, sock: TcpStream) -> Result<Box<dyn Wire>> {
        let conn = ClientConnection::new(Arc::clone(&self.client), name)
            .map_err(|e| Error::io(format!("tls client setup: {e}")))?;
        Ok(Box::new(StreamOwned::new(conn, sock)))
    }
}

/// The name to verify a peer against, from a `host:port` address.
///
/// Node certificates carry `subjectAltName = IP:<node ip>` and nothing else, so a peer
/// addressed by IP is verified against that SAN. A peer addressed by hostname would have
/// to be verified against a DNS SAN the cluster CA does not issue, so it is refused here
/// rather than at a handshake whose error message would be about certificates.
pub fn server_name_for(addr: &str) -> Result<ServerName> {
    let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    match host.parse::<std::net::IpAddr>() {
        Ok(ip) => Ok(ServerName::IpAddress(ip)),
        Err(_) => Err(Error::refused(format!(
            "peer address '{addr}' is not an IP. Node certificates carry only an IP \
             subjectAltName, so a peer named by hostname cannot be verified."
        ))),
    }
}

/// Does this address stay on the machine?
///
/// The one case plaintext is allowed: a connection that cannot leave the host cannot be
/// intercepted off it, and single-host multi-instance testing is how the protocol and the
/// state machine get exercised without three nodes.
pub fn is_loopback(addr: &str) -> bool {
    let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    match host.parse::<std::net::IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        Err(_) => host == "localhost",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_is_recognised_in_every_form_the_config_uses() {
        for addr in ["127.0.0.1:9105", "localhost:9105", "[::1]:9105", "::1:9105"] {
            assert!(is_loopback(addr), "{addr} should be loopback");
        }
        for addr in ["10.10.102.41:9105", "0.0.0.0:9105", "192.168.1.5:9105"] {
            assert!(!is_loopback(addr), "{addr} should not be loopback");
        }
    }

    #[test]
    fn a_peer_named_by_hostname_is_refused_rather_than_failed_at_the_handshake() {
        // The cluster CA issues IP SANs only. Failing here names the actual problem;
        // failing at the handshake produces a certificate error that sends an operator
        // looking at Impa.
        assert!(server_name_for("hci-02:9105").is_err());
        assert!(server_name_for("10.10.102.41:9105").is_ok());
    }

    #[test]
    fn the_plaintext_rule_is_the_same_from_both_ends() {
        // Loopback never needs it, whether or not material exists.
        assert_eq!(wire_policy("127.0.0.1:9105", false).unwrap(), false);
        assert_eq!(wire_policy("127.0.0.1:9105", true).unwrap(), false);
        // Anything routable does.
        assert_eq!(wire_policy("10.10.102.41:9105", true).unwrap(), true);
        // And without material, a routable address is refused rather than downgraded.
        // This is the assertion that matters: the failure mode being guarded against is
        // a daemon that quietly ships guest data in the clear because a file was absent.
        let refused = wire_policy("10.10.102.41:9105", false);
        assert!(refused.is_err());
        let msg = format!("{:?}", refused.unwrap_err());
        assert!(msg.contains("plaintext"), "{msg}");
    }

    #[test]
    fn a_bind_to_all_interfaces_is_not_loopback() {
        // 0.0.0.0 reaches the machine from outside it, so it is exactly the case that
        // must not be mistaken for local.
        assert!(!is_loopback("0.0.0.0:9105"));
        assert!(wire_policy("0.0.0.0:9105", false).is_err());
    }

    #[test]
    fn an_address_without_a_port_still_parses() {
        assert!(server_name_for("10.10.102.41").is_ok());
        assert!(is_loopback("127.0.0.1"));
    }
}
