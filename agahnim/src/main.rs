use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::Arc;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::process::Command;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;
use futures_util::{SinkExt, StreamExt};
use tokio_rustls::rustls::{Certificate, PrivateKey, ServerConfig};
use tokio_rustls::TlsAcceptor;


fn load_certs(path: &Path) -> std::io::Result<Vec<Certificate>> {
    let certfile = File::open(path)?;
    let mut reader = BufReader::new(certfile);
    let certs = rustls_pemfile::certs(&mut reader)?
        .into_iter()
        .map(Certificate)
        .collect();
    Ok(certs)
}

fn load_key(path: &Path) -> std::io::Result<PrivateKey> {
    let keyfile = File::open(path)?;
    let mut reader = BufReader::new(keyfile);
    let keys = rustls_pemfile::pkcs8_private_keys(&mut reader)?;
    if keys.is_empty() {
        let mut reader2 = BufReader::new(File::open(path)?);
        let ec_keys = rustls_pemfile::ec_private_keys(&mut reader2)?;
        if ec_keys.is_empty() {
            let mut reader3 = BufReader::new(File::open(path)?);
            let rsa_keys = rustls_pemfile::rsa_private_keys(&mut reader3)?;
            if rsa_keys.is_empty() {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "No private keys found"));
            }
            return Ok(PrivateKey(rsa_keys[0].clone()));
        }
        return Ok(PrivateKey(ec_keys[0].clone()));
    }
    Ok(PrivateKey(keys[0].clone()))
}

#[tokio::main]
async fn main() {
    let port = std::env::args()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(8081);

    let cert_path = Path::new("/etc/hci/spectrum/certs/server.crt");
    let key_path = Path::new("/etc/hci/spectrum/certs/server.key");

    let tls_acceptor = if cert_path.exists() && key_path.exists() {
        match (load_certs(cert_path), load_key(key_path)) {
            (Ok(certs), Ok(key)) => {
                let config_res = ServerConfig::builder()
                    .with_safe_defaults()
                    .with_no_client_auth()
                    .with_single_cert(certs, key);
                match config_res {
                    Ok(config) => {
                        println!("Agahnim: SSL Certificates loaded successfully. Enabling TLS support.");
                        Some(TlsAcceptor::from(Arc::new(config)))
                    }
                    Err(e) => {
                        eprintln!("Agahnim: Failed to construct ServerConfig: {}", e);
                        None
                    }
                }
            }
            (cert_err, key_err) => {
                eprintln!("Agahnim: Error loading certificates: cert={:?}, key={:?}", cert_err.err(), key_err.err());
                None
            }
        }
    } else {
        println!("Agahnim: Certificate files not found at /etc/hci/spectrum/certs/. Running in non-TLS mode.");
        None
    };

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(&addr).await.expect("Failed to bind TCP listener");
    println!("Agahnim proxy listening on {}", addr);

    let tls_acceptor = tls_acceptor.map(Arc::new);

    while let Ok((stream, client_addr)) = listener.accept().await {
        let acceptor_clone = tls_acceptor.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, client_addr, acceptor_clone).await {
                eprintln!("Connection error with {}: {}", client_addr, e);
            }
        });
    }
}

struct PrefixedStream<S> {
    prefix: Vec<u8>,
    read_pos: usize,
    inner: S,
}

impl<S: AsyncRead + Unpin> AsyncRead for PrefixedStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let prefix_len = self.prefix.len();
        if self.read_pos < prefix_len {
            let to_read = std::cmp::min(prefix_len - self.read_pos, buf.remaining());
            buf.put_slice(&self.prefix[self.read_pos..self.read_pos + to_read]);
            self.read_pos += to_read;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for PrefixedStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

async fn handle_connection(
    stream: TcpStream,
    client_addr: SocketAddr,
    acceptor: Option<Arc<TlsAcceptor>>,
) -> Result<(), Box<dyn std::error::Error>> {
    stream.set_nodelay(true)?;
    let mut peek_buf = [0u8; 1];
    let is_tls = match stream.peek(&mut peek_buf).await {
        Ok(n) => n > 0 && peek_buf[0] == 0x16,
        Err(_) => false,
    };

    let token_mutex = Arc::new(std::sync::Mutex::new(String::new()));
    let token_clone = token_mutex.clone();

    let callback = move |request: &Request, mut response: Response| {
        let query = request.uri().query().unwrap_or("");
        for pair in query.split('&') {
            let parts: Vec<&str> = pair.split('=').collect();
            if parts.len() == 2 && parts[0] == "token" {
                if let Ok(mut guard) = token_clone.lock() {
                    *guard = parts[1].to_string();
                }
            }
        }
        if let Some(proto) = request.headers().get("sec-websocket-protocol") {
            response.headers_mut().insert("sec-websocket-protocol", proto.clone());
        }
        Ok(response)
    };

    if is_tls && acceptor.is_some() {
        let tls_acceptor = acceptor.unwrap();
        let mut tls_stream = tls_acceptor.accept(stream).await?;
        
        let mut buf = vec![0u8; 2048];
        let n = tls_stream.read(&mut buf).await?;
        let req_str = String::from_utf8_lossy(&buf[..n]);
        let req_str_lower = req_str.to_lowercase();
        if req_str_lower.contains("upgrade") && req_str_lower.contains("websocket") {
            let prefixed = PrefixedStream {
                prefix: buf[..n].to_vec(),
                read_pos: 0,
                inner: tls_stream,
            };
            let ws_stream = tokio_tungstenite::accept_hdr_async(prefixed, callback).await?;
            run_proxy(ws_stream, token_mutex, client_addr).await
        } else {
            let body = "<html><head><title>Console Authorized</title><style>body { background: #0d101a; color: #fff; font-family: sans-serif; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; } div { text-align: center; border: 1px solid rgba(255,255,255,0.1); padding: 40px; border-radius: 8px; background: rgba(255,255,255,0.02); }</style></head><body><div><h2>Certificate Authorized Successfully!</h2><p>You can now close this tab and click <strong>Reconnect</strong> on the SPICE console.</p></div></body></html>";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            tls_stream.write_all(resp.as_bytes()).await?;
            tls_stream.flush().await?;
            Ok(())
        }
    } else {
        let mut raw_stream = stream;
        let mut buf = vec![0u8; 2048];
        let n = raw_stream.read(&mut buf).await?;
        let req_str = String::from_utf8_lossy(&buf[..n]);
        let req_str_lower = req_str.to_lowercase();
        if req_str_lower.contains("upgrade") && req_str_lower.contains("websocket") {
            let prefixed = PrefixedStream {
                prefix: buf[..n].to_vec(),
                read_pos: 0,
                inner: raw_stream,
            };
            let ws_stream = tokio_tungstenite::accept_hdr_async(prefixed, callback).await?;
            run_proxy(ws_stream, token_mutex, client_addr).await
        } else {
            let body = "<html><head><title>Console Authorized</title><style>body { background: #0d101a; color: #fff; font-family: sans-serif; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; } div { text-align: center; border: 1px solid rgba(255,255,255,0.1); padding: 40px; border-radius: 8px; background: rgba(255,255,255,0.02); }</style></head><body><div><h2>Certificate Authorized Successfully!</h2><p>You can now close this tab and click <strong>Reconnect</strong> on the SPICE console.</p></div></body></html>";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            raw_stream.write_all(resp.as_bytes()).await?;
            raw_stream.flush().await?;
            Ok(())
        }
    }
}

async fn run_proxy<S>(
    ws_stream: tokio_tungstenite::WebSocketStream<S>,
    token_mutex: Arc<std::sync::Mutex<String>>,
    client_addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let token = {
        let guard = token_mutex.lock().unwrap();
        guard.clone()
    };

    if token.is_empty() {
        return Err("Missing token parameter".into());
    }

    println!("[Agahnim] Verifying token '{}' from client {}", token, client_addr);

    // Connect to Python token verifier
    let mut verifier_stream = TcpStream::connect("127.0.0.1:8089").await?;
    verifier_stream.write_all(token.as_bytes()).await?;
    verifier_stream.shutdown().await?; // Close write end so Python knows we're done sending

    let mut response = String::new();
    verifier_stream.read_to_string(&mut response).await?;

    if !response.starts_with("OK|") {
        return Err(format!("Token verification failed: '{}'", response).into());
    }

    let parts: Vec<&str> = response.split('|').collect();
    if parts.len() != 3 {
        return Err(format!("Invalid response format from verifier: '{}'", response).into());
    }

    let target_host = parts[1];
    let target_port = parts[2].parse::<u16>()?;

    println!("[Agahnim] Token verified. Proxying client to target {}:{}", target_host, target_port);

    let vnc_stream = TcpStream::connect((target_host, target_port)).await?;
    vnc_stream.set_nodelay(true)?;
    println!("[Agahnim] Connected to target {}:{}", target_host, target_port);

    // Split stream and copy bidirectionally
    let (mut ws_write, mut ws_read) = ws_stream.split();
    let (mut tcp_read, mut tcp_write) = vnc_stream.into_split();

    let client_to_vm = async move {
        while let Some(msg_result) = ws_read.next().await {
            match msg_result {
                Ok(msg) => {
                    if msg.is_binary() || msg.is_text() {
                        let data = msg.into_data();
                        if let Err(e) = tcp_write.write_all(&data).await {
                            eprintln!("[Agahnim] TCP write error: {}", e);
                            break;
                        }
                    } else if msg.is_close() {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("[Agahnim] WebSocket read error: {}", e);
                    break;
                }
            }
        }
    };

    let vm_to_client = async move {
        let mut buf = [0u8; 65536];
        loop {
            match tcp_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let msg = Message::Binary(buf[..n].to_vec());
                    if let Err(e) = ws_write.send(msg).await {
                        eprintln!("[Agahnim] WebSocket write error: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("[Agahnim] TCP read error: {}", e);
                    break;
                }
            }
        }
    };

    tokio::select! {
        _ = client_to_vm => {},
        _ = vm_to_client => {},
    }

    println!("[Agahnim] Tearing down connection for token '{}'", token);
    Ok(())
}
