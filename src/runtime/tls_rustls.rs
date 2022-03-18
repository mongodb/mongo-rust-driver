use std::{
    fs::File,
    io::{BufReader, Seek, SeekFrom},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_io::{AsyncRead, AsyncWrite};
use rustls::{
    internal::pemfile,
    Certificate,
    RootCertStore,
    ServerCertVerified,
    ServerCertVerifier,
    TLSError,
};
use rustls_pemfile::{read_one, Item};
use tokio::io::AsyncWrite as TokioAsyncWrite;
use tokio_rustls::TlsConnector;
use webpki::DNSNameRef;
use webpki_roots::TLS_SERVER_ROOTS;

use crate::{
    client::options::TlsOptions,
    error::{ErrorKind, Result},
};

use super::stream::AsyncTcpStream;

#[derive(Debug)]
pub(crate) struct AsyncTlsStream {
    inner: tokio_rustls::client::TlsStream<AsyncTcpStream>,
}

impl AsyncTlsStream {
    pub(crate) async fn connect(
        host: &str,
        tcp_stream: AsyncTcpStream,
        cfg: TlsOptions,
    ) -> Result<Self> {
        let name = DNSNameRef::try_from_ascii_str(host).map_err(|e| ErrorKind::DnsResolve {
            message: format!("could not resolve {:?}: {}", host, e),
        })?;
        let mut tls_config = make_rustls_config(cfg)?;
        tls_config.enable_sni = true;

        let connector: TlsConnector = Arc::new(tls_config).into();
        Ok(Self {
            inner: connector.connect(name, tcp_stream).await?,
        })
    }
}

impl AsyncRead for AsyncTlsStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        tokio_util::io::poll_read_buf(Pin::new(&mut self.inner), cx, &mut buf)
    }
}

impl AsyncWrite for AsyncTlsStream {
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

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Converts `TlsOptions` into a rustls::ClientConfig.
fn make_rustls_config(cfg: TlsOptions) -> Result<rustls::ClientConfig> {
    let mut config = rustls::ClientConfig::new();

    if let Some(true) = cfg.allow_invalid_certificates {
        config
            .dangerous()
            .set_certificate_verifier(Arc::new(NoCertVerifier {}));
    }

    let mut store = RootCertStore::empty();
    if let Some(path) = cfg.ca_file_path {
        store
            .add_pem_file(&mut BufReader::new(File::open(&path)?))
            .map_err(|_| ErrorKind::InvalidTlsConfig {
                message: format!(
                    "Unable to parse PEM-encoded root certificate from {}",
                    path.display()
                ),
            })?;
    } else {
        store.add_server_trust_anchors(&TLS_SERVER_ROOTS);
    }

    config.root_store = store;

    if let Some(path) = cfg.cert_key_file_path {
        let mut file = BufReader::new(File::open(&path)?);
        let certs = match pemfile::certs(&mut file) {
            Ok(certs) => certs,
            Err(()) => {
                return Err(ErrorKind::InvalidTlsConfig {
                    message: format!(
                        "Unable to parse PEM-encoded client certificate from {}",
                        path.display()
                    ),
                }
                .into())
            }
        };

        file.seek(SeekFrom::Start(0))?;
        let key = loop {
            match read_one(&mut file) {
                Ok(Some(Item::PKCS8Key(bytes))) | Ok(Some(Item::RSAKey(bytes))) => {
                    break rustls::PrivateKey(bytes)
                }
                Ok(Some(_)) => continue,
                Ok(None) => {
                    return Err(ErrorKind::InvalidTlsConfig {
                        message: format!("No PEM-encoded keys in {}", path.display()),
                    }
                    .into())
                }
                Err(_) => {
                    return Err(ErrorKind::InvalidTlsConfig {
                        message: format!(
                            "Unable to parse PEM-encoded item from {}",
                            path.display()
                        ),
                    }
                    .into())
                }
            }
        };

        config
            .set_single_client_cert(certs, key)
            .map_err(|e| ErrorKind::InvalidTlsConfig {
                message: e.to_string(),
            })?;
    }

    Ok(config)
}

struct NoCertVerifier {}

impl ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _: &RootCertStore,
        _: &[Certificate],
        _: webpki::DNSNameRef,
        _: &[u8],
    ) -> std::result::Result<ServerCertVerified, TLSError> {
        Ok(ServerCertVerified::assertion())
    }
}
