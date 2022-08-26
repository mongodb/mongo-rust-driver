use std::{
    convert::TryFrom,
    fs::File,
    io::{BufReader, Seek, SeekFrom},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::SystemTime,
};

use rustls::{
    client::{ClientConfig, ServerCertVerified, ServerCertVerifier, ServerName},
    Certificate,
    Error as TlsError,
    OwnedTrustAnchor,
    RootCertStore,
};
use rustls_pemfile::{certs, read_one, Item};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::TlsConnector;
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

/// Configuration required to use TLS. Creating this is expensive, so its best to cache this value
/// and reuse it for multiple connections.
#[derive(Clone)]
pub(crate) struct TlsConfig {
    connector: TlsConnector,
}

impl TlsConfig {
    /// Create a new `TlsConfig` from the provided options from the user.
    /// This operation is expensive, so the resultant `TlsConfig` should be cached.
    pub(crate) fn new(options: TlsOptions) -> Result<TlsConfig> {
        let mut tls_config = make_rustls_config(options)?;
        tls_config.enable_sni = true;

        let connector: TlsConnector = Arc::new(tls_config).into();
        Ok(TlsConfig { connector })
    }
}

impl AsyncTlsStream {
    pub(crate) async fn connect(
        host: &str,
        tcp_stream: AsyncTcpStream,
        cfg: &TlsConfig,
    ) -> Result<Self> {
        let name = ServerName::try_from(host).map_err(|e| ErrorKind::DnsResolve {
            message: format!("could not resolve {:?}: {}", host, e),
        })?;

        let conn = cfg
            .connector
            .connect_with(name, tcp_stream, |c| {
                c.set_buffer_limit(None);
            })
            .await?;
        Ok(Self { inner: conn })
    }
}

impl AsyncRead for AsyncTlsStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
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

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Converts `TlsOptions` into a rustls::ClientConfig.
fn make_rustls_config(cfg: TlsOptions) -> Result<rustls::ClientConfig> {
    let mut store = RootCertStore::empty();
    if let Some(path) = cfg.ca_file_path {
        let ders = certs(&mut BufReader::new(File::open(&path)?)).map_err(|_| {
            ErrorKind::InvalidTlsConfig {
                message: format!(
                    "Unable to parse PEM-encoded root certificate from {}",
                    path.display()
                ),
            }
        })?;
        store.add_parsable_certificates(&ders);
    } else {
        let trust_anchors = TLS_SERVER_ROOTS.0.iter().map(|ta| {
            OwnedTrustAnchor::from_subject_spki_name_constraints(
                ta.subject,
                ta.spki,
                ta.name_constraints,
            )
        });
        store.add_server_trust_anchors(trust_anchors);
    }

    let mut config = if let Some(path) = cfg.cert_key_file_path {
        let mut file = BufReader::new(File::open(&path)?);
        let certs = match certs(&mut file) {
            Ok(certs) => certs.into_iter().map(Certificate).collect(),
            Err(error) => {
                return Err(ErrorKind::InvalidTlsConfig {
                    message: format!(
                        "Unable to parse PEM-encoded client certificate from {}: {}",
                        path.display(),
                        error,
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

        ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(store)
            .with_single_cert(certs, key)
            .map_err(|error| ErrorKind::InvalidTlsConfig {
                message: error.to_string(),
            })?
    } else {
        ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(store)
            .with_no_client_auth()
    };

    if let Some(true) = cfg.allow_invalid_certificates {
        config
            .dangerous()
            .set_certificate_verifier(Arc::new(NoCertVerifier {}));
    }

    Ok(config)
}

struct NoCertVerifier {}

impl ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _: &Certificate,
        _: &[Certificate],
        _: &ServerName,
        _: &mut dyn Iterator<Item = &[u8]>,
        _: &[u8],
        _: SystemTime,
    ) -> std::result::Result<ServerCertVerified, TlsError> {
        Ok(ServerCertVerified::assertion())
    }
}
