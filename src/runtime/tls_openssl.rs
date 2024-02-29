use std::{pin::Pin, sync::Once};

use openssl::{
    error::ErrorStack,
    ssl::{SslConnector, SslFiletype, SslMethod, SslVerifyMode},
};
use tokio::net::TcpStream;
use tokio_openssl::SslStream;

use crate::{
    client::options::TlsOptions,
    error::{Error, ErrorKind, Result},
};

pub(super) type TlsStream = SslStream<TcpStream>;

/// Configuration required to use TLS. Creating this is expensive, so its best to cache this value
/// and reuse it for multiple connections.
#[derive(Clone)]
pub(crate) struct TlsConfig {
    connector: SslConnector,
    verify_hostname: bool,
}

impl TlsConfig {
    /// Create a new `TlsConfig` from the provided options from the user.
    /// This operation is expensive, so the resultant `TlsConfig` should be cached.
    pub(crate) fn new(options: TlsOptions) -> Result<TlsConfig> {
        let verify_hostname = match options.allow_invalid_hostnames {
            Some(b) => !b,
            None => true,
        };

        let connector = make_openssl_connector(options).map_err(|e| {
            Error::from(ErrorKind::InvalidTlsConfig {
                message: e.to_string(),
            })
        })?;

        Ok(TlsConfig {
            connector,
            verify_hostname,
        })
    }
}

pub(super) async fn tls_connect(
    host: &str,
    tcp_stream: TcpStream,
    cfg: &TlsConfig,
) -> Result<TlsStream> {
    init_trust();

    let mut stream = make_ssl_stream(host, tcp_stream, cfg).map_err(|err| {
        Error::from(ErrorKind::InvalidTlsConfig {
            message: err.to_string(),
        })
    })?;
    Pin::new(&mut stream).connect().await.map_err(|err| {
        use std::io;
        match err.into_io_error() {
            Ok(err) => err,
            Err(err) => io::Error::new(io::ErrorKind::Other, err),
        }
    })?;
    Ok(stream)
}

fn make_openssl_connector(cfg: TlsOptions) -> std::result::Result<SslConnector, ErrorStack> {
    let mut builder = SslConnector::builder(SslMethod::tls_client())?;

    let TlsOptions {
        allow_invalid_certificates,
        ca_file_path,
        cert_key_file_path,
        allow_invalid_hostnames: _,
    } = cfg;

    if let Some(true) = allow_invalid_certificates {
        builder.set_verify(SslVerifyMode::NONE);
    }
    if let Some(path) = ca_file_path {
        builder.set_ca_file(path)?;
    }
    if let Some(path) = cert_key_file_path {
        builder.set_certificate_file(path.clone(), SslFiletype::PEM)?;
        builder.set_private_key_file(path, SslFiletype::PEM)?;
    }

    Ok(builder.build())
}

fn init_trust() {
    static ONCE: Once = Once::new();
    ONCE.call_once(openssl_probe::init_ssl_cert_env_vars);
}

fn make_ssl_stream(
    host: &str,
    tcp_stream: TcpStream,
    cfg: &TlsConfig,
) -> std::result::Result<SslStream<TcpStream>, ErrorStack> {
    let ssl = cfg
        .connector
        .configure()?
        .use_server_name_indication(true)
        .verify_hostname(cfg.verify_hostname)
        .into_ssl(host)?;
    SslStream::new(ssl, tcp_stream)
}
