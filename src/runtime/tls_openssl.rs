use std::pin::Pin;
use std::sync::Once;
use std::task::{Context, Poll};

use futures_io::{AsyncRead, AsyncWrite};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode, SslFiletype};
use tokio::io::{AsyncWrite as TokioAsyncWrite};
use tokio_openssl::SslStream;

use crate::client::options::TlsOptions;
use crate::error::Result;

use super::stream::AsyncTcpStream;

#[derive(Debug)]
pub(crate) struct AsyncTlsStream {
    inner: SslStream<AsyncTcpStream>,
}

impl AsyncTlsStream {
    pub(crate) async fn connect(host: &str, tcp_stream: AsyncTcpStream, cfg: TlsOptions) -> Result<Self> {
        init_trust();

        let connector = make_openssl_connector(cfg)?;
        let ssl = connector.configure()?
            .use_server_name_indication(true)
            .verify_hostname(false)
            .into_ssl(host)?;
        let mut stream = SslStream::new(ssl, tcp_stream)?;
        Pin::new(&mut stream).connect().await?;
        Ok(AsyncTlsStream {
            inner: stream,
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

fn make_openssl_connector(cfg: TlsOptions) -> Result<SslConnector> {
    let mut builder = SslConnector::builder(SslMethod::tls_client())?;

    let TlsOptions {
        allow_invalid_certificates,
        ca_file_path,
        cert_key_file_path,
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