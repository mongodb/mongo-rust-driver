use std::{
    fs::File,
    io::Read,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
        LazyLock,
    },
};

use futures::FutureExt;
use reqwest::{Certificate, Client as HttpClient};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    bson::{doc, spec::ElementType, Document},
    client_encryption::{ClientEncryption, KmsConnectCallback},
    error::{Error, Result},
    options::{ServerAddress, TlsOptions},
    runtime::{tls_connect, TlsConfig, TlsStream},
    test::get_client_options,
    Client,
};

use super::{
    AWS_KMS,
    AWS_MASTER_KEY,
    CSFLE_TLS_CERT_DIR,
    DISABLE_CRYPT_SHARED,
    EXTRA_OPTIONS,
    KV_NAMESPACE,
};

#[derive(Clone, Copy)]
enum ProxyKind {
    Http,
    Https,
}

impl ProxyKind {
    fn http_client(&self) -> &HttpClient {
        match self {
            Self::Http => {
                static HTTP_CLIENT: LazyLock<HttpClient> = LazyLock::new(Default::default);
                &HTTP_CLIENT
            }
            Self::Https => {
                static HTTPS_CLIENT: LazyLock<HttpClient> = LazyLock::new(|| {
                    let ca_file_path = PathBuf::from(&*CSFLE_TLS_CERT_DIR).join("ca.pem");
                    let mut pem = Vec::new();
                    File::open(ca_file_path)
                        .unwrap()
                        .read_to_end(&mut pem)
                        .unwrap();
                    let certificate = Certificate::from_pem(&pem).unwrap();
                    HttpClient::builder()
                        .add_root_certificate(certificate)
                        .build()
                        .unwrap()
                });
                &HTTPS_CLIENT
            }
        }
    }

    fn prefix(&self) -> &str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }

    fn port(&self) -> u16 {
        match self {
            Self::Http => 9004,
            Self::Https => 9005,
        }
    }
}

async fn reset_metrics(proxy_kind: ProxyKind) {
    proxy_kind
        .http_client()
        .post(format!(
            "{}://127.0.0.1:{}/reset",
            proxy_kind.prefix(),
            proxy_kind.port()
        ))
        .send()
        .await
        .unwrap();
}

async fn get_connect_count(proxy_kind: ProxyKind) -> usize {
    let metrics = proxy_kind
        .http_client()
        .get(format!(
            "{}://127.0.0.1:{}/metrics",
            proxy_kind.prefix(),
            proxy_kind.port()
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    // the metrics response looks something like:
    // "connect_count 1\nconnect_target kms.us-east-1.amazonaws.com:443\n"
    for metric in metrics.split("\n") {
        let (k, v) = metric.split_once(" ").expect(metric);
        if k == "connect_count" {
            return v.parse::<usize>().expect(v);
        }
    }
    panic!("connect_count not reported: {metrics}");
}

async fn make_http_proxy_stream(address: ServerAddress) -> Result<TcpStream> {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", ProxyKind::Http.port())).await?;
    connect_to_proxy(&mut stream, address).await?;
    Ok(stream)
}

async fn make_https_proxy_stream(address: ServerAddress) -> Result<TlsStream<TcpStream>> {
    let tcp_stream = TcpStream::connect(format!("127.0.0.1:{}", ProxyKind::Https.port())).await?;

    let ca_file_path = PathBuf::from(&*CSFLE_TLS_CERT_DIR).join("ca.pem");
    let tls_config = TlsConfig::new(TlsOptions::builder().ca_file_path(ca_file_path).build())?;
    let mut tls_stream = tls_connect("127.0.0.1", tcp_stream, &tls_config).await?;

    connect_to_proxy(&mut tls_stream, address).await?;
    Ok(tls_stream)
}

async fn connect_to_proxy(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    address: ServerAddress,
) -> Result<()> {
    let connect = format!("CONNECT {address} HTTP/1.1\r\nHost: {address}\r\n\r\n");
    stream.write_all(connect.as_bytes()).await?;

    let expected = b"HTTP/1.1 200";
    let mut buf = vec![0; 100]; // as of writing, the server's response is ~40 bytes
    let _ = stream.read(&mut buf).await?;
    if &buf[0..expected.len()] != expected {
        return Err(Error::custom(format!("invalid response: {buf:?}")));
    }

    Ok(())
}

async fn test_explicit_encryption(
    proxy_kind: ProxyKind,
    callback: KmsConnectCallback,
) -> Result<()> {
    reset_metrics(proxy_kind).await;

    let client_encryption = ClientEncryption::builder(
        Client::for_test().await.into_client(),
        KV_NAMESPACE.clone(),
        vec![AWS_KMS.clone()],
    )
    .kms_connect_callback(callback)
    .build()?;

    client_encryption
        .create_data_key(AWS_MASTER_KEY.clone())
        .await?;

    let connect_count = get_connect_count(proxy_kind).await;
    assert!(connect_count >= 1);

    Ok(())
}

/// Case 1
#[tokio::test]
async fn plain_http_proxy() {
    test_explicit_encryption(
        ProxyKind::Http,
        KmsConnectCallback::new(move |server_address: ServerAddress| {
            async move { make_http_proxy_stream(server_address).await }.boxed()
        }),
    )
    .await
    .unwrap();
}

/// Case 2
#[tokio::test]
async fn https_proxy() {
    test_explicit_encryption(
        ProxyKind::Https,
        KmsConnectCallback::new(move |server_address: ServerAddress| {
            async move { make_https_proxy_stream(server_address).await }.boxed()
        }),
    )
    .await
    .unwrap();
}

/// Case 3
#[tokio::test]
async fn proxy_auto_encryption() {
    let client = Client::for_test().await;
    client
        .database("keyvault")
        .collection::<()>("datakeys")
        .drop()
        .await
        .unwrap();
    client
        .database("db")
        .collection::<()>("coll")
        .drop()
        .await
        .unwrap();

    let client_encryption = ClientEncryption::builder(
        client.clone().into_client(),
        KV_NAMESPACE.clone(),
        vec![AWS_KMS.clone()],
    )
    .kms_connect_callback(KmsConnectCallback::new(move |server_address| {
        async move { make_http_proxy_stream(server_address).await }.boxed()
    }))
    .build()
    .unwrap();
    let data_key_id = client_encryption
        .create_data_key(AWS_MASTER_KEY.clone())
        .await
        .unwrap();

    let schema = doc! {
        "bsonType": "object",
        "properties": {
            "encrypted_string": {
                "encrypt": {
                    "keyId": [data_key_id],
                    "bsonType": "string",
                    "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                }
            }
        }
    };

    reset_metrics(ProxyKind::Http).await;

    let client_encrypted = Client::encrypted_builder(
        get_client_options().await.clone(),
        KV_NAMESPACE.clone(),
        vec![AWS_KMS.clone()],
    )
    .unwrap()
    .schema_map(vec![("db.coll", schema)])
    .kms_connect_callback(KmsConnectCallback::new(
        move |server_address: ServerAddress| {
            async move { make_http_proxy_stream(server_address).await }.boxed()
        },
    ))
    .extra_options(EXTRA_OPTIONS.clone())
    .disable_crypt_shared(*DISABLE_CRYPT_SHARED)
    .build()
    .await
    .unwrap();

    let coll = client_encrypted.database("db").collection("coll");
    coll.insert_one(doc! { "_id": 1, "encrypted_string": "hello" })
        .await
        .unwrap();
    let doc = coll.find_one(doc! { "_id": 1 }).await.unwrap().unwrap();
    assert_eq!(doc.get_str("encrypted_string").unwrap(), "hello");

    let doc: Document = client
        .database("db")
        .collection("coll")
        .find_one(doc! { "_id": 1 })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        doc.get("encrypted_string").unwrap().element_type(),
        ElementType::Binary
    );

    let connect_count = get_connect_count(ProxyKind::Http).await;
    assert!(connect_count >= 1);
}

/// Case 4
#[tokio::test]
async fn error() {
    test_explicit_encryption(
        ProxyKind::Http,
        KmsConnectCallback::new(move |_: ServerAddress| {
            async move { Err(Error::custom("error")) as Result<TcpStream> }.boxed()
        }),
    )
    .await
    .unwrap_err();
}

// Case 5 skipped: CSOT not supported

#[tokio::test]
async fn retry() {
    let attempt = Arc::new(AtomicUsize::new(0));
    let closure_attempt = attempt.clone();
    test_explicit_encryption(
        ProxyKind::Http,
        KmsConnectCallback::new(move |server_address: ServerAddress| {
            let attempt = closure_attempt.clone();
            async move {
                if attempt.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(std::io::Error::other("mock network error").into())
                } else {
                    make_http_proxy_stream(server_address).await
                }
            }
            .boxed()
        }),
    )
    .await
    .unwrap();
    assert!(attempt.load(Ordering::SeqCst) >= 2);
}
