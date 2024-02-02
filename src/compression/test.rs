// Tests OP_COMPRESSED.  To actually test compression you need to look at
// server logs to see if decompression is happening.  Even if these tests
// are run against a server that does not support compression
// these tests won't fail because the messages will be sent without compression
// (as indicated in the specs).

use bson::{doc, Bson};

use crate::{
    client::options::ClientOptions,
    compression::{Compressor, CompressorId, Decoder},
    test::{get_client_options, TestClient},
};

#[cfg(feature = "zlib-compression")]
#[test]
fn test_zlib_compressor() {
    let zlib_compressor = Compressor::Zlib { level: Some(4) };
    assert_eq!(CompressorId::Zlib, zlib_compressor.id());
    let mut encoder = zlib_compressor.to_encoder().unwrap();
    assert!(encoder.write_all(b"foo").is_ok());
    assert!(encoder.write_all(b"bar").is_ok());
    assert!(encoder.write_all(b"ZLIB").is_ok());

    let compressed_bytes = encoder.finish().unwrap();

    let decoder = Decoder::from_u8(CompressorId::Zlib as u8).unwrap();
    let original_bytes = decoder.decode(compressed_bytes.as_slice()).unwrap();
    assert_eq!(b"foobarZLIB", original_bytes.as_slice());
}

#[cfg(feature = "zstd-compression")]
#[test]
fn test_zstd_compressor() {
    let zstd_compressor = Compressor::Zstd { level: None };
    assert_eq!(CompressorId::Zstd, zstd_compressor.id());
    let mut encoder = zstd_compressor.to_encoder().unwrap();
    assert!(encoder.write_all(b"foo").is_ok());
    assert!(encoder.write_all(b"bar").is_ok());
    assert!(encoder.write_all(b"ZSTD").is_ok());

    let compressed_bytes = encoder.finish().unwrap();

    let decoder = Decoder::from_u8(CompressorId::Zstd as u8).unwrap();
    let original_bytes = decoder.decode(compressed_bytes.as_slice()).unwrap();
    assert_eq!(b"foobarZSTD", original_bytes.as_slice());
}

#[cfg(feature = "snappy-compression")]
#[test]
fn test_snappy_compressor() {
    let snappy_compressor = Compressor::Snappy;
    assert_eq!(CompressorId::Snappy, snappy_compressor.id());
    let mut encoder = snappy_compressor.to_encoder().unwrap();
    assert!(encoder.write_all(b"foo").is_ok());
    assert!(encoder.write_all(b"bar").is_ok());
    assert!(encoder.write_all(b"SNAPPY").is_ok());

    let compressed_bytes = encoder.finish().unwrap();

    let decoder = Decoder::from_u8(CompressorId::Snappy as u8).unwrap();
    let original_bytes = decoder.decode(compressed_bytes.as_slice()).unwrap();
    assert_eq!(b"foobarSNAPPY", original_bytes.as_slice());
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[cfg(feature = "zlib-compression")]
async fn ping_server_with_zlib_compression() {
    let mut client_options = get_client_options().await.clone();
    client_options.compressors = Some(vec![Compressor::Zlib { level: Some(4) }]);
    send_ping_with_compression(client_options).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[cfg(feature = "zstd-compression")]
async fn ping_server_with_zstd_compression() {
    let mut client_options = get_client_options().await.clone();
    client_options.compressors = Some(vec![Compressor::Zstd { level: None }]);
    send_ping_with_compression(client_options).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[cfg(feature = "snappy-compression")]
async fn ping_server_with_snappy_compression() {
    let mut client_options = get_client_options().await.clone();
    client_options.compressors = Some(vec![Compressor::Snappy]);
    send_ping_with_compression(client_options).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[cfg(all(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
async fn ping_server_with_all_compressors() {
    let mut client_options = get_client_options().await.clone();
    client_options.compressors = Some(vec![
        Compressor::Zlib { level: None },
        Compressor::Snappy,
        Compressor::Zstd { level: None },
    ]);
    send_ping_with_compression(client_options).await;
}

async fn send_ping_with_compression(client_options: ClientOptions) {
    let client = TestClient::with_options(Some(client_options)).await;
    let ret = client.database("admin").run_command(doc! {"ping": 1}).await;

    assert!(ret.is_ok());
    let ret = ret.unwrap();
    assert_eq!(ret.get("ok"), Some(Bson::Double(1.0)).as_ref());
}
