use bson::{doc, Bson};
use std::time::Duration;

use crate::{
    compression::{Compressor, CompressorID, Decoder},
    options::ClientOptions,
    test::TestClient,
};

#[test]
fn test_zlib_compressor() {
    let zlib_compressor = Compressor::Zlib { level: 4 };
    assert_eq!(CompressorID::ZlibID, zlib_compressor.to_compressor_id());
    let mut encoder = zlib_compressor.to_encoder().unwrap();
    assert!(encoder.write_all(b"foo").is_ok());
    assert!(encoder.write_all(b"bar").is_ok());
    assert!(encoder.write_all(b"ZLIB").is_ok());

    let compressed_bytes = encoder.finish().unwrap();

    let decoder = Decoder::from_u8(CompressorID::ZlibID as u8).unwrap();
    let original_bytes = decoder.decode(compressed_bytes.as_slice()).unwrap();
    assert_eq!(b"foobarZLIB", original_bytes.as_slice());
}

#[test]
fn test_zstd_compressor() {
    let zstd_compressor = Compressor::Zstd { level: 0 };
    assert_eq!(CompressorID::ZstdID, zstd_compressor.to_compressor_id());
    let mut encoder = zstd_compressor.to_encoder().unwrap();
    assert!(encoder.write_all(b"foo").is_ok());
    assert!(encoder.write_all(b"bar").is_ok());
    assert!(encoder.write_all(b"ZSTD").is_ok());

    let compressed_bytes = encoder.finish().unwrap();

    let decoder = Decoder::from_u8(CompressorID::ZstdID as u8).unwrap();
    let original_bytes = decoder.decode(compressed_bytes.as_slice()).unwrap();
    assert_eq!(b"foobarZSTD", original_bytes.as_slice());
}

#[test]
fn test_snappy_compressor() {
    let snappy_compressor = Compressor::Snappy;
    assert_eq!(CompressorID::SnappyID, snappy_compressor.to_compressor_id());
    let mut encoder = snappy_compressor.to_encoder().unwrap();
    assert!(encoder.write_all(b"foo").is_ok());
    assert!(encoder.write_all(b"bar").is_ok());
    assert!(encoder.write_all(b"SNAPPY").is_ok());

    let compressed_bytes = encoder.finish().unwrap();

    let decoder = Decoder::from_u8(CompressorID::SnappyID as u8).unwrap();
    let original_bytes = decoder.decode(compressed_bytes.as_slice()).unwrap();
    assert_eq!(b"foobarSNAPPY", original_bytes.as_slice());
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn ping_server_with_zlib_compression() {
    let client_options = ClientOptions::builder()
        .compressors(vec!["zlib".to_string()])
        .zlib_compression(4)
        .server_selection_timeout(Duration::new(2, 0))
        .build();

    let client = TestClient::with_options(Some(client_options)).await;
    let ret = client
        .database("admin")
        .run_command(doc! {"ping": 1}, None)
        .await;

    assert!(ret.is_ok());
    let ret = ret.unwrap();
    assert_eq!(ret.get("ok"), Some(Bson::Double(1.0)).as_ref());
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn ping_server_with_zstd_compression() {
    let client_options = ClientOptions::builder()
        .compressors(vec!["zstd".to_string()])
        .server_selection_timeout(Duration::new(2, 0))
        .build();

    let client = TestClient::with_options(Some(client_options)).await;
    let ret = client
        .database("admin")
        .run_command(doc! {"ping": 1}, None)
        .await;

    assert!(ret.is_ok());
    let ret = ret.unwrap();
    assert_eq!(ret.get("ok"), Some(Bson::Double(1.0)).as_ref());
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn ping_server_with_snappy_compression() {
    let client_options = ClientOptions::builder()
        .compressors(vec!["snappy".to_string()])
        .server_selection_timeout(Duration::new(2, 0))
        .build();

    let client = TestClient::with_options(Some(client_options)).await;
    let ret = client
        .database("admin")
        .run_command(doc! {"ping": 1}, None)
        .await;

    assert!(ret.is_ok());
    let ret = ret.unwrap();
    assert_eq!(ret.get("ok"), Some(Bson::Double(1.0)).as_ref());
}
