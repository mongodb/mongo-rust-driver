use crate::{options::Compressor, test::get_client_options};

// Verifies that a compressor is properly set when a compression feature flag is enabled. Actual
// compression behavior is tested by running the driver test suite with a compressor configured;
// this test just makes sure our setup is correct.
#[tokio::test]
async fn test_compression_enabled() {
    let options = get_client_options().await;
    let compressors = options
        .compressors
        .as_ref()
        .expect("compressors client option should be set when compression is enabled");

    #[cfg(feature = "zstd-compression")]
    assert!(compressors
        .iter()
        .any(|compressor| matches!(compressor, Compressor::Zstd { .. })));

    #[cfg(feature = "zlib-compression")]
    assert!(compressors
        .iter()
        .any(|compressor| matches!(compressor, Compressor::Zlib { .. })));

    #[cfg(feature = "snappy-compression")]
    assert!(compressors
        .iter()
        .any(|compressor| matches!(compressor, Compressor::Snappy)));
}
