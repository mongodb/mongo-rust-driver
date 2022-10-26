use futures_util::io::AsyncReadExt;

use crate::{
    gridfs::options::GridFsBucketOptions,
    test::{
        run_spec_test_with_path,
        spec::unified_runner::{run_unified_format_test_filtered, TestCase},
        TestClient,
        LOCK,
    },
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard = LOCK.run_concurrently().await;
    run_spec_test_with_path(&["gridfs"], |path, f| {
        run_unified_format_test_filtered(path, f, test_predicate)
    })
    .await;
}

fn test_predicate(test: &TestCase) -> bool {
    let lower = test.description.to_lowercase();

    // The Rust driver doesn't support the disableMD5 and contentType options for upload.
    !lower.contains("sans md5") && !lower.contains("contenttype")
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn download_stream_across_buffers() {
    let _guard = LOCK.run_concurrently().await;

    let client = TestClient::new().await;

    let options = GridFsBucketOptions::builder().chunk_size_bytes(2).build();
    let bucket = client
        .database("download_stream_across_buffers")
        .gridfs_bucket(options);
    bucket.drop().await.unwrap();

    let data: Vec<u8> = (0..10).collect();
    let id = bucket
        .upload_from_futures_0_3_reader("test", &data[..], None)
        .await
        .unwrap();

    let mut download_stream = bucket.open_download_stream(id.into()).await.unwrap();
    let mut buf = vec![0u8; 6];

    download_stream.read_exact(&mut buf[..1]).await.unwrap();
    assert_eq!(&buf[..1], &data[..1]);

    download_stream.read_exact(&mut buf[1..4]).await.unwrap();
    assert_eq!(&buf[1..4], &data[1..4]);

    download_stream.read_exact(&mut buf[4..6]).await.unwrap();
    assert_eq!(&buf[4..6], &data[4..6]);

    download_stream.read_to_end(&mut buf).await.unwrap();
    assert_eq!(&buf, &data);
}
