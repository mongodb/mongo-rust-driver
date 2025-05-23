use std::time::Duration;

use futures_util::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    bson::{doc, Bson, Document},
    error::{Error, ErrorKind, GridFsErrorKind},
    gridfs::{GridFsBucket, GridFsFindOneOptions, GridFsUploadStream},
    options::{FindOneOptions, GridFsBucketOptions, GridFsUploadOptions},
    runtime,
    test::{
        fail_command_supported,
        get_client_options,
        spec::unified_runner::run_unified_tests,
        topology_is_sharded,
        util::fail_point::{FailPoint, FailPointMode},
    },
    Client,
};

#[tokio::test]
async fn run_unified() {
    run_unified_tests(&["gridfs"])
        // The Rust driver doesn't support the disableMD5 option.
        .skip_files(&["upload-disableMD5.json"])
        .await;
}

#[tokio::test]
async fn download_stream_across_buffers() {
    let client = Client::for_test().await;

    let options = GridFsBucketOptions::builder().chunk_size_bytes(3).build();
    let bucket = client
        .database("download_stream_across_buffers")
        .gridfs_bucket(options);
    bucket.drop().await.unwrap();

    let data: Vec<u8> = (0..20).collect();
    let id = {
        let mut stream = bucket.open_upload_stream("test").await.unwrap();
        stream.write_all(&data[..]).await.unwrap();
        stream.close().await.unwrap();
        stream.id().clone()
    };

    let mut download_stream = bucket.open_download_stream(id).await.unwrap();
    let mut buf = vec![0u8; 12];

    // read in a partial chunk
    download_stream.read_exact(&mut buf[..1]).await.unwrap();
    assert_eq!(&buf[..1], &data[..1]);

    // read in the rest of the cached chunk
    download_stream.read_exact(&mut buf[1..3]).await.unwrap();
    assert_eq!(&buf[..3], &data[..3]);

    // read in multiple full chunks and one byte of a chunk
    download_stream.read_exact(&mut buf[3..10]).await.unwrap();
    assert_eq!(&buf[..10], &data[..10]);

    // read in one more byte from the buffered chunk
    download_stream.read_exact(&mut buf[10..11]).await.unwrap();
    assert_eq!(&buf[..11], &data[..11]);

    // read in the last byte from the buffered chunk
    download_stream.read_exact(&mut buf[11..12]).await.unwrap();
    assert_eq!(&buf[..12], &data[..12]);

    // read in the rest of the data
    download_stream.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, data);
}

#[tokio::test]
async fn upload_stream() {
    let client = Client::for_test().await;
    let bucket_options = GridFsBucketOptions::builder().chunk_size_bytes(4).build();
    let bucket = client
        .database("upload_stream")
        .gridfs_bucket(bucket_options);
    bucket.drop().await.unwrap();

    upload_test(&bucket, &[], None).await;
    upload_test(&bucket, &[11], None).await;
    upload_test(&bucket, &[11, 22, 33], None).await;
    upload_test(&bucket, &[11, 22, 33, 44], None).await;
    upload_test(&bucket, &[11, 22, 33, 44, 55], None).await;
    upload_test(&bucket, &[11, 22, 33, 44, 55, 66, 77, 88], None).await;
    upload_test(
        &bucket,
        &[11],
        Some(
            GridFsUploadOptions::builder()
                .metadata(doc! { "x": 1 })
                .build(),
        ),
    )
    .await;
}

async fn upload_test(bucket: &GridFsBucket, data: &[u8], options: Option<GridFsUploadOptions>) {
    let filename = format!(
        "length_{}_{}_options",
        data.len(),
        if options.is_some() { "with" } else { "without" }
    );
    let mut upload_stream = bucket
        .open_upload_stream(&filename)
        .with_options(options.clone())
        .await
        .unwrap();
    upload_stream.write_all(data).await.unwrap();
    upload_stream.close().await.unwrap();

    let mut uploaded = Vec::new();
    bucket
        .open_download_stream(upload_stream.id().clone())
        .await
        .unwrap()
        .read_to_end(&mut uploaded)
        .await
        .unwrap();
    assert_eq!(data, &uploaded);

    let file = bucket
        .find_one(doc! { "_id": upload_stream.id() })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(file.metadata, options.and_then(|opts| opts.metadata));
    assert_eq!(file.filename, Some(filename));
}

#[tokio::test]
async fn upload_stream_multiple_buffers() {
    let client = Client::for_test().await;
    let bucket_options = GridFsBucketOptions::builder().chunk_size_bytes(3).build();
    let bucket = client
        .database("upload_stream_multiple_buffers")
        .gridfs_bucket(bucket_options);
    bucket.drop().await.unwrap();

    let mut upload_stream = bucket
        .open_upload_stream("upload_stream_multiple_buffers")
        .await
        .unwrap();

    let data: Vec<u8> = (0..20).collect();

    // exactly one chunk
    upload_stream.write_all(&data[..3]).await.unwrap();

    // partial chunk
    upload_stream.write_all(&data[3..5]).await.unwrap();

    // rest of chunk
    upload_stream.write_all(&data[5..6]).await.unwrap();

    // multiple chunks
    upload_stream.write_all(&data[6..12]).await.unwrap();

    // one byte
    upload_stream.write_all(&data[12..13]).await.unwrap();

    // one more byte
    upload_stream.write_all(&data[13..14]).await.unwrap();

    // rest of chunk and partial chunk
    upload_stream.write_all(&data[14..18]).await.unwrap();

    // rest of data
    upload_stream.write_all(&data[18..20]).await.unwrap();

    // flush should do nothing; make sure that's the case
    upload_stream.flush().await.unwrap();

    // close stream
    upload_stream.close().await.unwrap();

    let mut uploaded = Vec::new();
    bucket
        .open_download_stream(upload_stream.id().clone())
        .await
        .unwrap()
        .read_to_end(&mut uploaded)
        .await
        .unwrap();
    assert_eq!(uploaded, data);
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_stream_errors() {
    let mut options = get_client_options().await.clone();
    if topology_is_sharded().await {
        options.hosts.drain(1..);
    }
    let client = Client::for_test().options(options).await;

    let bucket = client.database("upload_stream_errors").gridfs_bucket(None);
    bucket.drop().await.unwrap();

    // Error attempting to write to stream after closing.
    let mut upload_stream = bucket
        .open_upload_stream("upload_stream_errors")
        .await
        .unwrap();
    upload_stream.close().await.unwrap();
    assert_closed(&bucket, upload_stream).await;

    // Error attempting to write to stream after abort.
    let mut upload_stream = bucket
        .open_upload_stream("upload_stream_errors")
        .await
        .unwrap();
    upload_stream.abort().await.unwrap();
    assert_closed(&bucket, upload_stream).await;

    if !fail_command_supported().await {
        return;
    }

    // Error attempting to write to stream after write failure.
    let mut upload_stream = bucket
        .open_upload_stream("upload_stream_errors")
        .chunk_size_bytes(1)
        .await
        .unwrap();

    let fail_point = FailPoint::fail_command(&["insert"], FailPointMode::Times(1)).error_code(1234);
    let _guard = client.enable_fail_point(fail_point).await.unwrap();

    let error = get_mongo_error(upload_stream.write_all(&[11]).await);
    assert_eq!(error.sdam_code(), Some(1234));

    assert_closed(&bucket, upload_stream).await;

    // Error attempting to write to stream after close failure.
    let mut upload_stream = bucket
        .open_upload_stream("upload_stream_errors")
        .chunk_size_bytes(1)
        .await
        .unwrap();

    upload_stream.write_all(&[11]).await.unwrap();

    let fail_point = FailPoint::fail_command(&["insert"], FailPointMode::Times(1)).error_code(1234);
    let _guard = client.enable_fail_point(fail_point).await.unwrap();

    let error = get_mongo_error(upload_stream.close().await);
    assert_eq!(error.sdam_code(), Some(1234));

    assert_closed(&bucket, upload_stream).await;
}

#[tokio::test]
async fn drop_aborts() {
    let client = Client::for_test().await;
    let bucket = client.database("upload_stream_abort").gridfs_bucket(None);
    bucket.drop().await.unwrap();

    let mut upload_stream = bucket
        .open_upload_stream("upload_stream_abort")
        .await
        .unwrap();
    let id = upload_stream.id().clone();
    upload_stream.write_all(&[11]).await.unwrap();
    drop(upload_stream);

    assert_no_chunks_written(&bucket, &id).await;
}

#[tokio::test]
async fn write_future_dropped() {
    let client = Client::for_test().await;
    let bucket = client
        .database("upload_stream_abort")
        .gridfs_bucket(GridFsBucketOptions::builder().chunk_size_bytes(1).build());
    bucket.drop().await.unwrap();

    let mut upload_stream = bucket
        .open_upload_stream("upload_stream_abort")
        .await
        .unwrap();
    let chunks = vec![0u8; 100_000];

    assert!(
        runtime::timeout(Duration::from_millis(1), upload_stream.write(&chunks))
            .await
            .is_err()
    );

    let close_error = get_mongo_error(upload_stream.close().await);
    assert!(matches!(
        *close_error.kind,
        ErrorKind::GridFs(GridFsErrorKind::WriteInProgress)
    ));
}

async fn assert_closed(bucket: &GridFsBucket, mut upload_stream: GridFsUploadStream) {
    assert_shut_down_error(upload_stream.write_all(&[11]).await);
    assert_shut_down_error(upload_stream.close().await);
    assert_shut_down_error(upload_stream.flush().await);
    let abort_error = upload_stream.abort().await.unwrap_err();
    assert!(matches!(
        *abort_error.kind,
        ErrorKind::GridFs(GridFsErrorKind::UploadStreamClosed)
    ));

    assert_no_chunks_written(bucket, upload_stream.id()).await;
}

fn assert_shut_down_error(result: std::result::Result<(), futures_io::Error>) {
    let error = get_mongo_error(result);
    assert!(matches!(
        *error.kind,
        ErrorKind::GridFs(GridFsErrorKind::UploadStreamClosed)
    ));
}

fn get_mongo_error(result: std::result::Result<(), futures_io::Error>) -> Error {
    *result
        .unwrap_err()
        .into_inner()
        .unwrap()
        .downcast::<Error>()
        .unwrap()
}

async fn assert_no_chunks_written(bucket: &GridFsBucket, id: &Bson) {
    assert!(bucket
        .chunks()
        .clone_with_type::<Document>()
        .find_one(doc! { "files_id": id })
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn test_gridfs_bucket_find_one() {
    let data = &[1, 2, 3, 4];
    let client = Client::for_test().await;

    let options = GridFsBucketOptions::default();
    let bucket = client.database("gridfs_find_one").gridfs_bucket(options);

    let filename = String::from("somefile");
    let mut upload_stream = bucket.open_upload_stream(&filename).await.unwrap();
    upload_stream.write_all(data).await.unwrap();
    upload_stream.close().await.unwrap();

    let found = bucket
        .find_one(doc! { "_id": upload_stream.id() })
        .await
        .unwrap()
        .unwrap();

    assert_eq!(&found.id, upload_stream.id());
    assert_eq!(found.length, 4);
    assert_eq!(found.filename, Some(filename));
}

#[test]
fn test_gridfs_find_one_options_from() {
    let default_options = GridFsFindOneOptions::default();
    let find_one_options = FindOneOptions::from(default_options);
    assert_eq!(find_one_options.max_time, None);
    assert_eq!(find_one_options.skip, None);
    assert_eq!(find_one_options.sort, None);

    let options = GridFsFindOneOptions::builder()
        .sort(doc! { "foo": -1 })
        .skip(1)
        .max_time(Duration::from_millis(42))
        .build();

    let find_one_options = FindOneOptions::from(options);
    assert_eq!(find_one_options.max_time, Some(Duration::from_millis(42)));
    assert_eq!(find_one_options.skip, Some(1));
    assert_eq!(find_one_options.sort, Some(doc! {"foo": -1}));
}
