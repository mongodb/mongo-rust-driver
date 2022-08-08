use crate::{gridfs::options::GridFsBucketOptions, selection_criteria::{SelectionCriteria, ReadPreference}};
#[cfg(test)]
use crate::test::TestClient;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bson::doc;

#[tokio::test]
async fn test_gridfs_bucket() {
    let client = TestClient::new().await;

    let db = client.database("testdb");
    let bucket = db.gridfs_bucket(None);
    println!("x 4");

    let mut upload_stream = bucket.open_upload_stream("foo", None).await;
    println!("x 5");

    let data = "hello world".as_bytes();
    println!("x 6");

    println!("this is the data being written: {:?}", &data);

    upload_stream.write(data).await.unwrap();
    println!("x 8");

    upload_stream.finish().await.unwrap();
    println!("x 9");

    let mut download_stream = bucket.open_download_stream_by_name("foo".to_string(), None).await.unwrap();
    println!("x 10");
    let mut buf = [0u8; 200];

    println!("x 11");
    download_stream.read(&mut buf).await.unwrap();
    println!("x 12");
    dbg!(String::from_utf8(buf.to_vec()));
    println!("x 13");
}