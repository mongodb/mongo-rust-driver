use std::{
    fs::File,
    path::{Path, PathBuf},
};

use mongodb::bson::Document;
use std::sync::LazyLock;
use tokio::sync::OnceCell;

pub static DATA_PATH: LazyLock<PathBuf> =
    LazyLock::new(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("data"));

async fn get_data(once_cell: &OnceCell<Document>, path: &[&str]) -> Document {
    once_cell
        .get_or_init(|| async {
            let mut data_path = DATA_PATH.clone();
            data_path.extend(path);
            let file = tokio::task::spawn_blocking(|| File::open(data_path))
                .await
                .unwrap()
                .unwrap();
            tokio::task::spawn_blocking(|| serde_json::from_reader(file))
                .await
                .unwrap()
                .unwrap()
        })
        .await
        .clone()
}

pub async fn get_small_doc() -> Document {
    static SMALL_DOC: OnceCell<Document> = OnceCell::const_new();
    get_data(&SMALL_DOC, &["single_and_multi_document", "small_doc.json"]).await
}

pub async fn get_large_doc() -> Document {
    static LARGE_DOC: OnceCell<Document> = OnceCell::const_new();
    get_data(&LARGE_DOC, &["single_and_multi_document", "large_doc.json"]).await
}

pub async fn get_flat_bson() -> Document {
    static FLAT_BSON: OnceCell<Document> = OnceCell::const_new();
    get_data(&FLAT_BSON, &["extended_bson", "flat_bson.json"]).await
}

pub async fn get_deep_bson() -> Document {
    static DEEP_BSON: OnceCell<Document> = OnceCell::const_new();
    get_data(&DEEP_BSON, &["extended_bson", "deep_bson.json"]).await
}

pub async fn get_full_bson() -> Document {
    static FULL_BSON: OnceCell<Document> = OnceCell::const_new();
    get_data(&FULL_BSON, &["extended_bson", "full_bson.json"]).await
}

pub async fn get_tweet() -> Document {
    static TWEET: OnceCell<Document> = OnceCell::const_new();
    get_data(&TWEET, &["single_and_multi_document", "tweet.json"]).await
}
