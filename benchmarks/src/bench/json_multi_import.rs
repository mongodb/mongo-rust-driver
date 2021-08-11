use std::path::PathBuf;

use anyhow::Result;
use futures::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use mongodb::{bson::Document, options::InsertManyOptions, Client, Collection, Database};
use serde::{Deserialize, Serialize};

use crate::{
    bench::{Benchmark, COLL_NAME, DATABASE_NAME},
    fs::{BufReader, File},
};

const TOTAL_FILES: usize = 100;

pub struct JsonMultiImportBenchmark {
    db: Database,
    coll: Collection<Tweet>,
    path: PathBuf,
}

// Specifies the options to a `JsonMultiImportBenchmark::setup` operation.
pub struct Options {
    pub path: PathBuf,
    pub uri: String,
}

#[async_trait::async_trait]
impl Benchmark for JsonMultiImportBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        db.drop(None).await?;

        let coll = db.collection(&COLL_NAME);

        Ok(JsonMultiImportBenchmark {
            db,
            coll,
            path: options.path,
        })
    }

    async fn before_task(&mut self) -> Result<()> {
        self.coll.drop(None).await?;
        self.db.create_collection(COLL_NAME.as_str(), None).await?;

        Ok(())
    }

    async fn do_task(&self) -> Result<()> {
        let mut tasks = FuturesUnordered::new();

        for i in 0..TOTAL_FILES {
            let coll_ref = self.coll.clone();
            let path = self.path.clone();

            tasks.push(crate::spawn(async move {
                // Note that errors are unwrapped within threads instead of propagated with `?`.
                // While we could set up a channel to send errors back to main thread, this is a lot
                // of work for little gain since we `unwrap()` in main.rs anyway.
                let mut docs: Vec<Tweet> = Vec::new();

                let json_file_name = path.join(format!("ldjson{:03}.txt", i));
                let file = File::open_read(&json_file_name).await.unwrap();

                let mut lines = BufReader::new(file).lines();
                while let Some(line) = lines.try_next().await.unwrap() {
                    docs.push(serde_json::from_str(&line).unwrap());
                }

                let opts = Some(InsertManyOptions::builder().ordered(false).build());
                coll_ref.insert_many(docs, opts).await.unwrap();
            }));
        }

        while !tasks.is_empty() {
            tasks.next().await;
        }

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        self.db.drop(None).await?;

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Tweet {
    text: String,
    in_reply_to_status_id: i64,
    retweet_count: Option<i32>,
    contributors: Option<i32>,
    created_at: String,
    geo: Option<String>,
    source: String,
    coordinates: Option<String>,
    in_reply_to_screen_name: Option<String>,
    truncated: bool,
    entities: Entities,
    retweeted: bool,
    place: Option<String>,
    user: User,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Entities {
    user_mentions: Vec<Mention>,
    urls: Vec<String>,
    hashtags: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Mention {
    indices: Vec<i32>,
    screen_name: String,
    name: String,
    id: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct User {
    friends_count: i32,
    profile_sidebar_fill_color: String,
    location: String,
    verified: bool,
    follow_request_sent: Option<bool>,
    favourites_count: i32,
    profile_sidebar_border_color: String,
    profile_image_url: String,
    geo_enabled: bool,
    created_at: String,
    description: String,
    time_zone: String,
    url: String,
    screen_name: String,
    notifications: Option<Vec<Document>>,
    profile_background_color: String,
    listed_count: i32,
    lang: String,
}
