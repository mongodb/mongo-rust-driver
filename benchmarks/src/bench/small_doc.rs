use std::fs::File;

use bson::Document;
use mongodb::Client;

use crate::{bench::Benchmark, error::Result};

struct InsertOneBenchmark {
    doc: Document,
    client: Client,
}

impl Benchmark for InsertOneBenchmark {
    fn setup() -> Result<Self> {
        let client = Client::with_uri_str("mongodb://localhost:27017")?;
        let db = client.database("perftest");
        db.drop()?;
    }

    fn before_task(&self) {}
}
