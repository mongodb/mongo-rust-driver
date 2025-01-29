use std::{convert::TryInto, path::PathBuf};

use anyhow::{bail, Result};
use mongodb::bson::{Bson, Document};
use serde_json::Value;

use crate::{bench::Benchmark, fs::read_to_string};

pub struct BsonEncodeBenchmark {
    num_iter: usize,
    doc: Document,
}

pub struct Options {
    pub num_iter: usize,
    pub path: PathBuf,
}

#[async_trait::async_trait]
impl Benchmark for BsonEncodeBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let mut file = read_to_string(&options.path).await?;

        let json: Value = serde_json::from_str(&mut file)?;
        let doc = match json.try_into()? {
            Bson::Document(doc) => doc,
            _ => bail!("invalid json test file"),
        };

        Ok(BsonEncodeBenchmark {
            num_iter: options.num_iter,
            doc,
        })
    }

    async fn do_task(&mut self) -> Result<()> {
        for _ in 0..self.num_iter {
            let mut bytes: Vec<u8> = Vec::new();
            self.doc.to_writer(&mut bytes)?;
        }

        Ok(())
    }
}
