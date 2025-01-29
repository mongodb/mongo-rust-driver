use std::{convert::TryInto, path::PathBuf};

use anyhow::{bail, Result};
use mongodb::bson::{Bson, Document};
use serde_json::Value;

use crate::{bench::Benchmark, fs::read_to_string};

pub struct BsonDecodeBenchmark {
    num_iter: usize,
    bytes: Vec<u8>,
}

pub struct Options {
    pub num_iter: usize,
    pub path: PathBuf,
}

#[async_trait::async_trait]
impl Benchmark for BsonDecodeBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let file = read_to_string(&options.path).await?;

        let json: Value = serde_json::from_str(&file)?;
        let doc = match json.try_into()? {
            Bson::Document(doc) => doc,
            _ => bail!("invalid json test file"),
        };

        let mut bytes: Vec<u8> = Vec::new();
        doc.to_writer(&mut bytes)?;

        Ok(BsonDecodeBenchmark {
            num_iter: options.num_iter,
            bytes,
        })
    }

    async fn do_task(&mut self) -> Result<()> {
        for _ in 0..self.num_iter {
            // `&[u8]` implements `Read`, and `from_reader` needs a `&mut R: Read`, so we need a
            // `&mut &[u8]`.
            let _doc = Document::from_reader(&mut &self.bytes[..])?;
        }

        Ok(())
    }
}
