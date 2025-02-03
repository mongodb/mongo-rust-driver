use anyhow::Result;
use mongodb::bson::Document;

use crate::bench::Benchmark;

pub struct BsonDecodeBenchmark {
    num_iter: usize,
    bytes: Vec<u8>,
}

pub struct Options {
    pub num_iter: usize,
    pub doc: Document,
}

#[async_trait::async_trait]
impl Benchmark for BsonDecodeBenchmark {
    type Options = Options;
    type TaskState = ();

    async fn setup(options: Self::Options) -> Result<Self> {
        let mut bytes: Vec<u8> = Vec::new();
        options.doc.to_writer(&mut bytes)?;

        Ok(BsonDecodeBenchmark {
            num_iter: options.num_iter,
            bytes,
        })
    }

    async fn do_task(&self, _state: Self::TaskState) -> Result<()> {
        for _ in 0..self.num_iter {
            let _doc = Document::from_reader(&self.bytes[..])?;
        }

        Ok(())
    }
}
