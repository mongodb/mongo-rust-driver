use anyhow::Result;
use mongodb::bson::Document;

use crate::bench::Benchmark;

pub struct BsonEncodeBenchmark {
    num_iter: usize,
    doc: Document,
}

pub struct Options {
    pub num_iter: usize,
    pub doc: Document,
}

#[async_trait::async_trait]
impl Benchmark for BsonEncodeBenchmark {
    type Options = Options;
    type TaskState = ();

    async fn setup(options: Self::Options) -> Result<Self> {
        Ok(BsonEncodeBenchmark {
            num_iter: options.num_iter,
            doc: options.doc,
        })
    }

    async fn do_task(&self, _state: Self::TaskState) -> Result<()> {
        for _ in 0..self.num_iter {
            let mut bytes: Vec<u8> = Vec::new();
            self.doc.to_writer(&mut bytes)?;
        }

        Ok(())
    }
}
