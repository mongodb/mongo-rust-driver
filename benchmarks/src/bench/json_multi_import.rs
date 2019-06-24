use std::{fs::File, path::PathBuf};

use bson::Document;
use mongodb::{options::InsertManyOptions, Client, Collection, Database};

use crate::{
    bench::{parse_json_file_to_documents, Benchmark},
    error::Result,
};

const TOTAL_FILES: usize = 100;
const CHUNK_SIZE: usize = 40000;

pub struct JsonMultiImportBenchmark {
    db: Database,
    coll: Collection,
    num_threads: usize,
    path: PathBuf,
}

// Specifies the options to a `bench::json_multi_import::setup` operation.
pub struct Options {
    pub num_threads: usize,
    pub path: PathBuf,
    pub uri: String,
}

impl Benchmark for JsonMultiImportBenchmark {
    type Options = Options;

    fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri)?;
        let db = client.database("perftest");
        db.drop()?;

        // We need to create a collection in order to populate the field of the InsertManyBenchmark
        // being returned, so we create a placeholder that gets overwritten in before_task().
        let coll = db.collection("placeholder");

        Ok(JsonMultiImportBenchmark {
            db,
            coll,
            num_threads: options.num_threads,
            path: options.path,
        })
    }

    fn before_task(&mut self) -> Result<()> {
        self.coll = self.db.collection("corpus");
        self.coll.drop()?;

        Ok(())
    }

    fn do_task(&self) -> Result<()> {
        let mut num_each = TOTAL_FILES / self.num_threads;
        let remainder = TOTAL_FILES % self.num_threads;
        if remainder != 0 {
            num_each += 1;
        }

        let mut uploaded_files = 0;
        let mut threads = Vec::new();
        while uploaded_files < TOTAL_FILES {
            let num_files = std::cmp::min(TOTAL_FILES - uploaded_files, num_each);
            let coll_ref = self.coll.clone();
            let path_ref = self.path.clone();

            let thread = std::thread::spawn(move || {
                // Note that errors are unwrapped within threads instead of propagated with `?`.
                // While we could set up a channel to send errors back to main thread, this is a lot
                // of work for little gain since we `unwrap()` in main.rs anyway.
                let mut docs: Vec<Document> = Vec::new();

                for x in uploaded_files..uploaded_files + num_files {
                    let json_file_name = path_ref.join(format!("ldjson{:03}.txt", x));
                    let file = File::open(&json_file_name).unwrap();

                    let mut new_docs = parse_json_file_to_documents(file).unwrap();

                    docs.append(&mut new_docs);
                }

                // We can change this to a single `Collection::insert_many` once batching is
                // implemented in the driver TODO RUST-187.
                let opts = InsertManyOptions::builder().ordered(Some(false)).build();

                let mut doc_chunks: Vec<Vec<Document>> = Vec::new();
                while docs.len() > CHUNK_SIZE {
                    doc_chunks.push(docs.split_off(CHUNK_SIZE));
                }
                doc_chunks.push(docs);

                loop {
                    coll_ref
                        .insert_many(
                            match doc_chunks.pop() {
                                Some(chunk) => chunk,
                                None => break,
                            },
                            Some(opts.clone()),
                        )
                        .unwrap();
                }
            });
            threads.push(thread);

            uploaded_files += num_each;
        }

        for thread in threads {
            thread.join().unwrap();
        }

        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.db.drop()?;

        Ok(())
    }
}
