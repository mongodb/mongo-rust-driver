use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::PathBuf,
};

use mongodb::{Client, Collection, Database};

use crate::{
    bench::{parse_json_file_to_documents, Benchmark},
    error::Result,
};

const TOTAL_FILES: usize = 100;

pub struct JsonMultiExportBenchmark {
    db: Database,
    coll: Collection,
    num_threads: usize,
    path: PathBuf,
}

// Specifies the options to a `bench::json_multi_export::setup` operation.
pub struct Options {
    pub num_threads: usize,
    pub path: PathBuf,
    pub uri: String,
}

impl Benchmark for JsonMultiExportBenchmark {
    type Options = Options;

    fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri)?;
        let db = client.database("perftest");
        db.drop()?;

        // We need to create a collection in order to populate the field of the InsertManyBenchmark
        // being returned, so we create a placeholder that gets overwritten in before_task().
        let coll = db.collection("placeholder");

        for i in 0..TOTAL_FILES {
            let json_file_name = options.path.join(format!("ldjson{:03}.txt", i));
            let file = File::open(&json_file_name)?;

            let mut docs = parse_json_file_to_documents(file)?;

            loop {
                let mut doc = match docs.pop() {
                    Some(doc) => doc,
                    None => break,
                };
                doc.insert("file", i as i32);
                coll.insert_one(doc, None)?;
            }
        }

        Ok(JsonMultiExportBenchmark {
            db,
            coll,
            num_threads: options.num_threads,
            path: options.path,
        })
    }

    fn before_task(&mut self) -> Result<()> {
        self.coll = self.db.collection("corpus");
        self.coll.drop()?;

        for i in 0..TOTAL_FILES {
            fs::remove_file(self.path.join(format!("ldjson{:03}.txt", i)))?;
        }

        Ok(())
    }

    fn do_task(&self) -> Result<()> {
        let mut num_each = TOTAL_FILES / self.num_threads;
        let remainder = TOTAL_FILES % self.num_threads;
        if remainder != 0 {
            num_each += 1;
        }

        let mut downloaded_files = 0;
        let mut threads = Vec::new();
        while downloaded_files < TOTAL_FILES {
            let num_files = std::cmp::min(TOTAL_FILES - downloaded_files, num_each);
            let coll_ref = self.coll.clone();
            let path_ref = self.path.clone();

            let thread = std::thread::spawn(move || {
                // Note that errors are unwrapped within threads instead of propagated with `?`.
                // While we could set up a channel to send errors back to main thread, this is a lot
                // of work for little gain since we `unwrap()` in main.rs anyway.
                for i in downloaded_files..downloaded_files + num_files {
                    let file_name = path_ref.join(format!("ldjson{:03}.txt", i));
                    let mut file = OpenOptions::new()
                        .create(true)
                        .write(true)
                        .open(&file_name)
                        .unwrap();

                    let cursor = coll_ref
                        .find(Some(doc! { "file": i as i32 }), None)
                        .unwrap();

                    for doc in cursor {
                        writeln!(file, "{}", doc.unwrap().to_string()).unwrap();
                    }
                }
            });
            threads.push(thread);

            downloaded_files += num_each;
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
