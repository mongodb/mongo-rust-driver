#[macro_use]
extern crate bson;

use std::thread;

use mongodb::{
    options::{CollectionOptions, InsertManyOptions},
    read_preference::ReadPreference,
    Client,
};

const DB_NAME: &str = "test";
const NUM_COLLS: i64 = 10;
const NUM_DOCS: i64 = 5;

macro_rules! coll_name {
    ($i:expr) => {
        format!("thing{}", $i)
    };
}

fn populate(main_client: &Client) -> mongodb::error::Result<()> {
    let handles = (1..=NUM_COLLS).map(|i| {
        let client = main_client.clone();

        thread::spawn(move || {
            let coll = client.database(DB_NAME).collection(&coll_name!(i));

            let docs: Vec<_> = (1..=NUM_DOCS).map(|j| doc! { "x": j as i32 }).collect();
            coll.insert_many(
                docs,
                Some(InsertManyOptions::builder().ordered(true).build()),
            )
            .unwrap();
        })
    });

    for handle in handles {
        handle.join().unwrap();
    }

    Ok(())
}

fn count(client: &Client) -> mongodb::error::Result<i64> {
    let mut total = 0;

    for i in 1..=NUM_COLLS {
        let coll = client.database(DB_NAME).collection_with_options(
            &coll_name!(i),
            CollectionOptions::builder()
                .read_preference(ReadPreference::SecondaryPreferred {
                    tag_sets: None,
                    max_staleness: None,
                })
                .build(),
        );
        total += coll.estimated_document_count(None)?;
    }

    Ok(total)
}

fn run() -> mongodb::error::Result<()> {
    let client =
        Client::with_uri(option_env!("MONGODB_URI").unwrap_or("mongodb://localhost:27017"))?;
    client.database(DB_NAME).drop()?;
    populate(&client)?;

    println!("{}", count(&client)?);

    Ok(())
}

fn main() {
    if let Err(e) = run() {
        eprintln!("something went wrong!");
        eprintln!("{}", e);
    }
}
