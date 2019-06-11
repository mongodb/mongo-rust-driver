#[macro_use]
extern crate bson;

mod bench;
mod error;

use mongodb::{Client, Collection};

fn insert_and_count(coll: &Collection) {
    println!(
        "Num of docs = {}",
        coll.count_documents(None, None).unwrap()
    );

    coll.insert_one(doc! { "x": 1 }, None).unwrap();

    println!(
        "Num of docs = {}",
        coll.count_documents(None, None).unwrap()
    );
}

fn print_docs(coll: &Collection) {
    for doc in coll.find(None, None).unwrap() {
        println!("{}", doc.unwrap());
    }
}

fn main() {
    let client = Client::with_uri_str("mongodb://localhost:27017").unwrap();

    let coll = client.database("new_database").collection("new_collection");
    coll.drop().unwrap();

    insert_and_count(&coll);
    print_docs(&coll);
}
