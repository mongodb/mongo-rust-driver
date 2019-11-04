#![recursion_limit = "128"]

mod client;
mod coll;
mod db;
mod spec;
mod util;

use bson::Document;
use lazy_static::lazy_static;

use crate::util::{TestClient, TestLock};

lazy_static! {
    static ref CLIENT: TestClient = TestClient::new();
    static ref LOCK: TestLock = TestLock::new();
}

pub(crate) fn sort_document(document: &mut Document) {
    let temp = std::mem::replace(document, Default::default());

    let mut elements: Vec<_> = temp.into_iter().collect();
    elements.sort_by(|e1, e2| e1.0.cmp(&e2.0));

    document.extend(elements);
}
