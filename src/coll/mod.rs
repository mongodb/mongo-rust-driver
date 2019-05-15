pub mod options;

use bson::Document;

use crate::error::Result;

pub struct Collection {}

impl Collection {
    pub fn find_one(
        &self,
        filter: Option<Document>,
        options: Option<Document>,
    ) -> Result<Option<Document>> {
        unimplemented!()
    }
}
