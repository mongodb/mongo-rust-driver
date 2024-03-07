use std::borrow::Borrow;

use bson::RawDocumentBuf;
use serde::Serialize;

use crate::{coll::options::InsertManyOptions, error::Result, serde_util, Collection};

use super::CollRef;

impl<T: Serialize> Collection<T> {
    /// Inserts the data in `docs` into the collection.
    ///
    /// Note that this method accepts both owned and borrowed values, so the input documents
    /// do not need to be cloned in order to be passed in.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// `await` will return `Result<InsertManyResult>`.
    pub fn insert_many_2(&self, docs: impl IntoIterator<Item = impl Borrow<T>>) -> InsertMany {
        let human_readable = self.human_readable_serialization();
        InsertMany {
            coll: CollRef::new(self),
            docs: docs
                .into_iter()
                .map(|v| serde_util::to_raw_document_buf_with_options(v.borrow(), human_readable))
                .collect(),
            options: None,
        }
    }
}

#[must_use]
pub struct InsertMany<'a> {
    coll: CollRef<'a>,
    docs: Result<Vec<RawDocumentBuf>>,
    options: Option<InsertManyOptions>,
}
