use bson::Document;

use crate::{coll::options::EstimatedDocumentCountOptions, error::Result, Collection};

use super::{action_impl, option_setters};

impl<T> Collection<T> {
    /// Estimates the number of documents in the collection using collection metadata.
    ///
    /// Due to an oversight in versions 5.0.0 - 5.0.7 of MongoDB, the `count` server command,
    /// which `estimatedDocumentCount` uses in its implementation, was not included in v1 of the
    /// Stable API. Users of the Stable API with `estimatedDocumentCount` are recommended to
    /// upgrade their cluster to 5.0.8+ or set
    /// [`ServerApi::strict`](crate::options::ServerApi::strict) to false to avoid encountering
    /// errors.
    ///
    /// For more information on the behavior of the `count` server command, see
    /// [Count: Behavior](https://www.mongodb.com/docs/manual/reference/command/count/#behavior).
    ///
    /// `await` will return `Result<u64>`.
    pub fn estimated_document_count(&self) -> EstimatedDocumentCount {
        EstimatedDocumentCount {
            coll: self.as_untyped_ref(),
            options: None,
        }
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl<T> crate::sync::Collection<T> {
    /// Estimates the number of documents in the collection using collection metadata.
    ///
    /// Due to an oversight in versions 5.0.0 - 5.0.7 of MongoDB, the `count` server command,
    /// which `estimatedDocumentCount` uses in its implementation, was not included in v1 of the
    /// Stable API. Users of the Stable API with `estimatedDocumentCount` are recommended to
    /// upgrade their cluster to 5.0.8+ or set
    /// [`ServerApi::strict`](crate::options::ServerApi::strict) to false to avoid encountering
    /// errors.
    ///
    /// For more information on the behavior of the `count` server command, see
    /// [Count: Behavior](https://www.mongodb.com/docs/manual/reference/command/count/#behavior).
    ///
    /// [`run`](EstimatedDocumentCount::run) will return `Result<u64>`.
    pub fn estimated_document_count(&self) -> EstimatedDocumentCount {
        self.async_collection.estimated_document_count()
    }
}

/// Gather an estimated document count.  Create by calling [`Collection::estimated_document_count`].
#[must_use]
pub struct EstimatedDocumentCount<'a> {
    coll: &'a Collection<Document>,
    options: Option<EstimatedDocumentCountOptions>,
}

impl<'a> EstimatedDocumentCount<'a> {
    option_setters!(options: EstimatedDocumentCountOptions;
        max_time: std::time::Duration,
        selection_criteria: crate::selection_criteria::SelectionCriteria,
        read_concern: crate::options::ReadConcern,
        comment: bson::Bson,
    );
}

action_impl! {
    impl<'a> Action for EstimatedDocumentCount<'a> {
        type Future = EstimatedDocumentCountFuture;

        async fn execute(mut self) -> Result<u64> {
            resolve_options!(self.coll, self.options, [read_concern, selection_criteria]);
            let op = crate::operation::count::Count::new(self.coll.namespace(), self.options);
            self.coll.client().execute_operation(op, None).await
        }
    }
}
