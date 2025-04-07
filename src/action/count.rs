use bson::{Bson, Document};
use std::time::Duration;

use crate::{
    coll::options::{CountOptions, EstimatedDocumentCountOptions, Hint},
    collation::Collation,
    concern::ReadConcern,
    error::Result,
    selection_criteria::SelectionCriteria,
    ClientSession,
    Collection,
};

use super::{action_impl, deeplink, export_doc, option_setters, options_doc, CollRef};

impl<T> Collection<T>
where
    T: Send + Sync,
{
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
    /// `await` will return d[`Result<u64>`].
    #[deeplink]
    #[options_doc(estimated_doc_count)]
    pub fn estimated_document_count(&self) -> EstimatedDocumentCount {
        EstimatedDocumentCount {
            cr: CollRef::new(self),
            options: None,
        }
    }

    /// Gets the number of documents. This method returns an accurate count.
    ///
    /// Certain query operators cannot be used in the filter provided to this method, including
    /// `$where` and `$near`. See the documentation for the `$match` aggregation pipeline stage for
    /// full details on these restrictions:
    /// https://www.mongodb.com/docs/manual/reference/operator/aggregation/match/#restrictions
    ///
    /// `await` will return d[`Result<u64>`].
    #[deeplink]
    #[options_doc(count_docs)]
    pub fn count_documents(&self, filter: Document) -> CountDocuments {
        CountDocuments {
            cr: CollRef::new(self),
            filter,
            options: None,
            session: None,
        }
    }
}

#[cfg(feature = "sync")]
impl<T> crate::sync::Collection<T>
where
    T: Send + Sync,
{
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
    /// [`run`](EstimatedDocumentCount::run) will return d[`Result<u64>`].
    #[deeplink]
    #[options_doc(estimated_doc_count, sync)]
    pub fn estimated_document_count(&self) -> EstimatedDocumentCount {
        self.async_collection.estimated_document_count()
    }

    /// Gets the number of documents.
    ///
    /// Note that this method returns an accurate count.
    ///
    /// [`run`](CountDocuments::run) will return d[`Result<u64>`].
    #[deeplink]
    #[options_doc(count_docs, sync)]
    pub fn count_documents(&self, filter: Document) -> CountDocuments {
        self.async_collection.count_documents(filter)
    }
}

/// Gather an estimated document count.  Construct with [`Collection::estimated_document_count`].
#[must_use]
pub struct EstimatedDocumentCount<'a> {
    cr: CollRef<'a>,
    options: Option<EstimatedDocumentCountOptions>,
}

#[option_setters(crate::coll::options::EstimatedDocumentCountOptions)]
#[export_doc(estimated_doc_count)]
impl EstimatedDocumentCount<'_> {}

#[action_impl]
impl<'a> Action for EstimatedDocumentCount<'a> {
    type Future = EstimatedDocumentCountFuture;

    async fn execute(mut self) -> Result<u64> {
        resolve_options!(self.cr, self.options, [read_concern, selection_criteria]);
        let op = crate::operation::count::Count::new(self.cr.namespace(), self.options);
        self.cr.client().execute_operation(op, None).await
    }
}

/// Get an accurate count of documents.  Construct with [`Collection::count_documents`].
#[must_use]
pub struct CountDocuments<'a> {
    cr: CollRef<'a>,
    filter: Document,
    options: Option<CountOptions>,
    session: Option<&'a mut ClientSession>,
}

#[option_setters(crate::coll::options::CountOptions)]
#[export_doc(count_docs)]
impl<'a> CountDocuments<'a> {
    /// Use the provided session when running the operation.
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

#[action_impl]
impl<'a> Action for CountDocuments<'a> {
    type Future = CountDocumentsFuture;

    async fn execute(mut self) -> Result<u64> {
        resolve_read_concern_with_session!(self.cr, self.options, self.session.as_ref())?;
        resolve_selection_criteria_with_session!(self.cr, self.options, self.session.as_ref())?;

        let op = crate::operation::count_documents::CountDocuments::new(
            self.cr.namespace(),
            self.filter,
            self.options,
        )?;
        self.cr.client().execute_operation(op, self.session).await
    }
}
