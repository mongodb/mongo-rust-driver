use std::time::Duration;

use bson::{doc, Document};
use serde::Serialize;
use typed_builder::TypedBuilder;

use crate::{
    bson_util,
    coll::options::{
        FindOneAndDeleteOptions,
        FindOneAndReplaceOptions,
        FindOneAndUpdateOptions,
        ReturnDocument,
        UpdateModifications,
    },
    collation::Collation,
    concern::WriteConcern,
};

#[serde_with::skip_serializing_none]
#[derive(Debug, Default, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct FindAndModifyOptions {
    #[builder(default)]
    pub(crate) sort: Option<Document>,

    #[builder(default)]
    pub(crate) remove: Option<bool>,

    #[builder(default)]
    pub(crate) update: Option<UpdateModifications>,

    #[builder(default)]
    pub(crate) new: Option<bool>,

    #[builder(default)]
    pub(crate) upsert: Option<bool>,

    #[builder(default)]
    pub(crate) bypass_document_validation: Option<bool>,

    #[builder(default)]
    pub(crate) write_concern: Option<WriteConcern>,

    #[builder(default)]
    pub(crate) array_filters: Option<Vec<Document>>,

    #[builder(default)]
    #[serde(
        serialize_with = "bson_util::serialize_duration_as_i64_millis",
        rename = "maxTimeMS"
    )]
    pub(crate) max_time: Option<Duration>,

    #[builder(default)]
    #[serde(rename = "fields")]
    pub(crate) projection: Option<Document>,

    #[builder(default)]
    pub(crate) collation: Option<Collation>,
}

impl FindAndModifyOptions {
    pub(super) fn from_find_one_and_delete_options(
        opts: FindOneAndDeleteOptions,
    ) -> FindAndModifyOptions {
        FindAndModifyOptions::builder()
            .collation(opts.collation)
            .max_time(opts.max_time)
            .projection(opts.projection)
            .sort(opts.sort)
            .remove(true)
            .write_concern(opts.write_concern)
            .build()
    }

    pub(super) fn from_find_one_and_replace_options(
        replacement: Document,
        opts: FindOneAndReplaceOptions,
    ) -> FindAndModifyOptions {
        let replacement = UpdateModifications::Document(replacement);
        FindAndModifyOptions::builder()
            .collation(opts.collation)
            .bypass_document_validation(opts.bypass_document_validation)
            .max_time(opts.max_time)
            .projection(opts.projection)
            .new(return_document_to_bool(opts.return_document))
            .sort(opts.sort)
            .upsert(opts.upsert)
            .update(replacement)
            .write_concern(opts.write_concern)
            .build()
    }

    pub(super) fn from_find_one_and_update_options(
        update: UpdateModifications,
        opts: FindOneAndUpdateOptions,
    ) -> FindAndModifyOptions {
        FindAndModifyOptions::builder()
            .collation(opts.collation)
            .array_filters(opts.array_filters)
            .bypass_document_validation(opts.bypass_document_validation)
            .max_time(opts.max_time)
            .projection(opts.projection)
            .new(return_document_to_bool(opts.return_document))
            .sort(opts.sort)
            .upsert(opts.upsert)
            .update(update)
            .write_concern(opts.write_concern)
            .build()
    }
}

fn return_document_to_bool(return_document: Option<ReturnDocument>) -> Option<bool> {
    if let Some(return_document) = return_document {
        return match return_document {
            ReturnDocument::After => Some(true),
            ReturnDocument::Before => Some(false),
        };
    }
    None
}
