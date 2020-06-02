use std::time::Duration;

use serde::{Serialize, Serializer};
use typed_builder::TypedBuilder;

use crate::{
    bson::{doc, Document},
    bson_util,
    coll::options::{
        FindOneAndDeleteOptions,
        FindOneAndReplaceOptions,
        FindOneAndUpdateOptions,
        Hint,
        ReturnDocument,
        UpdateModifications,
    },
    collation::Collation,
    concern::WriteConcern,
};

#[derive(Debug, Serialize)]
pub(super) enum Modification {
    #[serde(rename = "remove", serialize_with = "self::serialize_true")]
    Delete,
    #[serde(rename = "update")]
    Update(UpdateModifications),
}

fn serialize_true<S: Serializer>(s: S) -> std::result::Result<S::Ok, S::Error> {
    s.serialize_bool(true)
}

#[serde_with::skip_serializing_none]
#[derive(Debug, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct FindAndModifyOptions {
    #[serde(flatten)]
    pub(crate) modification: Modification,

    #[builder(default)]
    pub(crate) sort: Option<Document>,

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
        serialize_with = "bson_util::serialize_duration_as_int_millis",
        rename = "maxTimeMS"
    )]
    pub(crate) max_time: Option<Duration>,

    #[builder(default)]
    #[serde(rename = "fields")]
    pub(crate) projection: Option<Document>,

    #[builder(default)]
    pub(crate) collation: Option<Collation>,

    #[builder(default)]
    pub(crate) hint: Option<Hint>,
}

impl FindAndModifyOptions {
    pub(super) fn from_find_one_and_delete_options(
        opts: FindOneAndDeleteOptions,
    ) -> FindAndModifyOptions {
        FindAndModifyOptions::builder()
            .modification(Modification::Delete)
            .collation(opts.collation)
            .max_time(opts.max_time)
            .projection(opts.projection)
            .sort(opts.sort)
            .write_concern(opts.write_concern)
            .hint(opts.hint)
            .build()
    }

    pub(super) fn from_find_one_and_replace_options(
        replacement: Document,
        opts: FindOneAndReplaceOptions,
    ) -> FindAndModifyOptions {
        let replacement = UpdateModifications::Document(replacement);
        FindAndModifyOptions::builder()
            .modification(Modification::Update(replacement))
            .collation(opts.collation)
            .bypass_document_validation(opts.bypass_document_validation)
            .max_time(opts.max_time)
            .projection(opts.projection)
            .new(return_document_to_bool(opts.return_document))
            .sort(opts.sort)
            .upsert(opts.upsert)
            .write_concern(opts.write_concern)
            .build()
    }

    pub(super) fn from_find_one_and_update_options(
        update: UpdateModifications,
        opts: FindOneAndUpdateOptions,
    ) -> FindAndModifyOptions {
        FindAndModifyOptions::builder()
            .modification(Modification::Update(update))
            .collation(opts.collation)
            .array_filters(opts.array_filters)
            .bypass_document_validation(opts.bypass_document_validation)
            .max_time(opts.max_time)
            .projection(opts.projection)
            .new(return_document_to_bool(opts.return_document))
            .sort(opts.sort)
            .upsert(opts.upsert)
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
