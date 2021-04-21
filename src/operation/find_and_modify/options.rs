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

    #[builder(default, setter(strip_option))]
    pub(crate) sort: Option<Document>,

    #[builder(default, setter(strip_option))]
    pub(crate) new: Option<bool>,

    #[builder(default, setter(strip_option))]
    pub(crate) upsert: Option<bool>,

    #[builder(default, setter(strip_option))]
    pub(crate) bypass_document_validation: Option<bool>,

    #[builder(default, setter(strip_option))]
    pub(crate) write_concern: Option<WriteConcern>,

    #[builder(default, setter(strip_option))]
    pub(crate) array_filters: Option<Vec<Document>>,

    #[serde(
        serialize_with = "bson_util::serialize_duration_as_int_millis",
        rename = "maxTimeMS"
    )]
    #[builder(default, setter(strip_option))]
    pub(crate) max_time: Option<Duration>,

    #[serde(rename = "fields")]
    #[builder(default, setter(strip_option))]
    pub(crate) projection: Option<Document>,

    #[builder(default, setter(strip_option))]
    pub(crate) collation: Option<Collation>,

    #[builder(default, setter(strip_option))]
    pub(crate) hint: Option<Hint>,
}

impl FindAndModifyOptions {
    pub(super) fn from_find_one_and_delete_options(
        opts: FindOneAndDeleteOptions,
    ) -> FindAndModifyOptions {
        let mut modify_opts = FindAndModifyOptions::builder()
            .modification(Modification::Delete)
            .build();

        modify_opts.collation = opts.collation;
        modify_opts.max_time = opts.max_time;
        modify_opts.projection = opts.projection;
        modify_opts.sort = opts.sort;
        modify_opts.write_concern = opts.write_concern;
        modify_opts.hint = opts.hint;
        modify_opts
    }

    pub(super) fn from_find_one_and_replace_options(
        replacement: Document,
        opts: FindOneAndReplaceOptions,
    ) -> FindAndModifyOptions {
        let replacement = UpdateModifications::Document(replacement);
        let mut modify_opts = FindAndModifyOptions::builder()
            .modification(Modification::Update(replacement))
            .build();

        modify_opts.collation = opts.collation;
        modify_opts.bypass_document_validation = opts.bypass_document_validation;
        modify_opts.max_time = opts.max_time;
        modify_opts.projection = opts.projection;
        modify_opts.new = return_document_to_bool(opts.return_document);
        modify_opts.sort = opts.sort;
        modify_opts.upsert = opts.upsert;
        modify_opts.write_concern = opts.write_concern;
        modify_opts.hint = opts.hint;

        modify_opts
    }

    pub(super) fn from_find_one_and_update_options(
        update: UpdateModifications,
        opts: FindOneAndUpdateOptions,
    ) -> FindAndModifyOptions {
        let mut modify_opts = FindAndModifyOptions::builder()
            .modification(Modification::Update(update))
            .build();

        modify_opts.collation = opts.collation;
        modify_opts.array_filters = opts.array_filters;
        modify_opts.bypass_document_validation = opts.bypass_document_validation;
        modify_opts.max_time = opts.max_time;
        modify_opts.projection = opts.projection;
        modify_opts.new = return_document_to_bool(opts.return_document);
        modify_opts.sort = opts.sort;
        modify_opts.upsert = opts.upsert;
        modify_opts.write_concern = opts.write_concern;
        modify_opts.hint = opts.hint;

        modify_opts
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
