use std::time::Duration;

use serde::Serialize;
use typed_builder::TypedBuilder;

use crate::{
    bson::{doc, Bson, Document},
    coll::options::{
        FindOneAndDeleteOptions,
        FindOneAndReplaceOptions,
        FindOneAndUpdateOptions,
        Hint,
        ReturnDocument,
    },
    collation::Collation,
    concern::WriteConcern,
    operation::UpdateOrReplace,
    serde_util,
};

#[derive(Clone, Debug)]
pub(crate) enum Modification {
    Delete,
    Update(UpdateOrReplace),
}

#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, TypedBuilder, Serialize, Default)]
#[builder(field_defaults(setter(into)))]
#[serde(rename_all = "camelCase")]
pub(crate) struct FindAndModifyOptions {
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

    #[serde(
        serialize_with = "serde_util::serialize_duration_option_as_int_millis",
        rename = "maxTimeMS"
    )]
    #[builder(default)]
    pub(crate) max_time: Option<Duration>,

    #[serde(rename = "fields")]
    #[builder(default)]
    pub(crate) projection: Option<Document>,

    #[builder(default)]
    pub(crate) collation: Option<Collation>,

    #[builder(default)]
    pub(crate) hint: Option<Hint>,

    #[builder(default)]
    #[serde(rename = "let")]
    pub(crate) let_vars: Option<Document>,

    #[builder(default)]
    pub(crate) comment: Option<Bson>,
}

impl From<FindOneAndDeleteOptions> for FindAndModifyOptions {
    fn from(options: FindOneAndDeleteOptions) -> Self {
        Self {
            sort: options.sort,
            new: None,
            upsert: None,
            bypass_document_validation: None,
            write_concern: options.write_concern,
            array_filters: None,
            max_time: options.max_time,
            projection: options.projection,
            collation: options.collation,
            hint: options.hint,
            let_vars: options.let_vars,
            comment: options.comment,
        }
    }
}

impl From<FindOneAndUpdateOptions> for FindAndModifyOptions {
    fn from(options: FindOneAndUpdateOptions) -> Self {
        Self {
            sort: options.sort,
            new: return_document_to_bool(options.return_document),
            upsert: options.upsert,
            bypass_document_validation: options.bypass_document_validation,
            write_concern: options.write_concern,
            array_filters: options.array_filters,
            max_time: options.max_time,
            projection: options.projection,
            collation: options.collation,
            hint: options.hint,
            let_vars: options.let_vars,
            comment: options.comment,
        }
    }
}

impl From<FindOneAndReplaceOptions> for FindAndModifyOptions {
    fn from(options: FindOneAndReplaceOptions) -> Self {
        Self {
            sort: options.sort,
            new: return_document_to_bool(options.return_document),
            upsert: options.upsert,
            bypass_document_validation: options.bypass_document_validation,
            write_concern: options.write_concern,
            array_filters: None,
            max_time: options.max_time,
            projection: options.projection,
            collation: options.collation,
            hint: options.hint,
            let_vars: options.let_vars,
            comment: options.comment,
        }
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
