#![allow(missing_docs)]

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::{
    bson::{rawdoc, Array, Bson, Document, RawDocumentBuf},
    bson_util::{get_or_prepend_id_field, replacement_document_check, update_document_check},
    error::Result,
    options::{UpdateModifications, WriteConcern},
    serde_util::serialize_bool_or_true,
    Namespace,
};

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BulkWriteOptions {
    #[serialize_always]
    #[serde(serialize_with = "serialize_bool_or_true")]
    pub ordered: Option<bool>,
    pub bypass_document_validation: Option<bool>,
    pub comment: Option<Bson>,
    #[serde(rename = "let")]
    pub let_vars: Option<Document>,
    pub write_concern: Option<WriteConcern>,
}

#[skip_serializing_none]
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum WriteModel {
    #[non_exhaustive]
    InsertOne {
        #[serde(skip)]
        namespace: Namespace,
        document: Document,
    },
    #[non_exhaustive]
    #[serde(rename_all = "camelCase")]
    UpdateOne {
        #[serde(skip)]
        namespace: Namespace,
        filter: Document,
        #[serde(rename = "updateMods")]
        update: UpdateModifications,
        array_filters: Option<Array>,
        collation: Option<Document>,
        hint: Option<Bson>,
        upsert: Option<bool>,
    },
    #[non_exhaustive]
    #[serde(rename_all = "camelCase")]
    UpdateMany {
        #[serde(skip)]
        namespace: Namespace,
        filter: Document,
        #[serde(rename = "updateMods")]
        update: UpdateModifications,
        array_filters: Option<Array>,
        collation: Option<Document>,
        hint: Option<Bson>,
        upsert: Option<bool>,
    },
    #[non_exhaustive]
    #[serde(rename_all = "camelCase")]
    ReplaceOne {
        #[serde(skip)]
        namespace: Namespace,
        filter: Document,
        #[serde(rename = "updateMods")]
        replacement: Document,
        collation: Option<Document>,
        hint: Option<Bson>,
        upsert: Option<bool>,
    },
    #[non_exhaustive]
    DeleteOne {
        #[serde(skip)]
        namespace: Namespace,
        filter: Document,
        collation: Option<Document>,
        hint: Option<Bson>,
    },
    #[non_exhaustive]
    DeleteMany {
        #[serde(skip)]
        namespace: Namespace,
        filter: Document,
        collation: Option<Document>,
        hint: Option<Bson>,
    },
}

pub(crate) enum OperationType {
    Insert,
    Update,
    Delete,
}

impl WriteModel {
    pub(crate) fn namespace(&self) -> &Namespace {
        match self {
            Self::InsertOne { namespace, .. } => namespace,
            Self::UpdateOne { namespace, .. } => namespace,
            Self::UpdateMany { namespace, .. } => namespace,
            Self::ReplaceOne { namespace, .. } => namespace,
            Self::DeleteOne { namespace, .. } => namespace,
            Self::DeleteMany { namespace, .. } => namespace,
        }
    }

    pub(crate) fn operation_type(&self) -> OperationType {
        match self {
            Self::InsertOne { .. } => OperationType::Insert,
            Self::UpdateOne { .. } | Self::UpdateMany { .. } | Self::ReplaceOne { .. } => {
                OperationType::Update
            }
            Self::DeleteOne { .. } | Self::DeleteMany { .. } => OperationType::Delete,
        }
    }

    /// Whether this operation should apply to all documents that match the filter. Returns None if
    /// the operation does not use a filter.
    pub(crate) fn multi(&self) -> Option<bool> {
        match self {
            Self::UpdateMany { .. } | Self::DeleteMany { .. } => Some(true),
            Self::UpdateOne { .. } | Self::ReplaceOne { .. } | Self::DeleteOne { .. } => {
                Some(false)
            }
            Self::InsertOne { .. } => None,
        }
    }

    pub(crate) fn operation_name(&self) -> &'static str {
        match self.operation_type() {
            OperationType::Insert => "insert",
            OperationType::Update => "update",
            OperationType::Delete => "delete",
        }
    }

    /// Returns the operation-specific fields that should be included in this model's entry in the
    /// ops array. Also returns an inserted ID if this is an insert operation.
    pub(crate) fn get_ops_document_contents(&self) -> Result<(RawDocumentBuf, Option<Bson>)> {
        if let Self::UpdateOne { update, .. } | Self::UpdateMany { update, .. } = self {
            if let UpdateModifications::Document(update_document) = update {
                update_document_check(update_document)?;
            }
        } else if let Self::ReplaceOne { replacement, .. } = self {
            replacement_document_check(replacement)?;
        }

        let (mut model_document, inserted_id) = match self {
            Self::InsertOne { document, .. } => {
                let mut insert_document = RawDocumentBuf::from_document(document)?;
                let inserted_id = get_or_prepend_id_field(&mut insert_document)?;
                (rawdoc! { "document": insert_document }, Some(inserted_id))
            }
            _ => {
                let model_document = bson::to_raw_document_buf(&self)?;
                (model_document, None)
            }
        };

        if let Some(multi) = self.multi() {
            model_document.append("multi", multi);
        }

        Ok((model_document, inserted_id))
    }
}
