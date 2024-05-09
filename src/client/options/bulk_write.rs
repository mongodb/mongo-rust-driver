#![allow(missing_docs)]

use serde::{ser::SerializeMap, Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::{
    bson::{rawdoc, Array, Bson, Document, RawDocumentBuf},
    bson_util::{get_or_prepend_id_field, replacement_document_check, update_document_check},
    error::Result,
    options::{UpdateModifications, WriteConcern},
    Namespace,
};

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BulkWriteOptions {
    pub ordered: Option<bool>,
    pub bypass_document_validation: Option<bool>,
    pub comment: Option<Bson>,
    #[serde(rename = "let")]
    pub let_vars: Option<Document>,
    pub verbose_results: Option<bool>,
    pub write_concern: Option<WriteConcern>,
}

impl Serialize for BulkWriteOptions {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let BulkWriteOptions {
            ordered,
            bypass_document_validation,
            comment,
            let_vars,
            verbose_results,
            write_concern,
        } = self;

        let mut map_serializer = serializer.serialize_map(None)?;

        let ordered = ordered.unwrap_or(true);
        map_serializer.serialize_entry("ordered", &ordered)?;

        if let Some(bypass_document_validation) = bypass_document_validation {
            map_serializer
                .serialize_entry("bypassDocumentValidation", bypass_document_validation)?;
        }

        if let Some(ref comment) = comment {
            map_serializer.serialize_entry("comment", comment)?;
        }

        if let Some(ref let_vars) = let_vars {
            map_serializer.serialize_entry("let", let_vars)?;
        }

        let errors_only = verbose_results.map(|b| !b).unwrap_or(true);
        map_serializer.serialize_entry("errorsOnly", &errors_only)?;

        if let Some(ref write_concern) = write_concern {
            map_serializer.serialize_entry("writeConcern", write_concern)?;
        }

        map_serializer.end()
    }
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
