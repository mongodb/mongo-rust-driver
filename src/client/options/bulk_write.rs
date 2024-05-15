#![allow(missing_docs)]

use std::borrow::Borrow;

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use typed_builder::TypedBuilder;

use crate::{
    bson::{rawdoc, Array, Bson, Document, RawDocumentBuf},
    bson_util::{get_or_prepend_id_field, replacement_document_check, update_document_check},
    error::Result,
    options::{UpdateModifications, WriteConcern},
    serde_util::serialize_bool_or_true,
    Collection,
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
    InsertOne(InsertOneModel),
    UpdateOne(UpdateOneModel),
    UpdateMany(UpdateManyModel),
    ReplaceOne(ReplaceOneModel),
    DeleteOne(DeleteOneModel),
    DeleteMany(DeleteManyModel),
}

#[skip_serializing_none]
#[derive(Clone, Debug, Serialize, TypedBuilder)]
#[cfg_attr(test, derive(Deserialize))]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct InsertOneModel {
    #[serde(skip_serializing)]
    #[builder(!default)]
    pub namespace: Namespace,

    #[builder(!default)]
    pub document: Document,
}

impl From<InsertOneModel> for WriteModel {
    fn from(model: InsertOneModel) -> Self {
        Self::InsertOne(model)
    }
}

#[skip_serializing_none]
#[derive(Clone, Debug, Serialize, TypedBuilder)]
#[cfg_attr(test, derive(Deserialize))]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct UpdateOneModel {
    #[serde(skip_serializing)]
    #[builder(!default)]
    pub namespace: Namespace,

    #[builder(!default)]
    pub filter: Document,

    #[serde(rename(serialize = "updateMods"))]
    #[builder(!default)]
    pub update: UpdateModifications,

    pub array_filters: Option<Array>,

    pub collation: Option<Document>,

    pub hint: Option<Bson>,

    pub upsert: Option<bool>,
}

impl From<UpdateOneModel> for WriteModel {
    fn from(model: UpdateOneModel) -> Self {
        Self::UpdateOne(model)
    }
}

#[skip_serializing_none]
#[derive(Clone, Debug, Serialize, TypedBuilder)]
#[cfg_attr(test, derive(Deserialize))]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct UpdateManyModel {
    #[serde(skip_serializing)]
    #[builder(!default)]
    pub namespace: Namespace,

    #[builder(!default)]
    pub filter: Document,

    #[serde(rename(serialize = "updateMods"))]
    #[builder(!default)]
    pub update: UpdateModifications,

    pub array_filters: Option<Array>,

    pub collation: Option<Document>,

    pub hint: Option<Bson>,

    pub upsert: Option<bool>,
}

impl From<UpdateManyModel> for WriteModel {
    fn from(model: UpdateManyModel) -> Self {
        Self::UpdateMany(model)
    }
}

#[skip_serializing_none]
#[derive(Clone, Debug, Serialize, TypedBuilder)]
#[cfg_attr(test, derive(Deserialize))]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct ReplaceOneModel {
    #[serde(skip_serializing)]
    #[builder(!default)]
    pub namespace: Namespace,

    #[builder(!default)]
    pub filter: Document,

    #[serde(rename(serialize = "updateMods"))]
    #[builder(!default)]
    pub replacement: Document,

    pub collation: Option<Document>,

    pub hint: Option<Bson>,

    pub upsert: Option<bool>,
}

impl From<ReplaceOneModel> for WriteModel {
    fn from(model: ReplaceOneModel) -> Self {
        Self::ReplaceOne(model)
    }
}

#[skip_serializing_none]
#[derive(Clone, Debug, Serialize, TypedBuilder)]
#[cfg_attr(test, derive(Deserialize))]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct DeleteOneModel {
    #[serde(skip_serializing)]
    #[builder(!default)]
    pub namespace: Namespace,

    #[builder(!default)]
    pub filter: Document,

    pub collation: Option<Document>,

    pub hint: Option<Bson>,
}

impl From<DeleteOneModel> for WriteModel {
    fn from(model: DeleteOneModel) -> Self {
        Self::DeleteOne(model)
    }
}

#[skip_serializing_none]
#[derive(Clone, Debug, Serialize, TypedBuilder)]
#[cfg_attr(test, derive(Deserialize))]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct DeleteManyModel {
    #[serde(skip_serializing)]
    #[builder(!default)]
    pub namespace: Namespace,

    pub filter: Document,

    pub collation: Option<Document>,

    pub hint: Option<Bson>,
}

impl From<DeleteManyModel> for WriteModel {
    fn from(model: DeleteManyModel) -> Self {
        Self::DeleteMany(model)
    }
}

impl<T> Collection<T>
where
    T: Send + Sync + Serialize,
{
    pub fn insert_one_model(&self, document: impl Borrow<T>) -> Result<InsertOneModel> {
        let document = bson::to_document(document.borrow())?;
        Ok(InsertOneModel::builder()
            .namespace(self.namespace())
            .document(document)
            .build())
    }

    pub fn replace_one_model(
        &self,
        filter: Document,
        replacement: impl Borrow<T>,
    ) -> Result<ReplaceOneModel> {
        let replacement = bson::to_document(replacement.borrow())?;
        Ok(ReplaceOneModel::builder()
            .namespace(self.namespace())
            .filter(filter)
            .replacement(replacement)
            .build())
    }
}

pub(crate) enum OperationType {
    Insert,
    Update,
    Delete,
}

impl WriteModel {
    pub(crate) fn namespace(&self) -> &Namespace {
        match self {
            Self::InsertOne(model) => &model.namespace,
            Self::UpdateOne(model) => &model.namespace,
            Self::UpdateMany(model) => &model.namespace,
            Self::ReplaceOne(model) => &model.namespace,
            Self::DeleteOne(model) => &model.namespace,
            Self::DeleteMany(model) => &model.namespace,
        }
    }

    pub(crate) fn operation_type(&self) -> OperationType {
        match self {
            Self::InsertOne(_) => OperationType::Insert,
            Self::UpdateOne(_) | Self::UpdateMany(_) | Self::ReplaceOne(_) => OperationType::Update,
            Self::DeleteOne(_) | Self::DeleteMany(_) => OperationType::Delete,
        }
    }

    /// Whether this operation should apply to all documents that match the filter. Returns None if
    /// the operation does not use a filter.
    pub(crate) fn multi(&self) -> Option<bool> {
        match self {
            Self::UpdateMany(_) | Self::DeleteMany(_) => Some(true),
            Self::UpdateOne(_) | Self::ReplaceOne(_) | Self::DeleteOne(_) => Some(false),
            Self::InsertOne(_) => None,
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
        if let Self::UpdateOne(UpdateOneModel { update, .. })
        | Self::UpdateMany(UpdateManyModel { update, .. }) = self
        {
            if let UpdateModifications::Document(update_document) = update {
                update_document_check(update_document)?;
            }
        } else if let Self::ReplaceOne(ReplaceOneModel { replacement, .. }) = self {
            replacement_document_check(replacement)?;
        }

        let (mut model_document, inserted_id) = match self {
            Self::InsertOne(model) => {
                let mut insert_document = RawDocumentBuf::from_document(&model.document)?;
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
