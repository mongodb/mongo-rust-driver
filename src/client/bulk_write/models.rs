use std::collections::HashMap;

use serde::Serialize;
use serde_with::skip_serializing_none;

use crate::{
    bson::{oid::ObjectId, Bson, Document, RawDocumentBuf},
    error::Result,
    options::UpdateModifications,
    Namespace,
};

#[skip_serializing_none]
#[derive(Clone, Debug, Serialize)]
#[serde(untagged, rename_all = "camelCase")]
#[non_exhaustive]
pub enum WriteModel {
    #[non_exhaustive]
    InsertOne {
        #[serde(skip)]
        namespace: Namespace,
        document: Document,
    },
    #[non_exhaustive]
    UpdateOne {
        #[serde(skip)]
        namespace: Namespace,
        filter: Document,
        #[serde(rename = "updateMods")]
        update: UpdateModifications,
    },
    #[non_exhaustive]
    UpdateMany {
        #[serde(skip)]
        namespace: Namespace,
        filter: Document,
        #[serde(rename = "updateMods")]
        update: UpdateModifications,
    },
    #[non_exhaustive]
    ReplaceOne {
        #[serde(skip)]
        namespace: Namespace,
        filter: Document,
        #[serde(rename = "updateMods")]
        replacement: Document,
    },
    #[non_exhaustive]
    DeleteOne {
        #[serde(skip)]
        namespace: Namespace,
        filter: Document,
    },
    #[non_exhaustive]
    DeleteMany {
        #[serde(skip)]
        namespace: Namespace,
        filter: Document,
    },
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

    pub(crate) fn operation_name(&self) -> &'static str {
        match self {
            Self::DeleteOne { .. } | Self::DeleteMany { .. } => "delete",
            Self::InsertOne { .. } => "insert",
            Self::ReplaceOne { .. } | Self::UpdateOne { .. } | Self::UpdateMany { .. } => "update",
        }
    }

    pub(crate) fn to_raw_doc(&self) -> Result<RawDocumentBuf> {
        let mut doc = bson::to_raw_document_buf(&self)?;
        match self {
            Self::UpdateOne { .. } | Self::ReplaceOne { .. } | Self::DeleteOne { .. } => {
                doc.append("multi", false);
            }
            Self::UpdateMany { .. } | Self::DeleteMany { .. } => {
                doc.append("multi", true);
            }
            _ => {}
        }
        Ok(doc)
    }
}

pub(crate) fn add_ids_to_insert_one_models(
    models: &mut [WriteModel],
) -> Result<HashMap<usize, Bson>> {
    let mut ids = HashMap::new();
    for (i, model) in models.iter_mut().enumerate() {
        if let WriteModel::InsertOne { document, .. } = model {
            let id = match document.get("_id") {
                Some(id) => id.clone(),
                None => {
                    let id = ObjectId::new();
                    document.insert("_id", id);
                    Bson::ObjectId(id)
                }
            };
            ids.insert(i, id);
        }
    }
    Ok(ids)
}
