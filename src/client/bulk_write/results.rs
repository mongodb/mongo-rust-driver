#![allow(missing_docs)]

use std::collections::HashMap;

use serde::Serialize;

use crate::{
    bson::Bson,
    error::{Error, ErrorKind, Result},
    operation::{BulkWriteOperationResponse, BulkWriteSummaryInfo},
    results::{DeleteResult, InsertOneResult, UpdateResult},
    serde_util::serialize_indexed_map,
};

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct VerboseBulkWriteResult {
    pub inserted_count: i64,
    pub upserted_count: i64,
    pub matched_count: i64,
    pub modified_count: i64,
    pub deleted_count: i64,
    #[serde(serialize_with = "serialize_indexed_map")]
    pub insert_results: HashMap<usize, InsertOneResult>,
    #[serde(serialize_with = "serialize_indexed_map")]
    pub update_results: HashMap<usize, UpdateResult>,
    #[serde(serialize_with = "serialize_indexed_map")]
    pub delete_results: HashMap<usize, DeleteResult>,
}

impl VerboseBulkWriteResult {
    pub(crate) fn new(
        summary_info: BulkWriteSummaryInfo,
        inserted_ids: HashMap<usize, Bson>,
    ) -> Self {
        Self {
            inserted_count: summary_info.n_inserted,
            upserted_count: summary_info.n_upserted,
            matched_count: summary_info.n_matched,
            modified_count: summary_info.n_modified,
            deleted_count: summary_info.n_deleted,
            insert_results: inserted_ids
                .into_iter()
                .map(|(index, id)| (index, InsertOneResult { inserted_id: id }))
                .collect(),
            update_results: HashMap::new(),
            delete_results: HashMap::new(),
        }
    }

    pub(crate) fn add_update_result(&mut self, response: BulkWriteOperationResponse) -> Result<()> {
        self.update_results
            .insert(response.index, response.try_into()?);
        Ok(())
    }

    pub(crate) fn add_delete_result(&mut self, response: BulkWriteOperationResponse) {
        let delete_result = DeleteResult {
            deleted_count: response.n,
        };
        self.delete_results.insert(response.index, delete_result);
    }
}

impl TryFrom<BulkWriteOperationResponse> for UpdateResult {
    type Error = Error;

    fn try_from(response: BulkWriteOperationResponse) -> Result<Self> {
        let modified_count = response
            .n_modified
            .ok_or_else(|| ErrorKind::InvalidResponse {
                message: "missing nModified field in update operation response".into(),
            })?;
        Ok(Self {
            matched_count: response.n,
            modified_count,
            upserted_id: response.upserted,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SummaryBulkWriteResult {
    pub inserted_count: i64,
    pub upserted_count: i64,
    pub matched_count: i64,
    pub modified_count: i64,
    pub deleted_count: i64,
    #[serde(serialize_with = "serialize_indexed_map")]
    pub insert_results: HashMap<usize, InsertOneResult>,
}

impl From<VerboseBulkWriteResult> for SummaryBulkWriteResult {
    fn from(verbose_result: VerboseBulkWriteResult) -> Self {
        Self {
            inserted_count: verbose_result.inserted_count,
            upserted_count: verbose_result.upserted_count,
            matched_count: verbose_result.matched_count,
            modified_count: verbose_result.modified_count,
            deleted_count: verbose_result.deleted_count,
            insert_results: verbose_result.insert_results,
        }
    }
}
