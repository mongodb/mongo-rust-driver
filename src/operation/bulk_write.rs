#![allow(unused_variables, dead_code)]

use std::collections::HashMap;

use serde::Deserialize;

use crate::{
    bson::{rawdoc, Bson, RawArrayBuf, RawDocumentBuf},
    bson_util,
    client::bulk_write::{models::WriteModel, BulkWriteOptions},
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::{Error, Result},
    operation::OperationWithDefaults,
    Client,
    Cursor,
    Namespace,
};

use super::{CursorInfo, WriteResponseBody};

pub(crate) struct BulkWrite<'a> {
    pub(crate) models: &'a [WriteModel],
    pub(crate) options: BulkWriteOptions,
    pub(crate) client: Client,
}
/// A helper struct for tracking namespace information.
struct NamespaceInfo<'a> {
    namespaces: RawArrayBuf,
    // Cache the namespaces and their indexes to avoid traversing the namespaces array each time a
    // namespace is looked up or added.
    cache: HashMap<&'a Namespace, usize>,
}

impl<'a> NamespaceInfo<'a> {
    fn new() -> Self {
        Self {
            namespaces: RawArrayBuf::new(),
            cache: HashMap::new(),
        }
    }

    /// Gets the index for the given namespace in the nsInfo list, adding it to the list if it is
    /// not already present.
    fn get_index(&mut self, namespace: &'a Namespace) -> usize {
        match self.cache.get(namespace) {
            Some(index) => *index,
            None => {
                self.namespaces
                    .push(rawdoc! { "ns": namespace.to_string() });
                let next_index = self.cache.len();
                self.cache.insert(namespace, next_index);
                next_index
            }
        }
    }
}

impl<'a> OperationWithDefaults for BulkWrite<'a> {
    type O = (Cursor<BulkWriteOperationResponse>, BulkWriteSummaryInfo);

    type Command = RawDocumentBuf;

    const NAME: &'static str = "bulkWrite";

    fn build(&mut self, description: &StreamDescription) -> Result<Command<Self::Command>> {
        let mut namespace_info = NamespaceInfo::new();
        let mut ops = RawArrayBuf::new();
        for model in self.models {
            let namespace_index = namespace_info.get_index(model.namespace());

            let mut model_doc = rawdoc! { model.operation_name(): namespace_index as i32 };
            let model_fields = model.to_raw_doc()?;
            bson_util::extend_raw_document_buf(&mut model_doc, model_fields)?;

            ops.push(model_doc);
        }

        let command = rawdoc! {
            Self::NAME: 1,
            "ops": ops,
            "nsInfo": namespace_info.namespaces,
        };

        Ok(Command::new(Self::NAME, "admin", command))
    }

    fn handle_response(
        &self,
        response: RawCommandResponse,
        description: &StreamDescription,
    ) -> Result<Self::O> {
        let response: WriteResponseBody<BulkWriteResponse> = response.body()?;

        let specification = CursorSpecification::new(
            response.body.cursor,
            description.server_address.clone(),
            None,
            None,
            None,
        );
        let cursor = Cursor::new(self.client.clone(), specification, None, None);

        Ok((cursor, response.body.summary))
    }

    fn handle_error(&self, error: Error) -> Result<Self::O> {
        Err(error)
    }
}

#[derive(Deserialize)]
struct BulkWriteResponse {
    cursor: CursorInfo,
    #[serde(flatten)]
    summary: BulkWriteSummaryInfo,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BulkWriteSummaryInfo {
    pub(crate) n_inserted: i64,
    pub(crate) n_matched: i64,
    pub(crate) n_modified: i64,
    pub(crate) n_upserted: i64,
    pub(crate) n_deleted: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BulkWriteOperationResponse {
    #[serde(rename = "idx")]
    pub(crate) index: usize,
    pub(crate) n: u64,
    pub(crate) n_modified: Option<u64>,
    pub(crate) upserted: Option<Bson>,
}

impl BulkWriteOperationResponse {
    pub(crate) fn is_update_result(&self) -> bool {
        self.n_modified.is_some() || self.upserted.is_some()
    }
}
