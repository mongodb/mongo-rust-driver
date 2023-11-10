#![allow(unused_variables, dead_code)]

use std::collections::HashMap;

use crate::{
    bson::{rawdoc, RawArrayBuf, RawDocumentBuf},
    bson_util,
    client::bulk_write::{models::WriteModel, BulkWriteOptions},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::{Error, Result},
    operation::OperationWithDefaults,
    Client,
    Namespace,
};

pub(crate) struct BulkWrite {
    pub(crate) models: Vec<WriteModel>,
    pub(crate) options: Option<BulkWriteOptions>,
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

impl OperationWithDefaults for BulkWrite {
    type O = ();

    type Command = RawDocumentBuf;

    const NAME: &'static str = "bulkWrite";

    fn build(&mut self, description: &StreamDescription) -> Result<Command<Self::Command>> {
        let mut namespace_info = NamespaceInfo::new();
        let mut ops = RawArrayBuf::new();
        for model in &self.models {
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
        response.body::<RawDocumentBuf>()?;
        Ok(())
    }

    fn handle_error(&self, error: Error) -> Result<Self::O> {
        Err(error)
    }
}
