#[cfg(test)]
mod test;

use std::{collections::HashMap, convert::TryInto};

use bson::{oid::ObjectId, Bson, RawArrayBuf, RawDocumentBuf};
use serde::Serialize;

use crate::{
    bson::doc,
    bson_util,
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::{BulkWriteFailure, Error, ErrorKind, Result},
    operation::{
        remove_empty_write_concern,
        OperationWithDefaults,
        Retryability,
        WriteResponseBody,
    },
    options::{InsertManyOptions, WriteConcern},
    results::InsertManyResult,
    Namespace,
};

use super::CommandBody;

#[derive(Debug)]
pub(crate) struct Insert<'a, T> {
    ns: Namespace,
    documents: Vec<&'a T>,
    inserted_ids: Vec<Bson>,
    options: Option<InsertManyOptions>,
    encrypted: bool,
}

impl<'a, T> Insert<'a, T> {
    pub(crate) fn new(
        ns: Namespace,
        documents: Vec<&'a T>,
        options: Option<InsertManyOptions>,
        encrypted: bool,
    ) -> Self {
        Self {
            ns,
            options,
            documents,
            inserted_ids: vec![],
            encrypted,
        }
    }

    fn is_ordered(&self) -> bool {
        self.options
            .as_ref()
            .and_then(|o| o.ordered)
            .unwrap_or(true)
    }
}

impl<'a, T: Serialize> OperationWithDefaults for Insert<'a, T> {
    type O = InsertManyResult;
    type Command = InsertCommand;

    const NAME: &'static str = "insert";

    fn build(&mut self, description: &StreamDescription) -> Result<Command<InsertCommand>> {
        let mut docs = RawArrayBuf::new();
        let mut size = 0;
        let batch_size_limit = if self.encrypted {
            2_097_152
        } else {
            description.max_bson_object_size as u64
        };

        for (i, d) in self
            .documents
            .iter()
            .take(description.max_write_batch_size as usize)
            .enumerate()
        {
            let mut doc = bson::to_raw_document_buf(d)?;
            let id = match doc.get("_id")? {
                Some(b) => b.try_into()?,
                None => {
                    let mut new_doc = RawDocumentBuf::new();
                    let oid = ObjectId::new();
                    new_doc.append("_id", oid);

                    let mut new_bytes = new_doc.into_bytes();
                    new_bytes.pop(); // remove trailing null byte

                    let mut bytes = doc.into_bytes();
                    let oid_slice = &new_bytes[4..];
                    // insert oid at beginning of document
                    bytes.splice(4..4, oid_slice.iter().cloned());

                    // overwrite old length
                    let new_length = (bytes.len() as i32).to_le_bytes();
                    (&mut bytes[0..4]).copy_from_slice(&new_length);
                    doc = RawDocumentBuf::from_bytes(bytes)?;

                    Bson::ObjectId(oid)
                }
            };

            let doc_size = bson_util::array_entry_size_bytes(i, doc.as_bytes().len());

            if (size + doc_size) <= batch_size_limit {
                if self.inserted_ids.len() <= i {
                    self.inserted_ids.push(id);
                }
                docs.push(doc);
                size += doc_size;
            } else {
                break;
            }
        }

        if docs.is_empty() {
            return Err(ErrorKind::InvalidArgument {
                message: "document exceeds maxBsonObjectSize".to_string(),
            }
            .into());
        }
        let mut options = self.options.clone().unwrap_or_default();
        options.ordered = Some(self.is_ordered());
        remove_empty_write_concern!(Some(&mut options));

        let body = InsertCommand {
            insert: self.ns.coll.clone(),
            documents: docs,
            options,
        };

        Ok(Command::new("insert".to_string(), self.ns.db.clone(), body))
    }

    fn serialize_command(&mut self, cmd: Command<Self::Command>) -> Result<Vec<u8>> {
        let mut doc = bson::to_raw_document_buf(&cmd)?;
        // need to append documents separately because #[serde(flatten)] breaks the custom
        // serialization logic. See https://github.com/serde-rs/serde/issues/2106.
        doc.append("documents", cmd.body.documents);
        Ok(doc.into_bytes())
    }

    fn handle_response(
        &self,
        raw_response: RawCommandResponse,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        let response: WriteResponseBody = raw_response.body_utf8_lossy()?;

        let mut map = HashMap::new();
        if self.is_ordered() {
            // in ordered inserts, only the first n were attempted.
            for (i, id) in self
                .inserted_ids
                .iter()
                .enumerate()
                .take(response.n as usize)
            {
                map.insert(i, id.clone());
            }
        } else {
            // for unordered, add all the attempted ids and then remove the ones that have
            // associated write errors.
            for (i, id) in self.inserted_ids.iter().enumerate() {
                map.insert(i, id.clone());
            }

            if let Some(write_errors) = response.write_errors.as_ref() {
                for err in write_errors {
                    map.remove(&err.index);
                }
            }
        }

        if response.write_errors.is_some() || response.write_concern_error.is_some() {
            return Err(Error::new(
                ErrorKind::BulkWrite(BulkWriteFailure {
                    write_errors: response.write_errors,
                    write_concern_error: response.write_concern_error,
                    inserted_ids: map,
                }),
                response.labels,
            ));
        }

        Ok(InsertManyResult { inserted_ids: map })
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options.as_ref().and_then(|o| o.write_concern.as_ref())
    }

    fn retryability(&self) -> Retryability {
        Retryability::Write
    }
}

#[derive(Serialize)]
pub(crate) struct InsertCommand {
    insert: String,

    /// will be serialized in `serialize_command`
    #[serde(skip)]
    documents: RawArrayBuf,

    #[serde(flatten)]
    options: InsertManyOptions,
}

impl CommandBody for InsertCommand {}
