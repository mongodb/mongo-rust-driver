#[cfg(test)]
mod test;

use std::{collections::HashMap, io::Write};

use bson::{oid::ObjectId, spec::ElementType, Bson};
use serde::Serialize;

use crate::{
    bson::doc,
    bson_util,
    cmap::{Command, StreamDescription},
    error::{BulkWriteFailure, Error, ErrorKind, Result},
    operation::{Operation, Retryability, WriteResponseBody},
    options::{InsertManyOptions, WriteConcern},
    results::InsertManyResult,
    runtime::SyncLittleEndianWrite,
    Namespace,
};

use super::{CommandBody, CommandResponse};

#[derive(Debug)]
pub(crate) struct Insert<'a, T> {
    ns: Namespace,
    documents: Vec<&'a T>,
    inserted_ids: Vec<Bson>,
    options: InsertManyOptions,
}

impl<'a, T> Insert<'a, T> {
    pub(crate) fn new(
        ns: Namespace,
        documents: Vec<&'a T>,
        options: Option<InsertManyOptions>,
    ) -> Self {
        let mut options =
            options.unwrap_or_else(|| InsertManyOptions::builder().ordered(true).build());
        options.ordered = Some(options.ordered.unwrap_or(true));
        Self {
            ns,
            options,
            documents,
            inserted_ids: vec![],
        }
    }

    fn is_ordered(&self) -> bool {
        self.options.ordered.unwrap_or(true)
    }
}

impl<'a, T: Serialize> Operation for Insert<'a, T> {
    type O = InsertManyResult;
    type Command = InsertCommand;
    type Response = CommandResponse<WriteResponseBody>;

    const NAME: &'static str = "insert";

    fn build(&mut self, description: &StreamDescription) -> Result<Command<InsertCommand>> {
        if self.documents.is_empty() {
            return Err(ErrorKind::InvalidArgument {
                message: "must specify at least one document to insert".to_string(),
            }
            .into());
        }

        let mut docs: Vec<Vec<u8>> = Vec::new();
        let mut size = 0;

        for (i, d) in self
            .documents
            .iter()
            .take(description.max_write_batch_size as usize)
            .enumerate()
        {
            let mut doc = bson::to_vec(d)?;
            let id = match bson_util::raw_get(doc.as_slice(), "_id")? {
                Some(b) => b,
                None => {
                    let oid = ObjectId::new();
                    let new_len = doc.len() as i32 + 1 + 4 + 12;
                    doc.splice(0..4, new_len.to_le_bytes().iter().cloned());

                    let mut new_doc = Vec::new();
                    new_doc.write_u8(ElementType::ObjectId as u8)?;
                    new_doc.write_all(b"_id\0")?;
                    new_doc.extend(oid.bytes().iter());
                    doc.splice(4..4, new_doc.into_iter());

                    Bson::ObjectId(oid)
                }
            };

            let doc_size = bson_util::array_entry_size_bytes(i, doc.len());

            if (size + doc_size) <= description.max_bson_object_size as u64 {
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

        let body = InsertCommand {
            insert: self.ns.coll.clone(),
            documents: DocumentArraySpec {
                documents: docs,
                length: size as i32,
            },
            options: self.options.clone(),
        };

        Ok(Command::new("insert".to_string(), self.ns.db.clone(), body))
    }

    fn serialize_command(&mut self, cmd: Command<Self::Command>) -> Result<Vec<u8>> {
        // TODO: RUST-924 Use raw document API here instead.
        let mut serialized = bson::to_vec(&cmd)?;

        serialized.pop(); // drop null byte

        // write element type
        serialized.push(ElementType::Array as u8);

        // write key cstring
        serialized.write_all("documents".as_bytes())?;
        serialized.push(0);

        // write length of array
        let array_length = 4 + cmd.body.documents.length + 1; // add in 4 for length of array, 1 for null byte
        serialized.write_all(&array_length.to_le_bytes())?;

        for (i, doc) in cmd.body.documents.documents.into_iter().enumerate() {
            // write type of document
            serialized.push(ElementType::EmbeddedDocument as u8);

            // write array index
            serialized.write_all(i.to_string().as_bytes())?;
            serialized.push(0);

            // write document
            serialized.extend(doc);
        }

        // write null byte for array
        serialized.push(0);

        // write null byte for containing document
        serialized.push(0);

        // update length of original doc
        let final_length = serialized.len() as i32;
        (&mut serialized[0..4]).write_all(&final_length.to_le_bytes())?;

        Ok(serialized)
    }

    fn handle_response(
        &self,
        response: WriteResponseBody,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
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
        self.options.write_concern.as_ref()
    }

    fn retryability(&self) -> Retryability {
        Retryability::Write
    }
}

/// Data used for creating a BSON array.
struct DocumentArraySpec {
    /// The sum of the lengths of all the documents.
    length: i32,

    /// The serialized documents to be inserted.
    documents: Vec<Vec<u8>>,
}

#[derive(Serialize)]
pub(crate) struct InsertCommand {
    insert: String,

    /// will be serialized in `serialize_command`
    #[serde(skip)]
    documents: DocumentArraySpec,

    #[serde(flatten)]
    options: InsertManyOptions,
}

impl CommandBody for InsertCommand {}
