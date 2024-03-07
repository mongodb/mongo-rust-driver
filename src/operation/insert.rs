#[cfg(test)]
mod test;

use std::{collections::HashMap, convert::TryInto};

use bson::{oid::ObjectId, Bson, RawArrayBuf, RawDocument, RawDocumentBuf};

use crate::{
    bson::rawdoc,
    bson_util,
    checked::Checked,
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::{BulkWriteFailure, Error, ErrorKind, Result},
    operation::{OperationWithDefaults, Retryability, WriteResponseBody},
    options::{InsertManyOptions, WriteConcern},
    results::InsertManyResult,
    Namespace,
};

use super::{COMMAND_OVERHEAD_SIZE, MAX_ENCRYPTED_WRITE_SIZE};

#[derive(Debug)]
pub(crate) struct Insert<'a> {
    ns: Namespace,
    documents: Vec<&'a RawDocument>,
    inserted_ids: Vec<Bson>,
    options: InsertManyOptions,
    encrypted: bool,
}

impl<'a> Insert<'a> {
    pub(crate) fn new(
        ns: Namespace,
        documents: Vec<&'a RawDocument>,
        options: Option<InsertManyOptions>,
        encrypted: bool,
    ) -> Self {
        let mut options = options.unwrap_or_default();
        if options.ordered.is_none() {
            options.ordered = Some(true);
        }

        Self {
            ns,
            options,
            documents,
            inserted_ids: vec![],
            encrypted,
        }
    }
}

impl<'a> OperationWithDefaults for Insert<'a> {
    type O = InsertManyResult;
    type Command = RawDocumentBuf;

    const NAME: &'static str = "insert";

    fn build(&mut self, description: &StreamDescription) -> Result<Command<Self::Command>> {
        let mut docs = Vec::new();
        let mut size = 0;

        let max_doc_size = Checked::<usize>::try_from(description.max_bson_object_size)?;
        let max_doc_sequence_size =
            Checked::<usize>::try_from(description.max_message_size_bytes)? - COMMAND_OVERHEAD_SIZE;

        for (i, &d) in self
            .documents
            .iter()
            .take(Checked::new(description.max_write_batch_size).try_into()?)
            .enumerate()
        {
            let mut doc = d.to_owned();
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
                    let new_length = Checked::new(bytes.len()).try_into::<i32>()?.to_le_bytes();
                    bytes[0..4].copy_from_slice(&new_length);
                    doc = RawDocumentBuf::from_bytes(bytes)?;

                    Bson::ObjectId(oid)
                }
            };

            let doc_size = doc.as_bytes().len();
            if doc_size > max_doc_size.get()? {
                return Err(ErrorKind::InvalidArgument {
                    message: format!(
                        "insert document must be within {} bytes, but document provided is {} \
                         bytes",
                        max_doc_size, doc_size
                    ),
                }
                .into());
            }

            // From the spec: Drivers MUST not reduce the size limits for a single write before
            // automatic encryption. I.e. if a single document has size larger than 2MiB (but less
            // than `maxBsonObjectSize`) proceed with automatic encryption.
            if self.encrypted && i != 0 {
                let doc_entry_size = bson_util::array_entry_size_bytes(i, doc.as_bytes().len())?;
                if (Checked::new(size) + doc_entry_size).get()? >= MAX_ENCRYPTED_WRITE_SIZE {
                    break;
                }
            } else if (Checked::new(size) + doc_size).get()? > max_doc_sequence_size.get()? {
                break;
            }

            self.inserted_ids.push(id);
            docs.push(doc);
            size += doc_size;
        }

        let mut body = rawdoc! {
            Self::NAME: self.ns.coll.clone(),
        };

        let options_doc = bson::to_raw_document_buf(&self.options)?;
        bson_util::extend_raw_document_buf(&mut body, options_doc)?;

        if self.encrypted {
            // Auto-encryption does not support document sequences
            let mut raw_array = RawArrayBuf::new();
            for doc in docs {
                raw_array.push(doc);
            }
            body.append("documents", raw_array);
            Ok(Command::new(Self::NAME, &self.ns.db, body))
        } else {
            let mut command = Command::new(Self::NAME, &self.ns.db, body);
            command.add_document_sequence("documents", docs);
            Ok(command)
        }
    }

    fn handle_response(
        &self,
        raw_response: RawCommandResponse,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        let response: WriteResponseBody = raw_response.body_utf8_lossy()?;
        let response_n = Checked::<usize>::try_from(response.n)?;

        let mut map = HashMap::new();
        if self.options.ordered == Some(true) {
            // in ordered inserts, only the first n were attempted.
            for (i, id) in self.inserted_ids.iter().enumerate().take(response_n.get()?) {
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
