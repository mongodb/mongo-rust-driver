use std::collections::HashMap;

use crate::{
    bson::{rawdoc, Bson, RawDocument},
    bson_util::{
        array_entry_size_bytes,
        extend_raw_document_buf,
        get_or_prepend_id_field,
        vec_to_raw_array_buf,
    },
    checked::Checked,
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::{Error, ErrorKind, InsertManyError, Result},
    operation::{OperationWithDefaults, Retryability, WriteResponseBody},
    options::{InsertManyOptions, WriteConcern},
    results::InsertManyResult,
    Namespace,
};

use super::{ExecutionContext, MAX_ENCRYPTED_WRITE_SIZE, OP_MSG_OVERHEAD_BYTES};

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

impl OperationWithDefaults for Insert<'_> {
    type O = InsertManyResult;

    const NAME: &'static str = "insert";

    fn build(&mut self, description: &StreamDescription) -> Result<Command> {
        self.inserted_ids.clear();

        let max_doc_size: usize = Checked::new(description.max_bson_object_size).try_into()?;
        let max_message_size: usize =
            Checked::new(description.max_message_size_bytes).try_into()?;
        let max_operations: usize = Checked::new(description.max_write_batch_size).try_into()?;

        let mut command_body = rawdoc! { Self::NAME: self.ns.coll.clone() };
        let options = crate::bson_compat::serialize_to_raw_document_buf(&self.options)?;
        extend_raw_document_buf(&mut command_body, options)?;

        let max_document_sequence_size: usize = (Checked::new(max_message_size)
            - OP_MSG_OVERHEAD_BYTES
            - command_body.as_bytes().len())
        .try_into()?;

        let mut docs = Vec::new();
        let mut current_size = Checked::new(0);
        for (i, document) in self.documents.iter().take(max_operations).enumerate() {
            let mut document = crate::bson_compat::serialize_to_raw_document_buf(document)?;
            let id = get_or_prepend_id_field(&mut document)?;

            let doc_size = document.as_bytes().len();
            if doc_size > max_doc_size {
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
            if self.encrypted {
                let doc_entry_size = array_entry_size_bytes(i, document.as_bytes().len())?;
                current_size += doc_entry_size;
                if i != 0 && current_size.get()? >= MAX_ENCRYPTED_WRITE_SIZE {
                    break;
                }
            } else {
                current_size += doc_size;
                if current_size.get()? > max_document_sequence_size {
                    break;
                }
            }

            self.inserted_ids.push(id);
            docs.push(document);
        }

        let mut body = rawdoc! {
            Self::NAME: self.ns.coll.clone(),
        };

        let options_doc = crate::bson_compat::serialize_to_raw_document_buf(&self.options)?;
        extend_raw_document_buf(&mut body, options_doc)?;

        if self.encrypted {
            // Auto-encryption does not support document sequences
            body.append("documents", vec_to_raw_array_buf(docs));
            Ok(Command::new(Self::NAME, &self.ns.db, body))
        } else {
            let mut command = Command::new(Self::NAME, &self.ns.db, body);
            command.add_document_sequence("documents", docs);
            Ok(command)
        }
    }

    fn handle_response<'b>(
        &'b self,
        response: RawCommandResponse,
        _context: ExecutionContext<'b>,
    ) -> Result<Self::O> {
        let response: WriteResponseBody = response.body()?;
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
                ErrorKind::InsertMany(InsertManyError {
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
