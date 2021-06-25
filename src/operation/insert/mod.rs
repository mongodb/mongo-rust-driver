#[cfg(test)]
mod test;

use std::collections::HashMap;

use bson::{oid::ObjectId, Bson};
use serde::Serialize;

use crate::{
    bson::{doc, Document},
    bson_util,
    cmap::{Command, CommandResponse, StreamDescription},
    error::{BulkWriteFailure, Error, ErrorKind, Result},
    operation::{append_options, Operation, Retryability, WriteResponseBody},
    options::{InsertManyOptions, WriteConcern},
    results::InsertManyResult,
    Namespace,
};

#[derive(Debug)]
pub(crate) struct Insert<T> {
    ns: Namespace,
    documents: Vec<T>,
    inserted_ids: Vec<Bson>,
    options: Option<InsertManyOptions>,
}

impl<T> Insert<T> {
    pub(crate) fn new(
        ns: Namespace,
        documents: Vec<T>,
        options: Option<InsertManyOptions>,
    ) -> Self {
        Self {
            ns,
            options,
            documents,
            inserted_ids: vec![],
        }
    }

    fn is_ordered(&self) -> bool {
        self.options
            .as_ref()
            .and_then(|options| options.ordered)
            .unwrap_or(true)
    }
}

impl<T: Serialize> Operation for Insert<T> {
    type O = InsertManyResult;
    const NAME: &'static str = "insert";

    fn build(&mut self, description: &StreamDescription) -> Result<Command> {
        let mut docs: Vec<Document> = vec![];
        let mut size = 0;

        for (i, d) in self
            .documents
            .iter()
            .take(description.max_write_batch_size as usize)
            .enumerate()
        {
            let mut doc = bson::to_document(d)?;
            let id = doc
                .entry("_id".to_string())
                .or_insert_with(|| {
                    self.inserted_ids
                        .get(i)
                        .cloned()
                        .unwrap_or_else(|| Bson::ObjectId(ObjectId::new()))
                })
                .clone();

            let doc_size = bson_util::array_entry_size_bytes(i, &doc);

            if (size + doc_size) <= description.max_bson_object_size as u64 {
                if self.inserted_ids.len() <= i {
                    self.inserted_ids.push(id);
                }
                docs.push(doc);
                size += doc_size
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

        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
            "documents": docs,
        };

        append_options(&mut body, self.options.as_ref())?;

        body.insert("ordered", self.is_ordered());

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(
        &self,
        response: CommandResponse,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        let body: WriteResponseBody = response.body()?;

        let mut map = HashMap::new();
        if self.is_ordered() {
            // in ordered inserts, only the first n were attempted.
            for (i, id) in self.inserted_ids.iter().enumerate().take(body.n as usize) {
                map.insert(i, id.clone());
            }
        } else {
            // for unordered, add all the attempted ids and then remove the ones that have
            // associated write errors.
            for (i, id) in self.inserted_ids.iter().enumerate() {
                map.insert(i, id.clone());
            }

            if let Some(write_errors) = body.write_errors.as_ref() {
                for err in write_errors {
                    map.remove(&err.index);
                }
            }
        }

        if body.write_errors.is_some() || body.write_concern_error.is_some() {
            return Err(Error::new(
                ErrorKind::BulkWrite(BulkWriteFailure {
                    write_errors: body.write_errors,
                    write_concern_error: body.write_concern_error,
                    inserted_ids: map,
                }),
                body.labels,
            ));
        }

        Ok(InsertManyResult { inserted_ids: map })
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }

    fn retryability(&self) -> Retryability {
        Retryability::Write
    }
}
