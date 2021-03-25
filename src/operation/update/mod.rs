#[cfg(test)]
mod test;

use serde::Deserialize;

use crate::{
    bson::{doc, Bson, Document},
    bson_util,
    cmap::{Command, CommandResponse, StreamDescription},
    error::{convert_bulk_errors, Result},
    operation::{Operation, Retryability, WriteResponseBody},
    options::{UpdateModifications, UpdateOptions, WriteConcern},
    results::UpdateResult,
    Namespace,
};

#[derive(Debug)]
pub(crate) struct Update {
    ns: Namespace,
    filter: Document,
    update: UpdateModifications,
    multi: Option<bool>,
    options: Option<UpdateOptions>,
}

impl Update {
    #[cfg(test)]
    fn empty() -> Self {
        Update::new(
            Namespace {
                db: String::new(),
                coll: String::new(),
            },
            Document::new(),
            UpdateModifications::Document(Document::new()),
            false,
            None,
        )
    }

    pub(crate) fn new(
        ns: Namespace,
        filter: Document,
        update: UpdateModifications,
        multi: bool,
        options: Option<UpdateOptions>,
    ) -> Self {
        Self {
            ns,
            filter,
            update,
            multi: if multi { Some(true) } else { None },
            options,
        }
    }
}

impl Operation for Update {
    type O = UpdateResult;
    const NAME: &'static str = "update";

    fn build(&self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
        };

        let mut update = doc! {
            "q": self.filter.clone(),
            "u": self.update.to_bson(),
        };

        if let Some(ref options) = self.options {
            if let Some(upsert) = options.upsert {
                update.insert("upsert", upsert);
            }

            if let Some(ref array_filters) = options.array_filters {
                update.insert("arrayFilters", bson_util::to_bson_array(array_filters));
            }

            if let Some(ref hint) = options.hint {
                update.insert("hint", hint.to_bson());
            }

            if let Some(ref collation) = options.collation {
                update.insert("collation", bson::to_bson(collation)?);
            }

            if let Some(bypass_doc_validation) = options.bypass_document_validation {
                body.insert("bypassDocumentValidation", bypass_doc_validation);
            }

            if let Some(ref write_concern) = options.write_concern {
                body.insert("writeConcern", bson::to_bson(write_concern)?);
            }
        };

        if let Some(multi) = self.multi {
            update.insert("multi", multi);
        }

        body.insert("updates", vec![Bson::Document(update)]);
        body.insert("ordered", true); // command monitoring tests expect this (SPEC-1130)

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
        let body: WriteResponseBody<UpdateBody> = response.body()?;
        body.validate().map_err(convert_bulk_errors)?;

        let modified_count = body.n_modified;
        let upserted_id = body
            .upserted
            .as_ref()
            .and_then(|v| v.first())
            .and_then(|doc| doc.get("_id"))
            .map(Clone::clone);

        let matched_count = if upserted_id.is_some() { 0 } else { body.n };

        Ok(UpdateResult {
            matched_count,
            modified_count,
            upserted_id,
        })
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }

    fn retryability(&self) -> Retryability {
        if self.multi != Some(true) {
            Retryability::Write
        } else {
            Retryability::None
        }
    }
}

#[derive(Deserialize)]
struct UpdateBody {
    #[serde(rename = "nModified")]
    n_modified: i64,
    upserted: Option<Vec<Document>>,
}
