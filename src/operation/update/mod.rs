#[cfg(test)]
mod test;

use bson::{bson, doc, Bson, Document};
use serde::Deserialize;

use crate::{
    bson_util,
    cmap::{Command, CommandResponse, StreamDescription},
    error::{convert_bulk_errors, Result},
    operation::{Operation, WriteResponseBody},
    options::{UpdateModifications, UpdateOptions},
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

    fn build(&self, description: &StreamDescription) -> Result<Command> {
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

            if let Some(bypass_doc_validation) = options.bypass_document_validation {
                body.insert("bypassDocumentValidation", bypass_doc_validation);
            }

            if let Some(ref write_concern) = options.write_concern {
                body.insert("writeConcern", write_concern.to_bson());
            }
        };

        if let Some(multi) = self.multi {
            update.insert("multi", multi);
        }

        body.insert("updates", vec![Bson::Document(update)]);

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(&self, response: CommandResponse) -> Result<Self::O> {
        let body: WriteResponseBody<UpdateBody> = response.body()?;
        body.validate().map_err(convert_bulk_errors)?;

        let matched_count = body.n;
        let modified_count = body.n_modified;
        let upserted_id = body
            .upserted
            .as_ref()
            .and_then(|v| v.first())
            .and_then(|doc| doc.get("_id"))
            .map(Clone::clone);
        Ok(UpdateResult {
            matched_count,
            modified_count,
            upserted_id,
        })
    }
}

#[derive(Deserialize)]
struct UpdateBody {
    #[serde(rename = "nModified")]
    n_modified: i64,
    upserted: Option<Vec<Document>>,
}
