use futures_util::FutureExt;

use serde::Deserialize;

use crate::{
    bson::{doc, rawdoc, Document, RawArrayBuf, RawBson, RawDocumentBuf},
    bson_util,
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::{convert_bulk_errors, Result},
    operation::{OperationWithDefaults, Retryability, WriteResponseBody},
    options::{UpdateModifications, UpdateOptions, WriteConcern},
    results::UpdateResult,
    BoxFuture,
    Namespace,
};

use super::ExecutionContext;

#[derive(Clone, Debug)]
pub(crate) enum UpdateOrReplace {
    UpdateModifications(UpdateModifications),
    Replacement(RawDocumentBuf),
}

impl UpdateOrReplace {
    pub(crate) fn append_to_rawdoc(&self, doc: &mut RawDocumentBuf, key: &str) -> Result<()> {
        match self {
            Self::UpdateModifications(update_modifications) => match update_modifications {
                UpdateModifications::Document(document) => {
                    let raw = RawDocumentBuf::from_document(document)?;
                    doc.append(key, raw);
                }
                UpdateModifications::Pipeline(pipeline) => {
                    let raw = bson_util::to_raw_bson_array(pipeline)?;
                    doc.append(key, raw);
                }
            },
            Self::Replacement(replacement_doc) => {
                bson_util::replacement_raw_document_check(replacement_doc)?;
                doc.append_ref(key, replacement_doc);
            }
        }

        Ok(())
    }
}

impl From<UpdateModifications> for UpdateOrReplace {
    fn from(update_modifications: UpdateModifications) -> Self {
        Self::UpdateModifications(update_modifications)
    }
}

#[derive(Debug)]
pub(crate) struct Update {
    ns: Namespace,
    filter: Document,
    update: UpdateOrReplace,
    multi: Option<bool>,
    options: Option<UpdateOptions>,
}

impl Update {
    pub(crate) fn with_update(
        ns: Namespace,
        filter: Document,
        update: UpdateModifications,
        multi: bool,
        options: Option<UpdateOptions>,
    ) -> Self {
        Self {
            ns,
            filter,
            update: update.into(),
            multi: multi.then_some(true),
            options,
        }
    }

    pub(crate) fn with_replace_raw(
        ns: Namespace,
        filter: Document,
        update: RawDocumentBuf,
        multi: bool,
        options: Option<UpdateOptions>,
    ) -> Result<Self> {
        Ok(Self {
            ns,
            filter,
            update: UpdateOrReplace::Replacement(update),
            multi: multi.then_some(true),
            options,
        })
    }
}

impl OperationWithDefaults for Update {
    type O = UpdateResult;
    type Command = RawDocumentBuf;

    const NAME: &'static str = "update";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command<Self::Command>> {
        let mut body = rawdoc! {
            Self::NAME: self.ns.coll.clone(),
        };

        let mut update = rawdoc! {
            "q": RawDocumentBuf::from_document(&self.filter)?,
        };
        self.update.append_to_rawdoc(&mut update, "u")?;

        if let Some(ref options) = self.options {
            if let Some(upsert) = options.upsert {
                update.append("upsert", upsert);
            }

            if let Some(ref array_filters) = options.array_filters {
                update.append("arrayFilters", bson_util::to_raw_bson_array(array_filters)?);
            }

            if let Some(ref hint) = options.hint {
                update.append("hint", hint.to_raw_bson()?);
            }

            if let Some(ref collation) = options.collation {
                update.append("collation", bson::to_raw_document_buf(&collation)?);
            }

            if let Some(bypass_doc_validation) = options.bypass_document_validation {
                body.append("bypassDocumentValidation", bypass_doc_validation);
            }

            if let Some(ref write_concern) = options.write_concern {
                if !write_concern.is_empty() {
                    body.append("writeConcern", bson::to_raw_document_buf(write_concern)?);
                }
            }

            if let Some(ref let_vars) = options.let_vars {
                body.append("let", bson::to_raw_document_buf(&let_vars)?);
            }

            if let Some(ref comment) = options.comment {
                body.append("comment", RawBson::try_from(comment.clone())?);
            }
        };

        if let Some(multi) = self.multi {
            update.append("multi", multi);
        }

        let mut updates = RawArrayBuf::new();
        updates.push(update);
        body.append("updates", updates);
        body.append("ordered", true); // command monitoring tests expect this (SPEC-1130)

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> BoxFuture<'a, Result<Self::O>> {
        async move {
            let response: WriteResponseBody<UpdateBody> = response.body_utf8_lossy()?;
            response.validate().map_err(convert_bulk_errors)?;

            let modified_count = response.n_modified;
            let upserted_id = response
                .upserted
                .as_ref()
                .and_then(|v| v.first())
                .and_then(|doc| doc.get("_id"))
                .cloned();

            let matched_count = if upserted_id.is_some() {
                0
            } else {
                response.body.n
            };

            Ok(UpdateResult {
                matched_count,
                modified_count,
                upserted_id,
            })
        }
        .boxed()
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
pub(crate) struct UpdateBody {
    n: u64,
    #[serde(rename = "nModified")]
    n_modified: u64,
    upserted: Option<Vec<Document>>,
}
