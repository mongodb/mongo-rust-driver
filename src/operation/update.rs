use serde::{Deserialize, Serialize};

use crate::{
    bson::{doc, rawdoc, Document, RawArrayBuf, RawBson, RawDocumentBuf},
    bson_util,
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::{convert_bulk_errors, Result},
    operation::{OperationWithDefaults, Retryability, WriteResponseBody},
    options::{UpdateModifications, UpdateOptions, WriteConcern},
    results::UpdateResult,
    serde_util::to_raw_document_buf_with_options,
    ClientSession,
    Namespace,
};

use super::{handle_response_sync, OperationResponse};

#[derive(Clone, Debug)]
pub(crate) enum UpdateOrReplace<'a, T = ()> {
    UpdateModifications(UpdateModifications),
    Replacement(&'a T),
}

impl<'a, T: Serialize> UpdateOrReplace<'a, T> {
    pub(crate) fn to_raw_bson(&self, human_readable_serialization: bool) -> Result<RawBson> {
        match self {
            Self::UpdateModifications(update_modifications) => match update_modifications {
                UpdateModifications::Document(document) => {
                    Ok(RawDocumentBuf::from_document(document)?.into())
                }
                UpdateModifications::Pipeline(pipeline) => bson_util::to_raw_bson_array(pipeline),
            },
            Self::Replacement(replacement) => {
                let replacement_doc =
                    to_raw_document_buf_with_options(replacement, human_readable_serialization)?;
                bson_util::replacement_raw_document_check(&replacement_doc)?;
                Ok(replacement_doc.into())
            }
        }
    }
}

impl From<UpdateModifications> for UpdateOrReplace<'_> {
    fn from(update_modifications: UpdateModifications) -> Self {
        Self::UpdateModifications(update_modifications)
    }
}

impl<'a, T: Serialize> From<&'a T> for UpdateOrReplace<'a, T> {
    fn from(t: &'a T) -> Self {
        Self::Replacement(t)
    }
}

#[derive(Debug)]
pub(crate) struct Update<'a, T = ()> {
    ns: Namespace,
    filter: Document,
    update: UpdateOrReplace<'a, T>,
    multi: Option<bool>,
    options: Option<UpdateOptions>,
    human_readable_serialization: bool,
}

impl Update<'_> {
    pub(crate) fn with_update(
        ns: Namespace,
        filter: Document,
        update: UpdateModifications,
        multi: bool,
        options: Option<UpdateOptions>,
        human_readable_serialization: bool,
    ) -> Self {
        Self {
            ns,
            filter,
            update: update.into(),
            multi: multi.then_some(true),
            options,
            human_readable_serialization,
        }
    }
}

impl<'a, T: Serialize> Update<'a, T> {
    pub(crate) fn with_replace(
        ns: Namespace,
        filter: Document,
        update: &'a T,
        multi: bool,
        options: Option<UpdateOptions>,
        human_readable_serialization: bool,
    ) -> Self {
        Self {
            ns,
            filter,
            update: update.into(),
            multi: multi.then_some(true),
            options,
            human_readable_serialization,
        }
    }
}

impl<'a, T: Serialize> OperationWithDefaults for Update<'a, T> {
    type O = UpdateResult;
    type Command = RawDocumentBuf;

    const NAME: &'static str = "update";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command<Self::Command>> {
        let mut body = rawdoc! {
            Self::NAME: self.ns.coll.clone(),
        };

        let mut update = rawdoc! {
            "q": RawDocumentBuf::from_document(&self.filter)?,
            "u": self.update.to_raw_bson(self.human_readable_serialization)?,
        };

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

    fn handle_response(
        &self,
        raw_response: RawCommandResponse,
        _description: &StreamDescription,
        _session: Option<&mut ClientSession>,
    ) -> OperationResponse<'static, Self::O> {
        handle_response_sync! {{
            let response: WriteResponseBody<UpdateBody> = raw_response.body_utf8_lossy()?;
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
        }}
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
