mod options;

use std::fmt::Debug;

use bson::{from_slice, RawBson};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use self::options::FindAndModifyOptions;
use crate::{
    bson::{doc, rawdoc, Document, RawDocumentBuf},
    bson_util,
    cmap::{Command, RawCommandResponse, StreamDescription},
    coll::{
        options::{
            FindOneAndDeleteOptions,
            FindOneAndReplaceOptions,
            FindOneAndUpdateOptions,
            UpdateModifications,
        },
        Namespace,
    },
    error::{ErrorKind, Result},
    operation::{
        append_options_to_raw_document,
        find_and_modify::options::Modification,
        remove_empty_write_concern,
        OperationWithDefaults,
        Retryability,
    },
    options::WriteConcern,
    ClientSession,
};

use super::{handle_response_sync, OperationResponse};

pub(crate) struct FindAndModify<'a, R, T: DeserializeOwned> {
    ns: Namespace,
    query: Document,
    modification: Modification<'a, R>,
    human_readable_serialization: Option<bool>,
    options: Option<FindAndModifyOptions>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: DeserializeOwned> FindAndModify<'_, (), T> {
    pub fn with_delete(
        ns: Namespace,
        query: Document,
        options: Option<FindOneAndDeleteOptions>,
    ) -> Self {
        FindAndModify {
            ns,
            query,
            modification: Modification::Delete,
            human_readable_serialization: None,
            options: options.map(Into::into),
            _phantom: Default::default(),
        }
    }

    pub fn with_update(
        ns: Namespace,
        query: Document,
        update: UpdateModifications,
        options: Option<FindOneAndUpdateOptions>,
    ) -> Result<Self> {
        if let UpdateModifications::Document(ref d) = update {
            bson_util::update_document_check(d)?;
        };
        Ok(FindAndModify {
            ns,
            query,
            modification: Modification::Update(update.into()),
            human_readable_serialization: None,
            options: options.map(Into::into),
            _phantom: Default::default(),
        })
    }
}

impl<'a, R: Serialize, T: DeserializeOwned> FindAndModify<'a, R, T> {
    pub fn with_replace(
        ns: Namespace,
        query: Document,
        replacement: &'a R,
        options: Option<FindOneAndReplaceOptions>,
        human_readable_serialization: bool,
    ) -> Result<Self> {
        Ok(FindAndModify {
            ns,
            query,
            modification: Modification::Update(replacement.into()),
            human_readable_serialization: Some(human_readable_serialization),
            options: options.map(Into::into),
            _phantom: Default::default(),
        })
    }
}

impl<'a, R: Serialize, T: DeserializeOwned> OperationWithDefaults for FindAndModify<'a, R, T> {
    type O = Option<T>;
    type Command = RawDocumentBuf;
    const NAME: &'static str = "findAndModify";

    fn build(&mut self, description: &StreamDescription) -> Result<Command<Self::Command>> {
        if let Some(ref options) = self.options {
            if options.hint.is_some() && description.max_wire_version.unwrap_or(0) < 8 {
                return Err(ErrorKind::InvalidArgument {
                    message: "Specifying a hint to find_one_and_x is not supported on server \
                              versions < 4.4"
                        .to_string(),
                }
                .into());
            }
        }

        let mut body = rawdoc! {
            Self::NAME: self.ns.coll.clone(),
            "query": RawDocumentBuf::from_document(&self.query)?,
        };

        let (key, modification) = match &self.modification {
            Modification::Delete => ("remove", true.into()),
            Modification::Update(update_or_replace) => (
                "update",
                update_or_replace
                    .to_raw_bson(self.human_readable_serialization.unwrap_or_default())?,
            ),
        };
        body.append(key, modification);

        if let Some(ref mut options) = self.options {
            remove_empty_write_concern!(Some(options));
        }
        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(
        &self,
        response: RawCommandResponse,
        _description: &StreamDescription,
        _session: Option<&mut ClientSession>,
    ) -> OperationResponse<'static, Self::O> {
        handle_response_sync! {{
            #[derive(Debug, Deserialize)]
            pub(crate) struct Response {
                value: RawBson,
            }
            let response: Response = response.body()?;

            match response.value {
                RawBson::Document(doc) => Ok(Some(from_slice(doc.as_bytes())?)),
                RawBson::Null => Ok(None),
                other => Err(ErrorKind::InvalidResponse {
                    message: format!(
                        "expected document for value field of findAndModify response, but instead \
                         got {:?}",
                        other
                    ),
                }
                .into()),
            }
        }}
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options.as_ref().and_then(|o| o.write_concern.as_ref())
    }

    fn retryability(&self) -> Retryability {
        Retryability::Write
    }
}
