pub(crate) mod options;

use std::{fmt::Debug, marker::PhantomData};

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
};

use super::UpdateOrReplace;

pub(crate) struct FindAndModify<T: DeserializeOwned> {
    ns: Namespace,
    query: Document,
    modification: Modification,
    options: Option<FindAndModifyOptions>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: DeserializeOwned> FindAndModify<T> {
    pub(crate) fn with_modification(
        ns: Namespace,
        query: Document,
        modification: Modification,
        options: Option<FindAndModifyOptions>,
    ) -> Self {
        Self {
            ns,
            query,
            modification,
            options,
            _phantom: PhantomData,
        }
    }
}

impl<T: DeserializeOwned> FindAndModify<T> {
    pub fn with_delete(
        ns: Namespace,
        query: Document,
        options: Option<FindOneAndDeleteOptions>,
    ) -> Self {
        FindAndModify {
            ns,
            query,
            modification: Modification::Delete,
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
            options: options.map(Into::into),
            _phantom: Default::default(),
        })
    }

    pub fn with_replace<R: Serialize>(
        ns: Namespace,
        query: Document,
        replacement: &R,
        options: Option<FindOneAndReplaceOptions>,
        human_readable_serialization: bool,
    ) -> Result<Self> {
        Ok(FindAndModify {
            ns,
            query,
            modification: Modification::Update(UpdateOrReplace::replacement(replacement, human_readable_serialization)?),
            options: options.map(Into::into),
            _phantom: Default::default(),
        })
    }
}

impl<T: DeserializeOwned> OperationWithDefaults for FindAndModify<T> {
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
                update_or_replace.to_raw_bson()?,
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
    ) -> Result<Self::O> {
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
                    "expected document for value field of findAndModify response, but instead got \
                     {:?}",
                    other
                ),
            }
            .into()),
        }
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options.as_ref().and_then(|o| o.write_concern.as_ref())
    }

    fn retryability(&self) -> Retryability {
        Retryability::Write
    }
}
