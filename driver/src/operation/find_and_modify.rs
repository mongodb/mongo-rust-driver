pub(crate) mod options;

use std::{fmt::Debug, marker::PhantomData};

use serde::{de::DeserializeOwned, Deserialize};

use self::options::FindAndModifyOptions;
use crate::{
    bson::{doc, rawdoc, Document, RawBson, RawDocumentBuf},
    bson_compat::{cstr, deserialize_from_slice, CStr},
    bson_util,
    cmap::{Command, RawCommandResponse, StreamDescription},
    coll::{options::UpdateModifications, Namespace},
    error::{ErrorKind, Result},
    operation::{
        append_options_to_raw_document,
        find_and_modify::options::Modification,
        OperationWithDefaults,
        Retryability,
    },
    options::WriteConcern,
};

use super::{ExecutionContext, UpdateOrReplace};

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
    ) -> Result<Self> {
        if let Modification::Update(UpdateOrReplace::UpdateModifications(
            UpdateModifications::Document(d),
        )) = &modification
        {
            bson_util::update_document_check(d)?;
        };
        Ok(Self {
            ns,
            query,
            modification,
            options,
            _phantom: PhantomData,
        })
    }
}

impl<T: DeserializeOwned> OperationWithDefaults for FindAndModify<T> {
    type O = Option<T>;
    const NAME: &'static CStr = cstr!("findAndModify");

    fn build(&mut self, description: &StreamDescription) -> Result<Command> {
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
            "query": RawDocumentBuf::try_from(&self.query)?,
        };

        match &self.modification {
            Modification::Delete => body.append(cstr!("remove"), true),
            Modification::Update(update_or_replace) => {
                update_or_replace.append_to_rawdoc(&mut body, cstr!("update"))?
            }
        }

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::new(Self::NAME, &self.ns.db, body))
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        #[derive(Debug, Deserialize)]
        pub(crate) struct Response {
            value: RawBson,
        }
        let response: Response = response.body()?;

        match response.value {
            RawBson::Document(doc) => Ok(Some(deserialize_from_slice(doc.as_bytes())?)),
            RawBson::Null => Ok(None),
            other => Err(ErrorKind::InvalidResponse {
                message: format!(
                    "expected document for value field of findAndModify response, but instead got \
                     {other:?}"
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

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl<T: DeserializeOwned> crate::otel::OtelInfoDefaults for FindAndModify<T> {
    fn target(&self) -> crate::otel::OperationTarget<'_> {
        (&self.ns).into()
    }
}
