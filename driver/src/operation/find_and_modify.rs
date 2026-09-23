pub(crate) mod options;

use std::{fmt::Debug, marker::PhantomData};

use serde::{de::DeserializeOwned, Deserialize};

use self::options::FindAndModifyOptions;
use crate::{
    bson::{doc, rawdoc, Document, RawBson, RawDocumentBuf},
    bson_compat::{cstr, deserialize_from_slice, CStr},
    bson_util,
    cmap::{Command, RawCommandResponse, StreamDescription},
    coll::options::UpdateModifications,
    error::{Error, Result},
    operation::{
        append_options_to_raw_document,
        default_impl,
        find_and_modify::options::Modification,
        to_feature,
        ExecutionContext,
        Feature,
        Operation,
        OperationDetails,
        ResponseHandlingKind,
        Retryability,
        UpdateOrReplace,
    },
    options::ClientOptions,
    Collection,
};

pub(crate) struct FindAndModify<T: DeserializeOwned> {
    target: Collection<Document>,
    query: Document,
    modification: Modification,
    options: Option<FindAndModifyOptions>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: DeserializeOwned> FindAndModify<T> {
    pub(crate) fn with_modification(
        target: Collection<Document>,
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
            target,
            query,
            modification,
            options,
            _phantom: PhantomData,
        })
    }
}

impl<T: DeserializeOwned> Operation for FindAndModify<T> {
    type O = Option<T>;
    const NAME: &'static CStr = cstr!("findAndModify");

    default_impl!(
        name,
        extract_at_cluster_time,
        handle_error,
        update_for_retry,
        pinned_connection
    );

    fn details(&self, options: &ClientOptions) -> OperationDetails {
        OperationDetails {
            response_handling_kind: ResponseHandlingKind::Borrowed,
            selection_criteria: Feature::NotSupported,
            read_concern: Feature::NotSupported,
            write_concern: to_feature!(self.options, write_concern),
            supports_sessions: true,
            retryability: Retryability::write(options),
            is_backpressure_retryable: options.retry_writes != Some(false),
            override_criteria: None,
            target: (&self.target).into(),
            is_after_cluster_time_write: true,
        }
    }

    fn build(
        &mut self,
        _description: &StreamDescription,
        op_details: &OperationDetails,
    ) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: self.target.name(),
            "query": RawDocumentBuf::try_from(&self.query)?,
        };

        match &self.modification {
            Modification::Delete => body.append(cstr!("remove"), true),
            Modification::Update(update_or_replace) => {
                update_or_replace.append_to_rawdoc(&mut body, cstr!("update"))?
            }
        }

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::from_operation_details(
            op_details,
            self.name(),
            body,
        ))
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        response.validate_single_write()?;
        #[derive(Debug, Deserialize)]
        struct Response {
            // deserializing directly into Option<T> doesn't report an error if `value` is missing
            value: RawBson,
        }
        let body = response.body::<Response>()?;
        match body.value {
            RawBson::Document(ref doc) => Ok(Some(deserialize_from_slice(doc.as_bytes())?)),
            RawBson::Null => Ok(None),
            ref other => Err(Error::invalid_response(format!(
                "expected document for value field of findAndModify response, but instead got \
                 {other:?}",
            ))),
        }
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl<T: DeserializeOwned> crate::otel::OtelInfoDefaults for FindAndModify<T> {}
