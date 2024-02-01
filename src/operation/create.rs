use std::time::Duration;

use bson::{Bson, Document};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::{
    bson::doc, cmap::{Command, RawCommandResponse, StreamDescription}, collation::Collation, db::options::{ChangeStreamPreAndPostImages, ClusteredIndex, IndexOptionDefaults, TimeseriesOptions, ValidationAction, ValidationLevel}, error::Result, operation::{
        append_options,
        remove_empty_write_concern,
        OperationWithDefaults,
        WriteConcernOnlyBody,
    }, options::WriteConcern, serde_util, Namespace
};

#[derive(Debug)]
pub(crate) struct Create {
    ns: Namespace,
    options: Option<CreateCollectionOptions>,
}
#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateCollectionOptions {
    pub(crate) capped: Option<bool>,
    #[serde(serialize_with = "serde_util::serialize_u64_option_as_i64")]
    pub(crate) size: Option<u64>,
    #[serde(serialize_with = "serde_util::serialize_u64_option_as_i64")]
    pub(crate) max: Option<u64>,
    pub(crate) storage_engine: Option<Document>,
    pub(crate) validator: Option<Document>,
    pub(crate) validation_level: Option<ValidationLevel>,
    pub(crate) validation_action: Option<ValidationAction>,
    pub(crate) view_on: Option<String>,
    pub(crate) pipeline: Option<Vec<Document>>,
    pub(crate) collation: Option<Collation>,
    pub(crate) write_concern: Option<WriteConcern>,
    pub(crate) index_option_defaults: Option<IndexOptionDefaults>,
    pub(crate) timeseries: Option<TimeseriesOptions>,
    #[serde(default, with = "serde_util::duration_option_as_int_seconds")]
    pub(crate) expire_after_seconds: Option<Duration>,
    pub(crate) change_stream_pre_and_post_images: Option<ChangeStreamPreAndPostImages>,
    pub(crate) clustered_index: Option<ClusteredIndex>,
    pub(crate) comment: Option<Bson>,
    #[cfg(feature = "in-use-encryption-unstable")]
    pub(crate) encrypted_fields: Option<Document>,
}

impl Create {
    #[cfg(test)]
    fn empty() -> Self {
        Self::new(
            Namespace {
                db: String::new(),
                coll: String::new(),
            },
            None,
        )
    }

    pub(crate) fn new(ns: Namespace, options: Option<CreateCollectionOptions>) -> Self {
        Self { ns, options }
    }
}

impl OperationWithDefaults for Create {
    type O = ();
    type Command = Document;

    const NAME: &'static str = "create";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
        };

        remove_empty_write_concern!(self.options);
        append_options(&mut body, self.options.as_ref())?;

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
        let response: WriteConcernOnlyBody = response.body()?;
        response.validate()
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
