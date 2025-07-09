use crate::bson::rawdoc;

use crate::{
    bson_compat::{cstr, CStr},
    bson_util::to_raw_bson_array_ser,
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::{ErrorKind, Result},
    index::IndexModel,
    operation::{append_options_to_raw_document, OperationWithDefaults},
    options::{CreateIndexOptions, WriteConcern},
    results::CreateIndexesResult,
    Namespace,
};

use super::{ExecutionContext, WriteConcernOnlyBody};

#[derive(Debug)]
pub(crate) struct CreateIndexes {
    ns: Namespace,
    indexes: Vec<IndexModel>,
    options: Option<CreateIndexOptions>,
}

impl CreateIndexes {
    pub(crate) fn new(
        ns: Namespace,
        indexes: Vec<IndexModel>,
        options: Option<CreateIndexOptions>,
    ) -> Self {
        Self {
            ns,
            indexes,
            options,
        }
    }
}

impl OperationWithDefaults for CreateIndexes {
    type O = CreateIndexesResult;
    const NAME: &'static CStr = cstr!("createIndexes");

    fn build(&mut self, description: &StreamDescription) -> Result<Command> {
        // commit quorum is not supported on < 4.4
        if description.max_wire_version.unwrap_or(0) < 9
            && self
                .options
                .as_ref()
                .is_some_and(|options| options.commit_quorum.is_some())
        {
            return Err(ErrorKind::InvalidArgument {
                message: "Specifying a commit quorum to create_index(es) is not supported on \
                          server versions < 4.4"
                    .to_string(),
            }
            .into());
        }

        self.indexes.iter_mut().for_each(|i| i.update_name()); // Generate names for unnamed indexes.
        let indexes = to_raw_bson_array_ser(&self.indexes)?;
        let mut body = rawdoc! {
            Self::NAME: self.ns.coll.clone(),
            "indexes": indexes,
        };

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::new(Self::NAME, &self.ns.db, body))
    }

    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        let response: WriteConcernOnlyBody = response.body()?;
        response.validate()?;
        let index_names = self.indexes.iter().filter_map(|i| i.get_name()).collect();
        Ok(CreateIndexesResult { index_names })
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
