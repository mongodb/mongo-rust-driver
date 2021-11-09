mod options;
#[cfg(test)]
mod test;

use std::fmt::Debug;

use serde::{de::DeserializeOwned, Deserialize};

use self::options::FindAndModifyOptions;
use crate::{
    bson::{doc, from_document, Bson, Document},
    bson_util,
    cmap::{Command, StreamDescription},
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
    operation::{append_options, Operation, Retryability},
    options::WriteConcern,
};

use super::CommandResponse;

pub(crate) struct FindAndModify<T = Document>
where
    T: DeserializeOwned,
{
    ns: Namespace,
    query: Document,
    options: FindAndModifyOptions,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> FindAndModify<T>
where
    T: DeserializeOwned,
{
    pub fn with_delete(
        ns: Namespace,
        query: Document,
        options: Option<FindOneAndDeleteOptions>,
    ) -> Self {
        let options =
            FindAndModifyOptions::from_find_one_and_delete_options(options.unwrap_or_default());
        FindAndModify {
            ns,
            query,
            options,
            _phantom: Default::default(),
        }
    }

    pub fn with_replace(
        ns: Namespace,
        query: Document,
        replacement: Document,
        options: Option<FindOneAndReplaceOptions>,
    ) -> Result<Self> {
        bson_util::replacement_document_check(&replacement)?;
        let options = FindAndModifyOptions::from_find_one_and_replace_options(
            replacement,
            options.unwrap_or_default(),
        );
        Ok(FindAndModify {
            ns,
            query,
            options,
            _phantom: Default::default(),
        })
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
        let options = FindAndModifyOptions::from_find_one_and_update_options(
            update,
            options.unwrap_or_default(),
        );
        Ok(FindAndModify {
            ns,
            query,
            options,
            _phantom: Default::default(),
        })
    }
}

impl<T> Operation for FindAndModify<T>
where
    T: DeserializeOwned,
{
    type O = Option<T>;
    type Command = Document;
    type Response = CommandResponse<Response>;
    const NAME: &'static str = "findAndModify";

    fn build(&mut self, description: &StreamDescription) -> Result<Command> {
        if self.options.hint.is_some() && description.max_wire_version.unwrap_or(0) < 8 {
            return Err(ErrorKind::InvalidArgument {
                message: "Specifying a hint to find_one_and_x is not supported on server versions \
                          < 4.4"
                    .to_string(),
            }
            .into());
        }

        let mut body: Document = doc! {
            Self::NAME: self.ns.coll.clone(),
            "query": self.query.clone(),
        };

        let mut options = self.options.clone();
        if let Some(write_concern) = self.write_concern() {
            if *write_concern == Default::default() {
                options.write_concern = None;
            }
        }
        append_options(&mut body, Some(&options))?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(
        &self,
        response: Response,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        match response.value {
            Bson::Document(doc) => Ok(Some(from_document(doc)?)),
            Bson::Null => Ok(None),
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
        self.options.write_concern.as_ref()
    }

    fn retryability(&self) -> Retryability {
        Retryability::Write
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct Response {
    value: Bson,
}
