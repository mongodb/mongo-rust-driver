mod options;
#[cfg(test)]
mod test;

use std::fmt::Debug;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use self::options::FindAndModifyOptions;
use crate::{
    bson::{doc, from_document, Bson, Document},
    bson_util,
    cmap::{Command, CommandResponse, StreamDescription},
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

pub(crate) struct FindAndModify<T = Document>
where
    T: Serialize + DeserializeOwned + Unpin + Debug,
{
    ns: Namespace,
    query: Document,
    options: FindAndModifyOptions,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> FindAndModify<T>
where
    T: Serialize + DeserializeOwned + Unpin + Debug,
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
    T: Serialize + DeserializeOwned + Unpin + Debug,
{
    type O = Option<T>;
    const NAME: &'static str = "findAndModify";

    fn build(&self, description: &StreamDescription) -> Result<Command> {
        if self.options.hint.is_some() {
            match description.max_wire_version {
                Some(version) if version < 8 => {
                    return Err(ErrorKind::OperationError {
                        message: "Specifying a hint is not supported on server versions < 4.4"
                            .to_string(),
                    }
                    .into());
                }
                None => {
                    return Err(ErrorKind::OperationError {
                        message: "Specifying a hint is not supported on server versions < 4.4"
                            .to_string(),
                    }
                    .into());
                }
                _ => {}
            }
        }

        let mut body: Document = doc! {
            Self::NAME: self.ns.coll.clone(),
            "query": self.query.clone(),
        };

        append_options(&mut body, Some(&self.options))?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(
        &self,
        response: CommandResponse,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        let body: ResponseBody = response.body()?;
        match body.value {
            Bson::Document(doc) => Ok(Some(from_document(doc)?)),
            Bson::Null => Ok(None),
            other => Err(ErrorKind::ResponseError {
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
struct ResponseBody {
    value: Bson,
}
