mod options;
#[cfg(test)]
mod test;

use bson::{doc, Bson, Document};
use serde::Deserialize;

use self::options::FindAndModifyOptions;
use crate::{
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
    operation::{append_options, Operation},
};

pub(crate) struct FindAndModify {
    ns: Namespace,
    query: Document,
    options: FindAndModifyOptions,
}

impl FindAndModify {
    pub fn with_delete(
        ns: Namespace,
        query: Document,
        options: Option<FindOneAndDeleteOptions>,
    ) -> Self {
        let options =
            FindAndModifyOptions::from_find_one_and_delete_options(options.unwrap_or_default());
        FindAndModify { ns, query, options }
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
        Ok(FindAndModify { ns, query, options })
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
        Ok(FindAndModify { ns, query, options })
    }
}

impl Operation for FindAndModify {
    type O = Option<Document>;
    const NAME: &'static str = "findAndModify";

    fn build(&self, description: &StreamDescription) -> Result<Command> {
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
    fn handle_response(&self, response: CommandResponse) -> Result<Self::O> {
        let body: ResponseBody = response.body()?;
        match body.value {
            Bson::Document(doc) => Ok(Some(doc)),
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
}

#[derive(Debug, Deserialize)]
struct ResponseBody {
    value: Bson,
}
