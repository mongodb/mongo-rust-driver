#[cfg(test)]
mod test;

use std::marker::PhantomData;

use serde::de::DeserializeOwned;

use crate::{
    bson::{doc, Document},
    cmap::{Command, StreamDescription},
    cursor::CursorSpecification,
    error::Result,
    operation::{append_options, CursorBody, Operation, Retryability},
    options::{ListCollectionsOptions, ReadPreference, SelectionCriteria},
};

use super::CommandResponse;

#[derive(Debug)]
pub(crate) struct ListCollections<T> {
    db: String,
    filter: Option<Document>,
    name_only: bool,
    options: Option<ListCollectionsOptions>,
    _phantom: PhantomData<T>,
}

impl<T> ListCollections<T> {
    #[cfg(test)]
    fn empty() -> Self {
        Self::new(String::new(), None, false, None)
    }

    pub(crate) fn new(
        db: String,
        filter: Option<Document>,
        name_only: bool,
        options: Option<ListCollectionsOptions>,
    ) -> Self {
        Self {
            db,
            filter,
            name_only,
            options,
            _phantom: PhantomData::default(),
        }
    }
}

impl<T> Operation for ListCollections<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    type O = CursorSpecification<T>;
    type Response = CommandResponse<CursorBody<T>>;

    const NAME: &'static str = "listCollections";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: 1,
        };

        let mut name_only = self.name_only;
        if let Some(ref filter) = self.filter {
            body.insert("filter", filter.clone());

            if name_only && filter.keys().any(|k| k != "name") {
                name_only = false;
            }
        }
        body.insert("nameOnly", name_only);

        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new(Self::NAME.to_string(), self.db.clone(), body))
    }

    fn handle_response(
        &self,
        response: CursorBody<T>,
        description: &StreamDescription,
    ) -> Result<Self::O> {
        Ok(CursorSpecification::new(
            response.cursor,
            description.server_address.clone(),
            self.options.as_ref().and_then(|opts| opts.batch_size),
            None,
        ))
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        Some(SelectionCriteria::ReadPreference(ReadPreference::Primary)).as_ref()
    }

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }
}
