use bson::{Document, doc};

use crate::{Namespace, cmap::Command};

use super::OperationWithDefaults;


#[derive(Debug)]
pub(crate) struct UpdateSearchIndex {
    ns: Namespace,
    name: String,
    definition: Document,
}

impl UpdateSearchIndex {
    pub(crate) fn new(
        ns: Namespace,
        name: String,
        definition: Document,
    ) -> Self {
        Self { ns, name, definition }
    }
}

impl OperationWithDefaults for UpdateSearchIndex {
    type O = ();
    type Command = Document;
    const NAME: &'static str = "updateSearchIndex";

    fn build(&mut self, _description: &crate::cmap::StreamDescription) -> crate::error::Result<crate::cmap::Command<Self::Command>> {
        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            doc! {
                Self::NAME: self.ns.coll.clone(),
                "name": &self.name,
                "definition": &self.definition,
            },
        ))
    }

    fn handle_response(
        &self,
        response: crate::cmap::RawCommandResponse,
        _description: &crate::cmap::StreamDescription,
    ) -> crate::error::Result<Self::O> {
        response.body()
    }
}