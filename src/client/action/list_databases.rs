use std::future::IntoFuture;

use bson::{Document, Bson};
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::{Client, ClientSession, operation::ListDatabases, error::Result, results::DatabaseSpecification, client::BoxFuture};

impl Client {
    /// Gets information about each database present in the cluster the Client is connected to.
    pub async fn list_databases(
        &self,
        filter: impl Into<Option<Document>>,
    ) -> ListDatabasesAction {
        ListDatabasesAction {
            client: &self,
            filter: filter.into(),
            options: Default::default(),
            session: None,
        }
    }
}

#[must_use]
pub struct ListDatabasesAction<'a> {
    client: &'a Client,
    filter: Option<Document>,
    options: Option<Options>,
    session: Option<&'a mut ClientSession>,
}

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Options {
    pub(crate) authorized_databases: Option<bool>,
    pub(crate) comment: Option<Bson>,
}

impl<'a> ListDatabasesAction<'a> {
    fn options(&mut self) -> &mut Options {
        self.options.get_or_insert_with(Options::default)
    }

    /// Determines which databases to return based on the user's access privileges. This option is
    /// only supported on server versions 4.0.5+.
    pub fn authorized_databases(mut self, value: bool) -> Self {
        self.options().authorized_databases = Some(value);
        self
    }

    /// Tags the query with an arbitrary [`Bson`] value to help trace the operation through the
    /// database profiler, currentOp and logs.
    ///
    /// This option is only available on server versions 4.4+.
    pub fn comment(mut self, value: Bson) -> Self {
        self.options().comment = Some(value);
        self
    }
}

impl<'a> IntoFuture for ListDatabasesAction<'a> {
    type Output = Result<Vec<DatabaseSpecification>>;

    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async {
            let op = ListDatabases::new(self.filter, false, self.options);
            self.client.execute_operation(op, self.session).await.and_then(|dbs| {
                dbs.into_iter()
                    .map(|db_spec| {
                        bson::from_slice(db_spec.as_bytes()).map_err(crate::error::Error::from)
                    })
                    .collect()
            })
        }.boxed()
    }
}