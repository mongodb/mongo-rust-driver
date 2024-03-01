use std::marker::PhantomData;

use bson::{Bson, Document};

#[cfg(feature = "sync")]
use crate::sync::Client as SyncClient;
use crate::{
    db::options::ListDatabasesOptions,
    error::{ErrorKind, Result},
    operation::list_databases as op,
    results::DatabaseSpecification,
    Client,
    ClientSession,
};

use super::{action_impl, option_setters, ListNames, ListSpecifications};

impl Client {
    /// Gets information about each database present in the cluster the Client is connected to.
    ///
    /// `await` will return `Result<Vec<`[`DatabaseSpecification`]`>>`.
    pub fn list_databases(&self) -> ListDatabases {
        ListDatabases {
            client: self,
            options: Default::default(),
            session: None,
            mode: PhantomData,
        }
    }

    /// Gets the names of the databases present in the cluster the Client is connected to.
    ///
    /// `await` will return `Result<Vec<String>>`.
    pub fn list_database_names(&self) -> ListDatabases<'_, ListNames> {
        ListDatabases {
            client: self,
            options: Default::default(),
            session: None,
            mode: PhantomData,
        }
    }
}

#[cfg(feature = "sync")]
impl SyncClient {
    /// Gets information about each database present in the cluster the Client is connected to.
    ///
    /// [run](ListDatabases::run) will return `Result<Vec<`[`DatabaseSpecification`]`>>`.
    pub fn list_databases(&self) -> ListDatabases {
        self.async_client.list_databases()
    }

    /// Gets the names of the databases present in the cluster the Client is connected to.
    ///
    /// [run](ListDatabases::run) will return `Result<Vec<String>>`.
    pub fn list_database_names(&self) -> ListDatabases<'_, ListNames> {
        self.async_client.list_database_names()
    }
}

/// Gets information about each database present in the cluster the Client is connected to.  Create
/// by calling [`Client::list_databases`] or [`Client::list_database_names`].
#[must_use]
pub struct ListDatabases<'a, M = ListSpecifications> {
    client: &'a Client,
    options: Option<ListDatabasesOptions>,
    session: Option<&'a mut ClientSession>,
    mode: PhantomData<M>,
}

impl<'a, M> ListDatabases<'a, M> {
    option_setters!(options: ListDatabasesOptions;
        filter: Document,
        authorized_databases: bool,
        comment: Bson,
    );

    /// Runs the query using the provided session.
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

action_impl! {
    impl<'a> Action for ListDatabases<'a, ListSpecifications> {
        type Future = ListDatabasesFuture;

        async fn execute(self) -> Result<Vec<DatabaseSpecification>> {
            let op = op::ListDatabases::new(false, self.options);
                self.client
                    .execute_operation(op, self.session)
                    .await
                    .and_then(|dbs| {
                        dbs.into_iter()
                            .map(|db_spec| {
                                bson::from_slice(db_spec.as_bytes()).map_err(crate::error::Error::from)
                            })
                            .collect()
                    })
        }
    }
}

action_impl! {
    impl<'a> Action for ListDatabases<'a, ListNames> {
        type Future = ListDatabaseNamesFuture;

        async fn execute(self) -> Result<Vec<String>> {
            let op = op::ListDatabases::new(true, self.options);
            match self.client.execute_operation(op, self.session).await {
                Ok(databases) => databases
                    .into_iter()
                    .map(|doc| {
                        let name = doc
                            .get_str("name")
                            .map_err(|_| ErrorKind::InvalidResponse {
                                message: "Expected \"name\" field in server response, but it was \
                                            not found"
                                    .to_string(),
                            })?;
                        Ok(name.to_string())
                    })
                    .collect(),
                Err(e) => Err(e),
            }
        }
    }
}
