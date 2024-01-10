use std::{future::IntoFuture, marker::PhantomData};

use bson::{Bson, Document};
use futures_util::FutureExt;

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
use crate::sync::Client as SyncClient;
use crate::{
    client::BoxFuture,
    error::{ErrorKind, Result},
    operation::list_databases as op,
    results::DatabaseSpecification,
    Client,
    ClientSession,
};

use super::option_setters;

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

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
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
/// by calling [`Client::list_databases`] or [`Client::list_database_names`] and execute with
/// `await` (or [run](ListDatabases::run) if using the sync client).
#[must_use]
pub struct ListDatabases<'a, M = ListSpecifications> {
    client: &'a Client,
    options: Option<op::Options>,
    session: Option<&'a mut ClientSession>,
    mode: PhantomData<M>,
}

pub struct ListSpecifications;
pub struct ListNames;

impl<'a, M> ListDatabases<'a, M> {
    option_setters!(options: op::Options;
        /// Filters the query.
        filter: Document,

        /// Determines which databases to return based on the user's access privileges. This option is
        /// only supported on server versions 4.0.5+.
        authorized_databases: bool,

        /// Tags the query with an arbitrary [`Bson`] value to help trace the operation through the
        /// database profiler, currentOp and logs.
        ///
        /// This option is only available on server versions 4.4+.
        comment: Bson,
    );

    /// Runs the query using the provided session.
    pub fn session(mut self, value: &'a mut ClientSession) -> Self {
        self.session = Some(value);
        self
    }
}

impl<'a> IntoFuture for ListDatabases<'a, ListSpecifications> {
    type Output = Result<Vec<DatabaseSpecification>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async {
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
        .boxed()
    }
}

impl<'a> IntoFuture for ListDatabases<'a, ListNames> {
    type Output = Result<Vec<String>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async {
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
        .boxed()
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl<'a, M> ListDatabases<'a, M>
where
    Self: IntoFuture,
{
    /// Synchronously execute this action.
    pub fn run(self) -> <Self as IntoFuture>::Output {
        crate::runtime::block_on(self.into_future())
    }
}
