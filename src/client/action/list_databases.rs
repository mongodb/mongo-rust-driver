use std::{future::IntoFuture, marker::PhantomData};

use bson::{Document, Bson};
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::{Client, ClientSession, operation::ListDatabases, error::{Result, ErrorKind}, results::DatabaseSpecification, client::BoxFuture};

impl Client {
    /// Gets information about each database present in the cluster the Client is connected to.
    pub fn list_databases(
        &self,
    ) -> Action {
        Action {
            client: &self,
            options: Default::default(),
            session: None,
            mode: PhantomData,
        }
    }

    /// Gets the names of the databases present in the cluster the Client is connected to.
    pub fn list_database_names(
        &self,
    ) -> Action<'_, Names> {
        Action {
            client: &self,
            options: Default::default(),
            session: None,
            mode: PhantomData,
        }
    }
}

/// Gets information about each database present in the cluster the Client is connected to.
#[must_use]
pub struct Action<'a, M: Mode = Full> {
    client: &'a Client,
    options: Option<Options>,
    session: Option<&'a mut ClientSession>,
    mode: PhantomData<M>,
}

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Options {
    pub(crate) filter: Option<Document>,
    pub(crate) authorized_databases: Option<bool>,
    pub(crate) comment: Option<Bson>,
}

mod private {
    pub trait Sealed {}
    impl Sealed for super::Full {}
    impl Sealed for super::Names {}    
}

pub trait Mode: private::Sealed {}
impl Mode for Full {}
impl Mode for Names {}

pub struct Full;
pub struct Names;

impl<'a, M: Mode> Action<'a, M> {
    fn options(&mut self) -> &mut Options {
        self.options.get_or_insert_with(Options::default)
    }

    #[cfg(test)]
    pub(crate) fn with_options(mut self, value: impl Into<Option<Options>>) -> Self {
        self.options = value.into();
        self
    }

    /// Filters the query.
    pub fn filter(mut self, value: impl Into<Option<Document>>) -> Self {
        self.options().filter = value.into();
        self
    }

    /// Determines which databases to return based on the user's access privileges. This option is
    /// only supported on server versions 4.0.5+.
    pub fn authorized_databases(mut self, value: impl Into<Option<bool>>) -> Self {
        self.options().authorized_databases = value.into();
        self
    }

    /// Tags the query with an arbitrary [`Bson`] value to help trace the operation through the
    /// database profiler, currentOp and logs.
    ///
    /// This option is only available on server versions 4.4+.
    pub fn comment(mut self, value: impl Into<Option<Bson>>) -> Self {
        self.options().comment = value.into();
        self
    }

    /// Runs the query using the provided session.
    pub fn session(mut self, value: impl Into<Option<&'a mut ClientSession>>) -> Self {
        self.session = value.into();
        self
    }
}

impl<'a> IntoFuture for Action<'a, Full> {
    type Output = Result<Vec<DatabaseSpecification>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async {
            let op = ListDatabases::new(false, self.options);
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

impl<'a> IntoFuture for Action<'a, Names> {
    type Output = Result<Vec<String>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async {
            let op = ListDatabases::new(true, self.options);
            match self.client.execute_operation(op, None).await {
                Ok(databases) => databases
                    .into_iter()
                    .map(|doc| {
                        let name = doc
                            .get_str("name")
                            .map_err(|_| ErrorKind::InvalidResponse {
                                message: "Expected \"name\" field in server response, but it was not \
                                        found"
                                    .to_string(),
                            })?;
                        Ok(name.to_string())
                    })
                    .collect(),
                Err(e) => Err(e),
            }
        }.boxed()
    }
}