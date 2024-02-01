use std::marker::PhantomData;

use bson::{Bson, Document};
use futures_util::TryStreamExt;

use crate::{
    db::options::ListCollectionsOptions,
    error::{Error, ErrorKind, Result},
    operation::list_collections as op,
    results::CollectionSpecification,
    ClientSession,
    Cursor,
    Database,
    SessionCursor,
};

use super::{
    action_impl,
    option_setters,
    ExplicitSession,
    ImplicitSession,
    ListNames,
    ListSpecifications,
};

impl Database {
    /// Gets information about each of the collections in the database.
    ///
    /// `await` will return `Result<`[`Cursor`]`<`[`CollectionSpecification`]`>>`.
    pub fn list_collections(&self) -> ListCollections {
        ListCollections {
            db: self,
            options: None,
            mode: PhantomData,
            session: ImplicitSession,
        }
    }

    /// Gets the names of the collections in the database.
    ///
    /// `await` will return `Result<Vec<String>>`.
    pub fn list_collection_names(&self) -> ListCollections<'_, ListNames> {
        ListCollections {
            db: self,
            options: None,
            mode: PhantomData,
            session: ImplicitSession,
        }
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl crate::sync::Database {
    /// Gets information about each of the collections in the database.
    ///
    /// [`run`](ListCollections::run) will return
    /// `Result<`[`Cursor`]`<`[`CollectionSpecification`]`>>`.
    pub fn list_collections(&self) -> ListCollections {
        self.async_database.list_collections()
    }

    /// Gets the names of the collections in the database.
    ///
    /// [`run`](ListCollections::run) will return `Result<Vec<String>>`.
    pub fn list_collection_names(&self) -> ListCollections<'_, ListNames> {
        self.async_database.list_collection_names()
    }
}

/// Gets information about each of the collections in the database.  Create by
/// calling [`Database::list_collections`] or [`Database::list_collection_names`] and execute
/// with `await` (or [`run`](ListCollections::run) if using the sync client).
#[must_use]
pub struct ListCollections<'a, M = ListSpecifications, S = ImplicitSession> {
    db: &'a Database,
    options: Option<ListCollectionsOptions>,
    mode: PhantomData<M>,
    session: S,
}

impl<'a, M> ListCollections<'a, M> {
    option_setters!(options: ListCollectionsOptions;
        filter: Document,
        batch_size: u32,
        comment: Bson,
    );
}

impl<'a, M> ListCollections<'a, M, ImplicitSession> {
    /// Runs the query using the provided session.
    pub fn session<'s>(
        self,
        value: impl Into<&'s mut ClientSession>,
    ) -> ListCollections<'a, M, ExplicitSession<'s>> {
        ListCollections {
            db: self.db,
            options: self.options,
            mode: PhantomData,
            session: ExplicitSession(value.into()),
        }
    }
}

action_impl! {
    impl Action<'a> for ListCollections<'a, ListSpecifications, ImplicitSession> {
        type Future = ListCollectionsFuture;

        async fn execute(self) -> Result<Cursor<CollectionSpecification>> {
            let list_collections = op::ListCollections::new(
                self.db.name().to_string(),
                false,
                self.options,
            );
            self.db.client()
                .execute_cursor_operation(list_collections)
                .await
        }

        fn sync_wrap(out) -> Result<crate::sync::Cursor<CollectionSpecification>> {
            out.map(crate::sync::Cursor::new)
        }
    }
}

action_impl! {
    impl Action<'a> for ListCollections<'a, ListSpecifications, ExplicitSession<'a>> {
        type Future = ListCollectionsSessionFuture;

        async fn execute(self) -> Result<SessionCursor<CollectionSpecification>> {
            let list_collections = op::ListCollections::new(
                self.db.name().to_string(),
                false,
                self.options,
            );
            self.db.client()
                .execute_session_cursor_operation(list_collections, self.session.0)
                .await
        }

        fn sync_wrap(out) -> Result<crate::sync::SessionCursor<CollectionSpecification>> {
            out.map(crate::sync::SessionCursor::new)
        }
    }
}

async fn list_collection_names_common(
    cursor: impl TryStreamExt<Ok = Document, Error = Error>,
) -> Result<Vec<String>> {
    cursor
        .and_then(|doc| match doc.get("name").and_then(Bson::as_str) {
            Some(name) => futures_util::future::ok(name.into()),
            None => futures_util::future::err(
                ErrorKind::InvalidResponse {
                    message: "Expected name field in server response, but there was none."
                        .to_string(),
                }
                .into(),
            ),
        })
        .try_collect()
        .await
}

action_impl! {
    impl Action<'a> for ListCollections<'a, ListNames, ImplicitSession> {
        type Future = ListCollectionNamesFuture;

        async fn execute(self) -> Result<Vec<String>> {
            let list_collections = op::ListCollections::new(
                self.db.name().to_string(),
                true,
                self.options,
            );
            let cursor: Cursor<Document> = self.db.client()
                .execute_cursor_operation(list_collections)
                .await?;
            return list_collection_names_common(cursor).await;
        }
    }
}

action_impl! {
    impl Action<'a> for ListCollections<'a, ListNames, ExplicitSession<'a>> {
        type Future = ListCollectionNamesSessionFuture;

        async fn execute(self) -> Result<Vec<String>> {
            let list_collections = op::ListCollections::new(
                self.db.name().to_string(),
                true,
                self.options,
            );
            let mut cursor: SessionCursor<Document> = self
                .db.client()
                .execute_session_cursor_operation(list_collections, &mut *self.session.0)
                .await?;

            list_collection_names_common(cursor.stream(self.session.0))
                .await
        }
    }
}
