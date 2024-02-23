//! Action builder types.

mod aggregate;
mod count;
mod create_collection;
mod create_index;
mod delete;
mod distinct;
mod drop;
mod drop_index;
mod list_collections;
mod list_databases;
mod list_indexes;
mod perf;
mod run_command;
mod session;
mod shutdown;
mod watch;

use std::{marker::PhantomData, ops::Deref};

pub use aggregate::Aggregate;
use bson::Document;
pub use count::{CountDocuments, EstimatedDocumentCount};
pub use create_collection::CreateCollection;
pub use create_index::CreateIndex;
pub use delete::Delete;
pub use distinct::Distinct;
pub use drop::{DropCollection, DropDatabase};
pub use drop_index::DropIndex;
pub use list_collections::ListCollections;
pub use list_databases::ListDatabases;
pub use list_indexes::ListIndexes;
pub use perf::WarmConnectionPool;
pub use run_command::{RunCommand, RunCursorCommand};
pub use session::StartSession;
pub use shutdown::Shutdown;
pub use watch::Watch;

#[allow(missing_docs)]
pub struct ListSpecifications;
#[allow(missing_docs)]
pub struct ListNames;

#[allow(missing_docs)]
pub struct ImplicitSession;
#[allow(missing_docs)]
pub struct ExplicitSession<'a>(&'a mut crate::ClientSession);

#[allow(missing_docs)]
pub struct Single;
#[allow(missing_docs)]
pub struct Multiple;

macro_rules! option_setters {
    (
        $opt_field:ident: $opt_field_ty:ty;
        $(
            $(#[$($attrss:tt)*])*
            $opt_name:ident: $opt_ty:ty,
        )+
    ) => {
        fn options(&mut self) -> &mut $opt_field_ty {
            self.$opt_field.get_or_insert_with(<$opt_field_ty>::default)
        }

        /// Set all options.  Note that this will replace all previous values set.
        pub fn with_options(mut self, value: impl Into<Option<$opt_field_ty>>) -> Self {
            self.options = value.into();
            self
        }

        $(
            #[doc = concat!("Set the [`", stringify!($opt_field_ty), "::", stringify!($opt_name), "`] option.")]
            $(#[$($attrss)*])*
            pub fn $opt_name(mut self, value: $opt_ty) -> Self {
                self.options().$opt_name = Some(value);
                self
            }
        )+
    };
}
use option_setters;

/// A pending action to execute on the server.  The action can be configured via chained methods and
/// executed via `await` (or `run` if using the sync client).
pub trait Action {
    /// The type of the value produced by execution.
    type Output;

    /// If the value is `Some`, call the provided function on `self`.  Convenient for chained
    /// updates with values that need to be set conditionally.  For example:
    /// ```rust
    /// # use mongodb::{Client, error::Result};
    /// # use bson::Document;
    /// use mongodb::action::Action;
    /// async fn list_my_collections(client: &Client, filter: Option<Document>) -> Result<Vec<String>> {
    ///     client.database("my_db")
    ///         .list_collection_names()
    ///         .optional(filter, |a, f| a.filter(f))
    ///         .await
    /// }
    /// ```
    fn optional<Value>(self, value: Option<Value>, f: impl FnOnce(Self, Value) -> Self) -> Self
    where
        Self: Sized,
    {
        match value {
            Some(value) => f(self, value),
            None => self,
        }
    }
}

/// Generates:
/// * an `IntoFuture` executing the given method body
/// * an opaque wrapper type for the future in case we want to do something more fancy than
///   BoxFuture.
/// * a `run` method for sync execution, optionally with a wrapper function
macro_rules! action_impl {
    // Generate with no sync type conversion
    (
        impl$(<$lt:lifetime $(, $($at:ident),+)?>)? Action for $action:ty {
            type Future = $f_ty:ident;
            async fn execute($($args:ident)+) -> $out:ty $code:block
        }
    ) => {
        crate::action::action_impl! {
            impl$(<$lt $(, $($at),+)?>)? Action for $action {
                type Future = $f_ty;
                async fn execute($($args)+) -> $out $code
                fn sync_wrap(out) -> $out { out }
            }
        }
    };
    // Generate with a sync type conversion
    (
        impl$(<$lt:lifetime $(, $($at:ident),+)?>)? Action for $action:ty {
            type Future = $f_ty:ident;
            async fn execute($($args:ident)+) -> $out:ty $code:block
            fn sync_wrap($($wrap_args:ident)+) -> $sync_out:ty $wrap_code:block
        }
    ) => {
        impl$(<$lt $(, $($at),+)?>)? std::future::IntoFuture for $action {
            type Output = $out;
            type IntoFuture = $f_ty$(<$lt>)?;

            fn into_future($($args)+) -> Self::IntoFuture {
                $f_ty(Box::pin(async move {
                    $code
                }))
            }
        }

        impl$(<$lt $(, $($at),+)?>)? crate::action::Action for $action {
            type Output = $out;
        }

        crate::action::action_impl_future_wrapper!($($lt)?, $f_ty, $out);

        impl$(<$lt>)? std::future::Future for $f_ty$(<$lt>)? {
            type Output = $out;

            fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                self.0.as_mut().poll(cx)
            }
        }

        #[cfg(any(feature = "sync", feature = "tokio-sync"))]
        impl$(<$lt $(, $($at),+)?>)? $action {
            /// Synchronously execute this action.
            pub fn run(self) -> $sync_out {
                let $($wrap_args)+ = crate::runtime::block_on(std::future::IntoFuture::into_future(self));
                return $wrap_code
            }
        }
    }
}
pub(crate) use action_impl;

macro_rules! action_impl_future_wrapper {
    (, $f_ty:ident, $out:ty) => {
        /// Opaque future type for action execution.
        pub struct $f_ty(crate::BoxFuture<'static, $out>);
    };
    ($lt:lifetime, $f_ty:ident, $out:ty) => {
        /// Opaque future type for action execution.
        pub struct $f_ty<$lt>(crate::BoxFuture<$lt, $out>);
    };
}
pub(crate) use action_impl_future_wrapper;

use crate::Collection;

pub(crate) struct CollRef<'a> {
    inner: Collection<Document>,
    _ref: PhantomData<&'a ()>,
}

impl<'a> CollRef<'a> {
    fn new<T>(coll: &'a Collection<T>) -> Self {
        Self {
            inner: coll.clone_with_type(),
            _ref: PhantomData,
        }
    }
}

impl<'a> Deref for CollRef<'a> {
    type Target = Collection<Document>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
