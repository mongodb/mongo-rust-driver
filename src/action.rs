//! Action builder types.

mod drop;
mod list_databases;
mod perf;
mod session;
mod shutdown;
mod watch;

pub use drop::{DropDatabase, DropDatabaseFuture};
pub use list_databases::{ListDatabases, ListSpecificationsFuture};
pub use perf::{WarmConnectionPool, WarmConnectionPoolFuture};
pub use session::{StartSession, StartSessionFuture};
pub use shutdown::{Shutdown, ShutdownFuture};
pub use watch::{Watch, WatchExplicitSessionFuture, WatchImplicitSessionFuture};

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

        #[cfg(test)]
        #[allow(dead_code)]
        pub(crate) fn with_options(mut self, value: impl Into<Option<$opt_field_ty>>) -> Self {
            self.options = value.into();
            self
        }

        $(
            $(#[$($attrss)*])*
            pub fn $opt_name(mut self, value: $opt_ty) -> Self {
                self.options().$opt_name = Some(value);
                self
            }
        )+
    };
}
use option_setters;

/// Generates:
/// * an `IntoFuture` executing the given method body
/// * an opaque wrapper type for the future in case we want to do something more fancy than
///   BoxFuture.
/// * a `run` method for sync execution, optionally with a wrapper function
///
/// The action type is assumed to have a 'a lifetype parameter.
macro_rules! action_impl {
    // Generate with no sync type conversion
    (
        type Action = $action:ty;
        type Future = $f_ty:ident;
        async fn execute($($args:ident)+) -> $out:ty $code:block
    ) => {
        crate::action::action_impl_inner! {
            $action => $f_ty;
            async fn($($args)+) -> $out $code
        }

        #[cfg(any(feature = "sync", feature = "tokio-sync"))]
        impl<'a> $action {
            /// Synchronously execute this action.
            pub fn run(self) -> $out {
                crate::runtime::block_on(std::future::IntoFuture::into_future(self))
            }
        }
    };
    // Generate with a sync type conversion
    (
        type Action = $action:ty;
        type Future = $f_ty:ident;
        async fn execute($($args:ident)+) -> $out:ty $code:block
        fn sync_wrap($($wrap_args:ident)+) -> $sync_out:ty $wrap_code:block
    ) => {
        crate::action::action_impl_inner! {
            $action => $f_ty;
            async fn($($args)+) -> $out $code
        }

        #[cfg(any(feature = "sync", feature = "tokio-sync"))]
        impl<'a> $action {
            /// Synchronously execute this action.
            pub fn run(self) -> $sync_out {
                let $($wrap_args)+ = crate::runtime::block_on(std::future::IntoFuture::into_future(self));
                return $wrap_code
            }
        }
    }
}
pub(crate) use action_impl;

macro_rules! action_impl_inner {
    (
        $action:ty => $f_ty:ident;
        async fn($($args:ident)+) -> $out:ty $code:block
    ) => {
        impl<'a> std::future::IntoFuture for $action {
            type Output = $out;
            type IntoFuture = $f_ty<'a>;

            fn into_future($($args)+) -> Self::IntoFuture {
                $f_ty(Box::pin(async move {
                    $code
                }))
            }
        }

        /// Opaque future type for action execution.
        pub struct $f_ty<'a>(crate::BoxFuture<'a, $out>);

        impl<'a> std::future::Future for $f_ty<'a> {
            type Output = $out;

            fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                self.0.as_mut().poll(cx)
            }
        }
    }
}
pub(crate) use action_impl_inner;
