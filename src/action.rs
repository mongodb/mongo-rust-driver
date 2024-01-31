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
        $($opt_name:ident: $opt_ty:ty,)+
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
macro_rules! action_impl {
    // Generate with no sync type conversion
    (
        impl Action$(<$lt:lifetime>)? for $action:ty {
            type Future = $f_ty:ident;
            async fn execute($($args:ident)+) -> $out:ty $code:block
        }
    ) => {
        crate::action::action_impl_inner! {
            $action => $f_ty;
            $($lt)?;
            async fn($($args)+) -> $out $code
        }

        #[cfg(any(feature = "sync", feature = "tokio-sync"))]
        impl$(<$lt>)? $action {
            /// Synchronously execute this action.
            pub fn run(self) -> $out {
                crate::runtime::block_on(std::future::IntoFuture::into_future(self))
            }
        }
    };
    // Generate with a sync type conversion
    (
        impl Action$(<$lt:lifetime>)? for $action:ty {
            type Future = $f_ty:ident;
            async fn execute($($args:ident)+) -> $out:ty $code:block
            fn sync_wrap($($wrap_args:ident)+) -> $sync_out:ty $wrap_code:block
        }
    ) => {
        crate::action::action_impl_inner! {
            $action => $f_ty;
            $($lt)?;
            async fn($($args)+) -> $out $code
        }

        #[cfg(any(feature = "sync", feature = "tokio-sync"))]
        impl$(<$lt>)? $action {
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
        $($lt:lifetime)?;
        async fn($($args:ident)+) -> $out:ty $code:block
    ) => {
        impl$(<$lt>)? std::future::IntoFuture for $action {
            type Output = $out;
            type IntoFuture = $f_ty$(<$lt>)?;

            fn into_future($($args)+) -> Self::IntoFuture {
                $f_ty(Box::pin(async move {
                    $code
                }))
            }
        }

        crate::action::action_impl_future_wrapper!($($lt)?, $f_ty, $out);

        impl$(<$lt>)? std::future::Future for $f_ty$(<$lt>)? {
            type Output = $out;

            fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                self.0.as_mut().poll(cx)
            }
        }
    }
}
pub(crate) use action_impl_inner;

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
