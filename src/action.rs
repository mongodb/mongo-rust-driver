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
pub use watch::{Watch, WatchImplicitSessionFuture, WatchExplicitSessionFuture};

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

macro_rules! action_execute_inner {
    (
        $action:ty => $f_ty:ident;
        $into_future:item;
        $out:ty
    ) => {
        impl<'a> std::future::IntoFuture for $action {
            type Output = $out;
            type IntoFuture = $f_ty<'a>;

            $into_future
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
pub(crate) use action_execute_inner;

macro_rules! action_execute {
    (
        $action:ty => $f_ty:ident;
        async fn($self:ident) -> $out:ty $code:block
    ) => {
        $crate::action::action_execute_inner!(
            $action => $f_ty;
            fn into_future($self) -> Self::IntoFuture {
                $f_ty(Box::pin(async move {
                    $code
                }))
            };
            $out
        );
    };
    (
        $action:ty => $f_ty:ident;
        async fn(mut $self:ident) -> $out:ty $code:block
    ) => {
        $crate::action::action_execute_inner!(
            $action => $f_ty;
            fn into_future(mut $self) -> Self::IntoFuture {
                $f_ty(Box::pin(async move {
                    $code
                }))
            };
            $out
        );
    }
}
pub(crate) use action_execute;
