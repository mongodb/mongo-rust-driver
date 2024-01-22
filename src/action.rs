//! Action builder types.

mod drop;
mod list_databases;
mod perf;
mod session;
mod shutdown;
mod watch;

pub use list_databases::ListDatabases;
pub use perf::WarmConnectionPool;
pub use session::StartSession;
pub use shutdown::Shutdown;
pub use watch::Watch;

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

macro_rules! action_future {
    ($act_name:ident ( $self:ident ) -> $f_ty:ident ( $out:ty ) $code:block) => {
        impl<'a> std::future::IntoFuture for $act_name<'a> {
            type Output = $out;
            type IntoFuture = $f_ty<'a>;

            fn into_future(mut $self) -> Self::IntoFuture {
                $f_ty(async move {
                    $code
                }.boxed())
            }
        }

        pub struct $f_ty<'a>(crate::BoxFuture<'a, $out>);

        impl<'a> std::future::Future for $f_ty<'a> {
            type Output = $out;

            fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                self.0.as_mut().poll(cx)
            }
        }
    };
    ($act_name:ident ( $self:ident ) -> $out:ty $code:block) => {
        paste::paste! {
            action_future!($act_name($self) -> [<$act_name Future>]($out) $code);
        }
    };
}
use action_future;
