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
