pub use crate::{client::auth, client::options::*, coll::options::*, db::options::*};
pub use crate::{client::options::*, coll::options::*, db::options::*};

macro_rules! resolve_option {
    ($obj:expr, $opts:expr, $field:ident) => {
        if let Some(option) = $obj.$field() {
            if !$opts
                .as_ref()
                .map(|opts| opts.$field.is_some())
                .unwrap_or(false)
            {
                $opts.get_or_insert_with(Default::default).$field = Some(option.clone());
            }
        }
    };
}

macro_rules! resolve_write_concern {
    ($obj:expr, $opts:expr) => {
        resolve_option!($obj, $opts, write_concern)
    };
}
