pub use crate::{
    client::auth, client::options::*, coll::options::*, collation::*, concern::*, db::options::*,
    selection_criteria::*,
};

/// Updates an options struct with the read preference/read concern/write concern of a
/// client/database/collection.
macro_rules! resolve_options {
    ($obj:expr, $opts:expr, [$( $field:ident ),+] ) => {
        $(
            if let Some(option) = $obj.$field() {
                if !$opts
                    .as_ref()
                    .map(|opts| opts.$field.is_some())
                    .unwrap_or(false)
                {
                    $opts.get_or_insert_with(Default::default).$field = Some(option.clone());
                }
            }
        )+
    };
}
