extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, spanned::Spanned, Error, ItemImpl};

#[proc_macro]
pub fn action_impl_2(input: TokenStream) -> TokenStream {
    let impl_ = parse_macro_input!(input as ItemImpl);
    fallible(impl_).unwrap_or_else(Error::into_compile_error).into()
}

fn fallible(input: ItemImpl) -> Result<proc_macro2::TokenStream, Error> {
    // Validate that it's `impl Action for ...`
    match input.trait_ {
        None => return Err(Error::new(input.span(), "A trait must be implemented")),
        Some((not, path, _)) => {
            if let Some(not) = not {
                return Err(Error::new(not.span(), "Must not be a negative impl"));
            }
            if !path.is_ident("Action") {
                return Err(Error::new(path.span(), "Trait must be `Action`"));
            }
        }
    }

    let generics = input.generics;
    let action = input.self_ty;

    Ok(quote! {
        impl #generics std::future::IntoFuture for #action {
            
        }
    })
}