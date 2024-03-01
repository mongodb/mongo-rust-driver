extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, spanned::Spanned, Error, ImplItem, ImplItemType, ItemImpl};

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
    let future_ty = find_item(&input.items, "Future type", |item| {
        let item_ty = impl_item_type(item)?;
        if item_ty.ident.to_string() != "Future" {
            return None;
        }
        Some(&item_ty.ty)
    })?;

    Ok(quote! {
        impl #generics std::future::IntoFuture for #action {
            type IntoFuture = #future_ty;
        }
    })
}

/// Finds the single item that matches the predicate.
fn find_item<'a, T>(items: &'a [ImplItem], name: &str, pred: impl Fn(&'a ImplItem) -> Option<T>) -> Result<T, Error> {
    let mut found = None;
    for item in items {
        if let Some(v) = pred(item) {
            if found.is_none() {
                found = Some(v);
            } else {
                return Err(Error::new(item.span(), format!("Duplicate {} found", name)));
            }
        }
    }
    match found {
        Some(v) => Ok(v),
        None => Err(Error::new(Span::call_site(), format!("No {} found", name))),
    }
}

fn impl_item_type(item: &ImplItem) -> Option<&ImplItemType> {
    match item {
        ImplItem::Type(t) => Some(t),
        _ => None,
    }
}