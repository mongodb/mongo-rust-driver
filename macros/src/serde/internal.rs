use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

use super::{deserialize_mirror, into_output, serialize_mirror, MirrorImpl};

pub(crate) fn derive_serialize(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    into_output(derive_serialize_inner(input))
}

pub(crate) fn derive_deserialize(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    into_output(derive_deserialize_inner(input))
}

fn derive_serialize_inner(input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;
    let (_, ty_generics, _) = input.generics.split_for_impl();
    let MirrorImpl {
        items,
        generics,
        body,
    } = serialize_mirror(&input)?;
    let (impl_generics, _, where_clause) = generics.split_for_impl();

    Ok(quote! {
        const _: () = {
            #items

            #[automatically_derived]
            #[allow(deprecated)]
            impl #impl_generics crate::serde_util::InternalSerialize for #name #ty_generics
                #where_clause
            {
                fn serialize_internal<__S>(
                    &self,
                    serializer: __S,
                ) -> ::std::result::Result<__S::Ok, __S::Error>
                where
                    __S: ::serde::Serializer,
                {
                    #body
                }
            }
        };
    })
}

fn derive_deserialize_inner(input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;
    let (_, ty_generics, _) = input.generics.split_for_impl();
    let MirrorImpl {
        items,
        generics,
        body,
    } = deserialize_mirror(&input)?;
    let (impl_generics, _, where_clause) = generics.split_for_impl();

    Ok(quote! {
        const _: () = {
            #items

            #[automatically_derived]
            #[allow(deprecated)]
            impl #impl_generics crate::serde_util::InternalDeserialize<'de> for #name #ty_generics
                #where_clause
            {
                fn deserialize_internal<__D>(
                    deserializer: __D,
                ) -> ::std::result::Result<Self, __D::Error>
                where
                    __D: ::serde::Deserializer<'de>,
                {
                    #body
                }
            }
        };
    })
}
