use proc_macro::TokenStream;
use proc_macro2::TokenTree;
use quote::{quote, ToTokens};
use syn::{parse_macro_input, spanned::Spanned, Data, DeriveInput, Fields, Meta};

#[proc_macro_derive(Project)]
pub fn mongo_projection_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let projection_methods = if let Data::Struct(data) = input.data {
        match data.fields {
            Fields::Named(fields) => {
                let methods = fields.named.iter().map(|f| {
                    let field_name = f.ident.as_ref().unwrap();
                    let method_name = syn::Ident::new(&field_name.to_string(), f.ident.span());
                    let mut renaming = None;
                    'outer: for attr in f.attrs.iter() {
                        if let Meta::List(list) = &attr.meta {
                            if let Some(ident) = list.path.get_ident() {
                                if ident == "serde" {
                                    let mut search_value = false;
                                    for token in list.tokens.to_token_stream() {
                                        if search_value {
                                            if let TokenTree::Literal(lit) = token {
                                                renaming = Some(
                                                    lit.to_string()
                                                        .strip_prefix('"')
                                                        .and_then(|s| s.strip_suffix('"'))
                                                        .unwrap()
                                                        .to_string()
                                                );
                                                break 'outer;
                                            }
                                        } else if let TokenTree::Ident(ident) = token {
                                            if ident == "rename" {
                                                search_value = true;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    let target = if let Some(renaming) = renaming {
                        renaming
                    } else {
                        field_name.to_string()
                    };
                    quote! {
                        pub fn #method_name(mut self) -> Self {
                            self.projection.insert(#target, 1);
                            self
                        }
                    }
                });
                quote! {
                    #(#methods)*
                }
            }
            _ => quote! {},
        }
    } else {
        quote! {}
    };

    let builder_name = syn::Ident::new(&format!("{}ProjectionBuilder", name), name.span());

    let expanded = quote! {
        impl #name {
            pub fn project() -> #builder_name {
                #builder_name {
                    projection: mongodb::bson::Document::new(),
                }
            }
        }

        pub struct #builder_name {
            projection: mongodb::bson::Document,
        }

        impl #builder_name {
            #projection_methods

            pub fn build(self) -> mongodb::bson::Document {
                self.projection
            }
        }
    };
    expanded.into()
}
