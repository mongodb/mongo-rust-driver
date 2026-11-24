use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::{
    parse_macro_input,
    parse_quote,
    punctuated::Punctuated,
    Attribute,
    Data,
    DeriveInput,
    Expr,
    ExprLit,
    Fields,
    Ident,
    Lit,
    LitStr,
    Meta,
    Token,
};

pub(crate) fn derive_deserialize(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    into_output(derive_deserialize_inner(input))
}

pub(crate) fn derive_serialize(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    into_output(derive_serialize_inner(input))
}

fn into_output(result: syn::Result<TokenStream>) -> proc_macro::TokenStream {
    match result {
        Ok(tokens) => tokens.into(),
        Err(e) => e.into_compile_error().into(),
    }
}

fn derive_deserialize_inner(input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;
    let mirror_name = Ident::new("__DeserializeMirror", Span::call_site());
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let mut mirror = mirror(&input, &mirror_name)?;

    let default_fn = Ident::new("__mirror_default", Span::call_site());
    let default_fn_str = LitStr::new(&default_fn.to_string(), Span::call_site());
    let default_impl = match find_container_default(&mut mirror.attrs, &default_fn_str)? {
        None => quote!(),
        Some(ContainerDefault::Trait) => {
            let convert = convert(&input.data, &quote!(#name), &quote!(Self));
            let mut generics = input.generics.clone();
            generics
                .make_where_clause()
                .predicates
                .push(parse_quote!(#name #ty_generics: ::std::default::Default));
            let (_, _, default_where_clause) = generics.split_for_impl();
            quote! {
                impl #impl_generics ::std::default::Default for #mirror_name #ty_generics
                    #default_where_clause
                {
                    fn default() -> Self {
                        let __value: #name #ty_generics = ::std::default::Default::default();
                        #convert
                    }
                }
            }
        }
        Some(ContainerDefault::Path(path)) => {
            let convert = convert(&input.data, &quote!(#name), &quote!(#mirror_name));
            quote! {
                fn #default_fn #impl_generics () -> #mirror_name #ty_generics #where_clause {
                    let __value: #name #ty_generics = #path();
                    #convert
                }
            }
        }
    };

    let convert = convert(&input.data, &quote!(#mirror_name), &quote!(Self));

    let mut de_generics = input.generics.clone();
    de_generics.params.insert(0, parse_quote!('de));
    de_generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(#mirror_name #ty_generics: ::serde::Deserialize<'de>));
    let (de_impl_generics, _, de_where_clause) = de_generics.split_for_impl();

    Ok(quote! {
        #[cfg(test)]
        const _: () = {
            #[derive(::serde::Deserialize)]
            #mirror

            #default_impl

            #[automatically_derived]
            impl #de_impl_generics ::serde::Deserialize<'de> for #name #ty_generics #de_where_clause {
                fn deserialize<__D>(deserializer: __D) -> ::std::result::Result<Self, __D::Error>
                where
                    __D: ::serde::Deserializer<'de>,
                {
                    let __value = <#mirror_name #ty_generics as ::serde::Deserialize<'de>>::deserialize(
                        deserializer,
                    )?;
                    ::std::result::Result::Ok(#convert)
                }
            }
        };
    })
}

fn derive_serialize_inner(input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;
    let mirror_name = Ident::new("__SerializeMirror", Span::call_site());
    let (impl_generics, ty_generics, _) = input.generics.split_for_impl();

    let mut mirror = mirror(&input, &mirror_name)?;
    let remote = LitStr::new(&name.to_string(), Span::call_site());
    mirror.attrs.push(parse_quote!(#[serde(remote = #remote)]));

    let mut ser_generics = input.generics.clone();
    let type_params: Vec<_> = ser_generics
        .type_params()
        .map(|param| param.ident.clone())
        .collect();
    let ser_where_clause = ser_generics.make_where_clause();
    for param in type_params {
        ser_where_clause
            .predicates
            .push(parse_quote!(#param: ::serde::Serialize));
    }
    let (_, _, ser_where_clause) = ser_generics.split_for_impl();

    Ok(quote! {
        #[cfg(test)]
        const _: () = {
            #[derive(::serde::Serialize)]
            #mirror

            #[automatically_derived]
            impl #impl_generics ::serde::Serialize for #name #ty_generics #ser_where_clause {
                fn serialize<__S>(&self, serializer: __S) -> ::std::result::Result<__S::Ok, __S::Error>
                where
                    __S: ::serde::Serializer,
                {
                    #mirror_name::serialize(self, serializer)
                }
            }
        };
    })
}

/// Creates a private copy of the input type with the given name that has only `#[serde(...)]`
/// attributes, suitable for passing to the serde derives.
fn mirror(input: &DeriveInput, mirror_name: &Ident) -> syn::Result<DeriveInput> {
    let mut mirror = input.clone();
    mirror.ident = mirror_name.clone();
    mirror.vis = syn::Visibility::Inherited;
    retain_serde_attrs(&mut mirror.attrs);
    match &mut mirror.data {
        Data::Struct(data) => strip_fields(&mut data.fields),
        Data::Enum(data) => {
            for variant in &mut data.variants {
                retain_serde_attrs(&mut variant.attrs);
                strip_fields(&mut variant.fields);
            }
        }
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(input, "unions are not supported"));
        }
    }
    Ok(mirror)
}

fn strip_fields(fields: &mut Fields) {
    for field in fields.iter_mut() {
        field.vis = syn::Visibility::Inherited;
        retain_serde_attrs(&mut field.attrs);
    }
}

fn retain_serde_attrs(attrs: &mut Vec<Attribute>) {
    attrs.retain(|attr| attr.path().is_ident("serde"));
}

/// Generate an expression that converts between the two types. The types must have the same shape.
fn convert(data: &Data, from: &TokenStream, to: &TokenStream) -> TokenStream {
    let arms: Vec<_> = match data {
        Data::Struct(data) => {
            let bindings = bindings(&data.fields);
            vec![quote!(#from #bindings => #to #bindings)]
        }
        Data::Enum(data) => data
            .variants
            .iter()
            .map(|variant| {
                let ident = &variant.ident;
                let bindings = bindings(&variant.fields);
                quote!(#from::#ident #bindings => #to::#ident #bindings)
            })
            .collect(),
        Data::Union(_) => unreachable!("unions are rejected before conversion"),
    };
    quote! {
        match __value {
            #(#arms,)*
        }
    }
}

/// Generates tokens that are valid both as a pattern binding every field and as an expression
/// constructing a value from those bindings.
fn bindings(fields: &Fields) -> TokenStream {
    match fields {
        Fields::Named(named) => {
            let fields = named.named.iter().enumerate().map(|(i, field)| {
                let ident = &field.ident;
                let binding = format_ident!("__field{}", i);
                quote!(#ident: #binding)
            });
            quote!({ #(#fields),* })
        }
        Fields::Unnamed(unnamed) => {
            let bindings = (0..unnamed.unnamed.len()).map(|i| format_ident!("__field{}", i));
            quote!((#(#bindings),*))
        }
        Fields::Unit => quote!(),
    }
}

enum ContainerDefault {
    /// `#[serde(default)]`
    Trait,
    /// `#[serde(default = "path")]`
    Path(syn::ExprPath),
}

/// Finds how default is specified for serde if it is specified. if #[serde(default = "path")] is
/// specified, replaces the path with the provided replacement.
fn find_container_default(
    attrs: &mut [Attribute],
    replacement: &LitStr,
) -> syn::Result<Option<ContainerDefault>> {
    let mut default = None;
    for attr in attrs {
        let mut metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        let mut changed = false;
        for meta in metas.iter_mut() {
            match meta {
                Meta::Path(p) if p.is_ident("default") => default = Some(ContainerDefault::Trait),
                Meta::NameValue(nv) if nv.path.is_ident("default") => {
                    let Expr::Lit(ExprLit {
                        lit: Lit::Str(path),
                        ..
                    }) = &nv.value
                    else {
                        return Err(syn::Error::new_spanned(
                            &nv.value,
                            "expected a string literal",
                        ));
                    };
                    default = Some(ContainerDefault::Path(path.parse()?));
                    nv.value = parse_quote!(#replacement);
                    changed = true;
                }
                _ => {}
            }
        }
        if changed {
            *attr = parse_quote!(#[serde(#metas)]);
        }
    }
    Ok(default)
}
