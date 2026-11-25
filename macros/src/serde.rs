pub(crate) mod internal;
pub(crate) mod test;

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::{
    parse_quote,
    punctuated::Punctuated,
    Attribute,
    Data,
    DeriveInput,
    Expr,
    ExprLit,
    Fields,
    Generics,
    Ident,
    Lifetime,
    Lit,
    LitStr,
    Meta,
    Token,
};

fn into_output(result: syn::Result<TokenStream>) -> proc_macro::TokenStream {
    match result {
        Ok(tokens) => tokens.into(),
        Err(e) => e.into_compile_error().into(),
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

/// The pieces needed to implement a serde trait for a type via a private mirror of it.
struct MirrorImpl {
    /// The mirror definition, deriving the corresponding serde trait, plus any supporting items.
    items: TokenStream,
    /// The original type's generics, extended with whatever the impl needs.
    generics: Generics,
    /// The body of the impl's method.
    body: TokenStream,
}

/// Implements serialization via a mirror that borrows each field of the original value.
fn serialize_mirror(input: &DeriveInput) -> syn::Result<MirrorImpl> {
    let name = &input.ident;
    let mirror_name = Ident::new("__SerializeMirror", Span::call_site());
    let lifetime = Lifetime::new("'__mirror", Span::call_site());

    let has_fields = match &input.data {
        Data::Struct(data) => !data.fields.is_empty(),
        Data::Enum(data) => data.variants.iter().any(|v| !v.fields.is_empty()),
        Data::Union(_) => false,
    };
    let mut mirror = mirror(input, &mirror_name, has_fields.then_some(&lifetime))?;

    let mut generics = input.generics.clone();
    if has_fields {
        mirror.generics.params.insert(0, parse_quote!(#lifetime));
    }
    let (_, mirror_ty_generics, _) = mirror.generics.split_for_impl();
    let bound = if has_fields {
        parse_quote!(for<#lifetime> #mirror_name #mirror_ty_generics: ::serde::Serialize)
    } else {
        parse_quote!(#mirror_name #mirror_ty_generics: ::serde::Serialize)
    };
    generics.make_where_clause().predicates.push(bound);

    let convert = convert(&input.data, &quote!(#name), &quote!(#mirror_name));

    Ok(MirrorImpl {
        items: quote! {
            #[derive(::serde::Serialize)]
            #mirror
        },
        generics,
        body: quote! {
            let __value = self;
            let __mirror = #convert;
            ::serde::Serialize::serialize(&__mirror, serializer)
        },
    })
}

/// Implements deserialization via an owned mirror of the original type.
fn deserialize_mirror(input: &DeriveInput) -> syn::Result<MirrorImpl> {
    let name = &input.ident;
    let mirror_name = Ident::new("__DeserializeMirror", Span::call_site());
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let mut mirror = mirror(input, &mirror_name, None)?;
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

    let mut generics = input.generics.clone();
    generics.params.insert(0, parse_quote!('de));
    generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(#mirror_name #ty_generics: ::serde::Deserialize<'de>));

    Ok(MirrorImpl {
        items: quote! {
            #[derive(::serde::Deserialize)]
            #mirror

            #default_impl
        },
        generics,
        body: quote! {
            let __value = <#mirror_name #ty_generics as ::serde::Deserialize<'de>>::deserialize(
                deserializer,
            )?;
            ::std::result::Result::Ok(#convert)
        },
    })
}

/// Creates a private copy of the input type with the given name that has only `#[serde(...)]`
/// attributes, suitable for passing to the serde derives. If a lifetime is given, each field's type
/// is replaced with a reference to it with that lifetime.
fn mirror(
    input: &DeriveInput,
    mirror_name: &Ident,
    borrow: Option<&Lifetime>,
) -> syn::Result<DeriveInput> {
    let mut mirror = input.clone();
    mirror.ident = mirror_name.clone();
    mirror.vis = syn::Visibility::Inherited;
    retain_serde_attrs(&mut mirror.attrs);
    match &mut mirror.data {
        Data::Struct(data) => strip_fields(&mut data.fields, borrow),
        Data::Enum(data) => {
            for variant in &mut data.variants {
                retain_serde_attrs(&mut variant.attrs);
                strip_fields(&mut variant.fields, borrow);
            }
        }
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(input, "unions are not supported"));
        }
    }
    Ok(mirror)
}

fn strip_fields(fields: &mut Fields, borrow: Option<&Lifetime>) {
    for field in fields.iter_mut() {
        field.vis = syn::Visibility::Inherited;
        retain_serde_attrs(&mut field.attrs);
        if let Some(lifetime) = borrow {
            let ty = &field.ty;
            field.ty = parse_quote!(&#lifetime #ty);
        }
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
