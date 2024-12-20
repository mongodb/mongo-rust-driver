extern crate proc_macro;

use std::collections::HashSet;

use macro_magic::mm_core::ForeignPath;
use quote::{quote, ToTokens};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input,
    parse_quote,
    spanned::Spanned,
    Attribute,
    Error,
    Fields,
    GenericArgument,
    Ident,
    ItemImpl,
    ItemStruct,
    Path,
    PathArguments,
    PathSegment,
    Token,
    Type,
    Visibility,
};

use crate::macro_error;

pub fn option_setters(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
    custom_tokens: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let opt_struct = parse_macro_input!(attr as ItemStruct);
    let mut impl_in = parse_macro_input!(item as ItemImpl);
    let args = parse_macro_input!(custom_tokens as OptionSettersArgs);

    // Gather information about each option struct field
    struct OptInfo {
        name: Ident,
        attrs: Vec<Attribute>,
        type_: Path,
    }
    let mut opt_info = vec![];
    let fields = match &opt_struct.fields {
        Fields::Named(f) => &f.named,
        _ => macro_error!(opt_struct.span(), "options struct must have named fields"),
    };
    for field in fields {
        if !matches!(field.vis, Visibility::Public(..)) {
            continue;
        }
        // name
        let name = match &field.ident {
            Some(f) => f.clone(),
            None => continue,
        };
        // doc and cfg attrs
        let mut attrs = vec![];
        for attr in &field.attrs {
            if attr.path().is_ident("doc") || attr.path().is_ident("cfg") {
                attrs.push(attr.clone());
            }
        }
        // type, unwrapped from `Option`
        let outer = match &field.ty {
            Type::Path(ty) => &ty.path,
            _ => macro_error!(field.span(), "invalid type"),
        };
        let type_ = match inner_type(outer, "Option") {
            Some(Type::Path(ty)) => ty.path.clone(),
            _ => macro_error!(field.span(), "invalid type"),
        };

        opt_info.push(OptInfo { name, attrs, type_ });
    }

    // Append utility fns to `impl` block item list
    let opt_field_type = &opt_struct.ident;
    impl_in.items.push(parse_quote! {
        #[allow(unused)]
        fn options(&mut self) -> &mut #opt_field_type {
            self.options.get_or_insert_with(<#opt_field_type>::default)
        }
    });
    impl_in.items.push(parse_quote! {
        /// Set all options.  Note that this will replace all previous values set.
        pub fn with_options(mut self, value: impl Into<Option<#opt_field_type>>) -> Self {
            self.options = value.into();
            self
        }
    });
    // Append setter fns to `impl` block item list
    for OptInfo { name, attrs, type_ } in opt_info {
        if args
            .skip
            .as_ref()
            .map_or(false, |skip| skip.contains(&name))
        {
            continue;
        }
        let (accept, value) = if type_.is_ident("String")
            || type_.is_ident("Bson")
            || path_eq(&type_, &["bson", "Bson"])
        {
            (quote! { impl Into<#type_> }, quote! { value.into() })
        } else if let Some(t) = inner_type(&type_, "Vec") {
            (
                quote! { impl IntoIterator<Item = #t> },
                quote! { value.into_iter().collect() },
            )
        } else {
            (quote! { #type_ }, quote! { value })
        };
        impl_in.items.push(parse_quote! {
            #(#attrs)*
            pub fn #name(mut self, value: #accept) -> Self {
                self.options().#name = Some(#value);
                self
            }
        });
    }

    // All done.
    impl_in.to_token_stream().into()
}

pub(crate) struct OptionSettersArgs {
    tokens: proc_macro2::TokenStream,
    foreign_path: syn::Path,      // <path>
    skip: Option<HashSet<Ident>>, // skip = [ident, ..]
}

impl Parse for OptionSettersArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let tokens: proc_macro2::TokenStream = input.fork().parse()?;

        let foreign_path = input.parse()?;
        let mut out = Self {
            tokens,
            foreign_path,
            skip: None,
        };
        if input.parse::<Option<Token![,]>>()?.is_none() || input.is_empty() {
            return Ok(out);
        }

        out.skip = Some(
            crate::parse_ident_list(input, "skip")?
                .into_iter()
                .collect(),
        );
        input.parse::<Option<Token![,]>>()?;

        Ok(out)
    }
}

impl ToTokens for OptionSettersArgs {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        tokens.extend(self.tokens.clone());
    }
}

impl ForeignPath for OptionSettersArgs {
    fn foreign_path(&self) -> &syn::Path {
        &self.foreign_path
    }
}

fn inner_type<'a>(path: &'a Path, outer: &str) -> Option<&'a Type> {
    if path.segments.len() != 1 {
        return None;
    }
    let PathSegment { ident, arguments } = path.segments.first()?;
    if ident != outer {
        return None;
    }
    let args = if let PathArguments::AngleBracketed(angle) = arguments {
        &angle.args
    } else {
        return None;
    };
    if args.len() != 1 {
        return None;
    }
    if let GenericArgument::Type(t) = args.first()? {
        return Some(t);
    }

    None
}

fn path_eq(path: &Path, segments: &[&str]) -> bool {
    if path.segments.len() != segments.len() {
        return false;
    }
    for (actual, expected) in path.segments.iter().zip(segments.into_iter()) {
        if actual.ident != expected {
            return false;
        }
        if !actual.arguments.is_empty() {
            return false;
        }
    }
    true
}
