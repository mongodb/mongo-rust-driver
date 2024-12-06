extern crate proc_macro;

use macro_magic::{import_tokens_attr, mm_core::ForeignPath};
use quote::{quote, ToTokens};
use syn::{
    braced,
    bracketed,
    parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input,
    parse_quote,
    parse_quote_spanned,
    punctuated::Punctuated,
    spanned::Spanned,
    token::Bracket,
    Attribute,
    Block,
    Error,
    Expr,
    Fields,
    GenericArgument,
    Generics,
    Ident,
    ImplItem,
    ImplItemFn,
    ItemImpl,
    ItemStruct,
    Lifetime,
    Lit,
    Meta,
    Path,
    PathArguments,
    PathSegment,
    Token,
    Type,
    Visibility,
};

/// Generates:
/// * an `IntoFuture` executing the given method body
/// * an opaque wrapper type for the future in case we want to do something more fancy than
///   BoxFuture.
/// * a `run` method for sync execution, optionally with a wrapper function
#[proc_macro_attribute]
pub fn action_impl(
    attrs: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let ActionImplAttrs { sync_type } = parse_macro_input!(attrs as ActionImplAttrs);
    let ActionImpl {
        generics,
        lifetime,
        action,
        future_name,
        exec_self_mut,
        exec_output,
        exec_body,
    } = parse_macro_input!(input as ActionImpl);

    let mut unbounded_generics = generics.clone();
    for lt in unbounded_generics.lifetimes_mut() {
        lt.bounds.clear();
    }
    for ty in unbounded_generics.type_params_mut() {
        ty.bounds.clear();
    }

    let sync_run = if let Some(sync_type) = sync_type {
        quote! {
            /// Synchronously execute this action.
            pub fn run(self) -> Result<#sync_type> {
                crate::sync::TOKIO_RUNTIME.block_on(std::future::IntoFuture::into_future(self)).map(<#sync_type>::new)
            }
        }
    } else {
        quote! {
            /// Synchronously execute this action.
            pub fn run(self) -> #exec_output {
                crate::sync::TOKIO_RUNTIME.block_on(std::future::IntoFuture::into_future(self))
            }
        }
    };

    quote! {
        impl #generics crate::action::private::Sealed for #action { }

        impl #generics crate::action::Action for #action { }

        impl #generics std::future::IntoFuture for #action {
            type Output = #exec_output;
            type IntoFuture = #future_name #unbounded_generics;

            fn into_future(#exec_self_mut self) -> Self::IntoFuture {
                #future_name (Box::pin(async move {
                    #exec_body
                }))
            }
        }

        pub struct #future_name #generics (crate::BoxFuture<#lifetime, #exec_output>);

        impl #generics std::future::Future for #future_name #unbounded_generics {
            type Output = #exec_output;

            fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                self.0.as_mut().poll(cx)
            }
        }

        #[cfg(feature = "sync")]
        impl #generics #action {
            #sync_run
        }
    }.into()
}

// impl<generics> Action for ActionType {
// type Future = FutureName;
// async fn execute([mut] self) -> OutType { <exec body> }
// [SyncWrap]
// }
struct ActionImpl {
    generics: Generics,
    lifetime: Lifetime,
    action: Type,
    future_name: Ident,
    exec_self_mut: Option<Token![mut]>,
    exec_output: Type,
    exec_body: Block,
}

impl Parse for ActionImpl {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        // impl<generics> Action for ActionType
        input.parse::<Token![impl]>()?;
        let generics: Generics = input.parse()?;
        let mut lifetime = None;
        for lt in generics.lifetimes() {
            if lifetime.is_some() {
                return Err(input.error("only one lifetime argument permitted"));
            }
            lifetime = Some(lt);
        }
        let lifetime = match lifetime {
            Some(lt) => lt.lifetime.clone(),
            None => parse_quote_spanned! { generics.span() => 'static },
        };
        parse_name(input, "Action")?;
        input.parse::<Token![for]>()?;
        let action = input.parse()?;

        let impl_body;
        braced!(impl_body in input);

        // type Future = FutureName;
        impl_body.parse::<Token![type]>()?;
        parse_name(&impl_body, "Future")?;
        impl_body.parse::<Token![=]>()?;
        let future_name = impl_body.parse()?;
        impl_body.parse::<Token![;]>()?;

        // async fn execute([mut] self) -> OutType { <exec body> }
        impl_body.parse::<Token![async]>()?;
        impl_body.parse::<Token![fn]>()?;
        parse_name(&impl_body, "execute")?;
        let exec_args;
        parenthesized!(exec_args in impl_body);
        let exec_self_mut = exec_args.parse()?;
        exec_args.parse::<Token![self]>()?;
        if !exec_args.is_empty() {
            return Err(exec_args.error("unexpected token"));
        }
        impl_body.parse::<Token![->]>()?;
        let exec_output = impl_body.parse()?;
        let exec_body = impl_body.parse()?;

        if !impl_body.is_empty() {
            return Err(exec_args.error("unexpected token"));
        }

        Ok(ActionImpl {
            generics,
            lifetime,
            action,
            future_name,
            exec_self_mut,
            exec_output,
            exec_body,
        })
    }
}

struct ActionImplAttrs {
    sync_type: Option<Type>,
}

impl Parse for ActionImplAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut out = Self { sync_type: None };
        if input.is_empty() {
            return Ok(out);
        }

        parse_name(input, "sync")?;
        input.parse::<Token![=]>()?;
        out.sync_type = Some(input.parse()?);
        Ok(out)
    }
}

/// Parse an identifier with a specific expected value.
fn parse_name(input: ParseStream, name: &str) -> syn::Result<Ident> {
    let ident = input.parse::<Ident>()?;
    if ident.to_string() != name {
        return Err(Error::new(
            ident.span(),
            format!("expected '{}', got '{}'", name, ident),
        ));
    }
    Ok(ident)
}

macro_rules! compile_error {
    ($span:expr, $($message:tt)+) => {{
        return Error::new($span, format!($($message)+)).into_compile_error().into();
    }};
}

/// Enables rustdoc links to types that link individually to each type
/// component.
#[proc_macro_attribute]
pub fn deeplink(
    _attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let mut impl_fn = parse_macro_input!(item as ImplItemFn);

    for attr in &mut impl_fn.attrs {
        // Skip non-`doc` attrs
        if attr.path() != &parse_quote! { doc } {
            continue;
        }
        // Get the string literal value from #[doc = "lit"]
        let mut text = match &mut attr.meta {
            Meta::NameValue(nv) => match &mut nv.value {
                Expr::Lit(el) => match &mut el.lit {
                    Lit::Str(ls) => ls.value(),
                    _ => continue,
                },
                _ => continue,
            },
            _ => continue,
        };
        // Process substrings delimited by "d[...]"
        while let Some(ix) = text.find("d[") {
            let pre = &text[..ix];
            let rest = &text[ix + 2..];
            let end = match rest.find(']') {
                Some(v) => v,
                None => compile_error!(attr.span(), "unterminated d["),
            };
            let body = &rest[..end];
            let post = &rest[end + 1..];
            // Strip inner backticks, if any
            let (fixed, body) = if body.starts_with('`') && body.ends_with('`') {
                (
                    true,
                    body.strip_prefix('`').unwrap().strip_suffix('`').unwrap(),
                )
            } else {
                (false, body)
            };
            // Build new string
            let mut new_text = pre.to_owned();
            if fixed {
                new_text.push_str("<code>");
            }
            new_text.push_str(&text_link(body));
            if fixed {
                new_text.push_str("</code>");
            }
            new_text.push_str(post);
            text = new_text;
        }
        *attr = parse_quote! { #[doc = #text] };
    }

    impl_fn.into_token_stream().into()
}

fn text_link(text: &str) -> String {
    // Break into segments delimited by '<' or '>'
    let segments = text.split_inclusive(&['<', '>'])
        // Put each delimiter in its own segment
        .flat_map(|s| {
            if s == "<" || s == ">" {
                vec![s]
            } else if let Some(sub) = s.strip_suffix(&['<', '>']) {
                vec![sub, &s[sub.len()..]]
            } else {
                vec![s]
            }
        });

    // Build output
    let mut out = vec![];
    for segment in segments {
        match segment {
            // Escape angle brackets
            "<" => out.push("&lt;"),
            ">" => out.push("&gt;"),
            // Don't link unit
            "()" => out.push("()"),
            // Link to types
            _ => {
                // Use the short name
                let short = segment
                    .rsplit_once("::")
                    .map(|(_, short)| short)
                    .unwrap_or(segment);
                out.extend(["[", short, "](", segment, ")"]);
            }
        }
    }
    out.concat()
}

#[proc_macro]
pub fn option_setters(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let OptionSettersList {
        opt_field_name,
        opt_field_type,
        setters,
    } = parse_macro_input!(input as OptionSettersList);

    let extras = quote! {
        #[allow(unused)]
        fn options(&mut self) -> &mut #opt_field_type {
            self.#opt_field_name.get_or_insert_with(<#opt_field_type>::default)
        }

        /// Set all options.  Note that this will replace all previous values set.
        pub fn with_options(mut self, value: impl Into<Option<#opt_field_type>>) -> Self {
            self.#opt_field_name = value.into();
            self
        }
    };

    let setters: Vec<_> = setters
        .into_iter()
        .map(|OptionSetter { attrs, name, type_ }| {
            let docstr = format!(
                "Set the [`{}::{}`] option.",
                opt_field_type.to_token_stream(),
                name
            );
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
            quote! {
                #[doc = #docstr]
                #(#attrs)*
                pub fn #name(mut self, value: #accept) -> Self {
                    self.options().#name = Some(#value);
                    self
                }
            }
        })
        .collect();

    quote! {
        #extras
        #(#setters)*
    }
    .into()
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

struct OptionSettersList {
    opt_field_name: Ident,
    opt_field_type: Type,
    setters: Vec<OptionSetter>,
}

impl Parse for OptionSettersList {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let opt_field_name = input.parse()?;
        input.parse::<Token![:]>()?;
        let opt_field_type = input.parse()?;
        input.parse::<Token![;]>()?;
        let setters = input
            .parse_terminated(OptionSetter::parse, Token![,])?
            .into_iter()
            .collect();
        Ok(Self {
            opt_field_name,
            opt_field_type,
            setters,
        })
    }
}

struct OptionSetter {
    attrs: Vec<Attribute>,
    name: Ident,
    type_: Path,
}

impl Parse for OptionSetter {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let name = input.parse()?;
        input.parse::<Token![:]>()?;
        let type_ = input.parse()?;
        Ok(Self { attrs, name, type_ })
    }
}

#[import_tokens_attr]
#[with_custom_parsing(OptionSettersArgs)]
#[proc_macro_attribute]
pub fn option_setters_2(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let opt_struct = parse_macro_input!(attr as ItemStruct);
    let mut impl_in = parse_macro_input!(item as ItemImpl);
    let args = parse_macro_input!(__custom_tokens as OptionSettersArgs);

    // Gather information about each option struct field
    struct OptInfo {
        name: Ident,
        attrs: Vec<Attribute>,
        type_: Path,
    }
    let mut opt_info = vec![];
    let fields = match &opt_struct.fields {
        Fields::Named(f) => &f.named,
        _ => compile_error!(opt_struct.span(), "options struct must have named fields"),
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
            _ => compile_error!(field.span(), "invalid type"),
        };
        let type_ = match inner_type(outer, "Option") {
            Some(Type::Path(ty)) => ty.path.clone(),
            _ => compile_error!(field.span(), "invalid type"),
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

    // Build rustdoc information.
    let doc_name = args.doc_name;
    let mut doc_impl = impl_in.clone();
    // Synthesize a fn entry for each extra listed so it'll get a rustdoc entry
    if let Some((_, extra)) = args.extra {
        for name in &extra.names {
            doc_impl.items.push(parse_quote! {
                pub fn #name(&self) {}
            });
        }
    }

    // All done.  Export the tokens for doc use as their own distinct (uncompiled) item.
    quote! {
        #impl_in

        #[macro_magic::export_tokens_no_emit(#doc_name)]
        #doc_impl
    }
    .into()
}

struct OptionSettersArgs {
    source_text: (Ident, Token![=]), // source =
    foreign_path: syn::Path,
    name_text: (Token![,], Ident, Token![=]), // , doc_name =
    doc_name: Ident,
    extra: Option<(Token![,], OptionSettersArgsExtra)>,
}

#[derive(Debug)]
struct OptionSettersArgsExtra {
    extra_text: (Ident, Token![=]), // extra =
    bracket: Bracket,
    names: Punctuated<Ident, Token![,]>,
}

impl Parse for OptionSettersArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let source_text = (parse_name(input, "source")?, input.parse()?);
        let foreign_path = input.parse()?;
        let name_text = (
            input.parse()?,
            parse_name(input, "doc_name")?,
            input.parse()?,
        );
        let doc_name = input.parse()?;
        let extra = if input.is_empty() {
            None
        } else {
            Some((input.parse()?, input.parse()?))
        };
        Ok(Self {
            source_text,
            foreign_path,
            name_text,
            doc_name,
            extra,
        })
    }
}

impl ToTokens for OptionSettersArgs {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let Self {
            source_text,
            foreign_path,
            name_text,
            doc_name,
            extra,
        } = &self;
        tokens.extend(source_text.0.to_token_stream());
        tokens.extend(source_text.1.to_token_stream());
        tokens.extend(foreign_path.to_token_stream());
        tokens.extend(name_text.0.to_token_stream());
        tokens.extend(name_text.1.to_token_stream());
        tokens.extend(name_text.2.to_token_stream());
        tokens.extend(doc_name.to_token_stream());
        if let Some(extra) = extra {
            tokens.extend(extra.0.to_token_stream());
            tokens.extend(extra.1.to_token_stream());
        }
    }
}

impl ForeignPath for OptionSettersArgs {
    fn foreign_path(&self) -> &syn::Path {
        &self.foreign_path
    }
}

impl Parse for OptionSettersArgsExtra {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let extra_text = (parse_name(input, "extra")?, input.parse::<Token![=]>()?);
        let content;
        let bracket = bracketed!(content in input);
        let names = Punctuated::parse_separated_nonempty(&content)?;
        Ok(Self {
            extra_text,
            bracket,
            names,
        })
    }
}

impl ToTokens for OptionSettersArgsExtra {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        tokens.extend(self.extra_text.0.to_token_stream());
        tokens.extend(self.extra_text.1.to_token_stream());
        self.bracket.surround(tokens, |content| {
            content.extend(self.names.to_token_stream());
        });
    }
}

#[import_tokens_attr]
#[with_custom_parsing(OptionsDocArgs)]
#[proc_macro_attribute]
pub fn options_doc(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let setters = parse_macro_input!(attr as ItemImpl);
    let mut impl_fn = parse_macro_input!(item as ImplItemFn);
    let args = parse_macro_input!(__custom_tokens as OptionsDocArgs);

    // Collect a list of names from the setters impl
    let mut setter_names = vec![];
    for item in &setters.items {
        match item {
            ImplItem::Fn(item) if matches!(item.vis, Visibility::Public(..)) => {
                setter_names.push(item.sig.ident.to_token_stream().to_string());
            }
            _ => continue,
        }
    }

    // Get the rustdoc path to the action type, i.e. the type with generic arguments stripped
    let mut doc_path = match &*setters.self_ty {
        Type::Path(p) => p.path.clone(),
        t => compile_error!(t.span(), "invalid options doc argument"),
    };
    for seg in &mut doc_path.segments {
        seg.arguments = PathArguments::None;
    }
    let doc_path = doc_path.to_token_stream().to_string();

    // Add the list of setters to the rustdoc for the fn
    impl_fn.attrs.push(parse_quote! {
        #[doc = ""]
    });
    let preamble = format!(
        "These methods can be chained before `{}` to set options:",
        if args.is_async() { ".await" } else { "run" }
    );
    impl_fn.attrs.push(parse_quote! {
        #[doc = #preamble]
    });
    for name in setter_names {
        let docstr = format!("  * [`{0}`]({1}::{0})", name, doc_path);
        impl_fn.attrs.push(parse_quote! {
            #[doc = #docstr]
        });
    }
    impl_fn.into_token_stream().into()
}

struct OptionsDocArgs {
    foreign_path: syn::Path,
    sync: Option<(Token![,], Ident)>,
}

impl OptionsDocArgs {
    fn is_async(&self) -> bool {
        self.sync.is_none()
    }
}

impl Parse for OptionsDocArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let foreign_path = input.parse()?;
        let sync = if input.is_empty() {
            None
        } else {
            Some((input.parse()?, parse_name(input, "sync")?))
        };

        Ok(Self { foreign_path, sync })
    }
}

impl ToTokens for OptionsDocArgs {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        tokens.extend(self.foreign_path.to_token_stream());
        if let Some((comma, ident)) = &self.sync {
            tokens.extend(comma.to_token_stream());
            tokens.extend(ident.to_token_stream());
        }
    }
}

impl ForeignPath for OptionsDocArgs {
    fn foreign_path(&self) -> &syn::Path {
        &self.foreign_path
    }
}
