extern crate proc_macro;

use quote::{quote, ToTokens};
use syn::{
    braced,
    parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input,
    parse_quote,
    parse_quote_spanned,
    spanned::Spanned,
    Attribute,
    Block,
    Error,
    Expr,
    GenericArgument,
    Generics,
    Ident,
    ImplItemFn,
    Lifetime,
    Lit,
    Meta,
    Path,
    PathArguments,
    PathSegment,
    Token,
    Type,
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
fn parse_name(input: ParseStream, name: &str) -> syn::Result<()> {
    let ident = input.parse::<Ident>()?;
    if ident.to_string() != name {
        return Err(Error::new(
            ident.span(),
            format!("expected '{}', got '{}'", name, ident),
        ));
    }
    Ok(())
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
                None => {
                    return Error::new(attr.span(), "unterminated d[")
                        .into_compile_error()
                        .into()
                }
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

    let extras = opt_field_name.map(|name| {
        quote! {
            #[allow(unused)]
            fn options(&mut self) -> &mut #opt_field_type {
                self.#name.get_or_insert_with(<#opt_field_type>::default)
            }

            /// Set all options.  Note that this will replace all previous values set.
            pub fn with_options(mut self, value: impl Into<Option<#opt_field_type>>) -> Self {
                self.#name = value.into();
                self
            }
        }
    });

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
            } else if let Some(t) = vec_arg(&type_) {
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

fn vec_arg(path: &Path) -> Option<&Type> {
    if path.segments.len() != 1 {
        return None;
    }
    let PathSegment { ident, arguments } = path.segments.first()?;
    if ident != "Vec" {
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
    opt_field_name: Option<Ident>,
    opt_field_type: Type,
    setters: Vec<OptionSetter>,
}

impl Parse for OptionSettersList {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let opt_field_name = if input.peek2(Token![:]) {
            let val = input.parse()?;
            input.parse::<Token![:]>()?;
            Some(val)
        } else {
            None
        };
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
