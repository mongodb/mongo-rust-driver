extern crate proc_macro;

use proc_macro2::Span;
use quote::{quote, ToTokens};
use syn::{
    braced,
    parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input,
    parse_quote,
    parse_quote_spanned,
    spanned::Spanned,
    visit_mut::VisitMut,
    Block,
    Error,
    Expr,
    Generics,
    Ident,
    ImplItemFn,
    Lifetime,
    Lit,
    Meta,
    PathArguments,
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
        // In expression position, the type needs to be of the form Foo::<Bar>, not Foo<Bar>
        let mut formal = sync_type.clone();
        struct Visitor;
        impl VisitMut for Visitor {
            fn visit_path_segment_mut(&mut self, segment: &mut syn::PathSegment) {
                if let PathArguments::AngleBracketed(args) = &mut segment.arguments {
                    if args.colon2_token.is_none() {
                        args.colon2_token = Some(Token![::](Span::call_site()));
                    }
                }
            }
        }
        syn::visit_mut::visit_type_mut(&mut Visitor, &mut formal);
        quote! {
            /// Synchronously execute this action.
            pub fn run(self) -> Result<#sync_type> {
                crate::sync::TOKIO_RUNTIME.block_on(std::future::IntoFuture::into_future(self)).map(#formal::new)
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
