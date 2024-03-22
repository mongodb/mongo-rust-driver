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
    Generics,
    Ident,
    ImplItemFn,
    Lifetime,
    Path,
    Token,
    Type,
};

/// Generates:
/// * an `IntoFuture` executing the given method body
/// * an opaque wrapper type for the future in case we want to do something more fancy than
///   BoxFuture.
/// * a `run` method for sync execution, optionally with a wrapper function
#[proc_macro]
pub fn action_impl(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ActionImpl {
        generics,
        lifetime,
        action,
        future_name,
        exec_self_mut,
        exec_output,
        exec_body,
        sync_wrap,
    } = parse_macro_input!(input as ActionImpl);

    let mut unbounded_generics = generics.clone();
    for lt in unbounded_generics.lifetimes_mut() {
        lt.bounds.clear();
    }
    for ty in unbounded_generics.type_params_mut() {
        ty.bounds.clear();
    }

    let SyncWrap {
        sync_arg_mut,
        sync_arg,
        sync_output,
        sync_body,
    } = sync_wrap.unwrap_or_else(|| {
        parse_quote! { fn sync_wrap(out) -> #exec_output { out } }
    });

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
            /// Synchronously execute this action.
            pub fn run(self) -> #sync_output {
                let #sync_arg_mut #sync_arg = crate::sync::TOKIO_RUNTIME.block_on(std::future::IntoFuture::into_future(self));
                #sync_body
            }
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
    sync_wrap: Option<SyncWrap>,
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

        // Optional SyncWrap.
        let sync_wrap = if impl_body.peek(Token![fn]) {
            Some(impl_body.parse()?)
        } else {
            None
        };

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
            sync_wrap,
        })
    }
}

// fn sync_wrap([mut] out) -> OutType { <out body> }
struct SyncWrap {
    sync_arg_mut: Option<Token![mut]>,
    sync_arg: Ident,
    sync_output: Type,
    sync_body: Block,
}

impl Parse for SyncWrap {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        input.parse::<Token![fn]>()?;
        parse_name(input, "sync_wrap")?;
        let args_input;
        parenthesized!(args_input in input);
        let sync_arg_mut = args_input.parse()?;
        let sync_arg = args_input.parse()?;
        if !args_input.is_empty() {
            return Err(args_input.error("unexpected token"));
        }
        input.parse::<Token![->]>()?;
        let sync_output = input.parse()?;
        let sync_body = input.parse()?;

        Ok(SyncWrap {
            sync_arg_mut,
            sync_arg,
            sync_output,
            sync_body,
        })
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

/// #[action_return_doc(return_type [, session = type] [, run = path])]
///
/// Generates linked documentation for the return value of an action.
#[proc_macro_attribute]
pub fn action_return_doc(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let ActionReturnArgs {
        ret_type,
        session_type,
        run,
    } = parse_macro_input!(attr as ActionReturnArgs);
    let mut impl_fn = parse_macro_input!(item as ImplItemFn);

    let call = if let Some(r) = run {
        format!("<code>{}</code>", doc_link(&r))
    } else {
        "`await`".to_string()
    };
    let session = if let Some(t) = session_type {
        format!(
            " or <code>{}</code> if a [`ClientSession`] is provided",
            doc_link(&t)
        )
    } else {
        String::new()
    };
    let s = format!(
        "\n\n{} will return <code>{}</code>{}.",
        call,
        doc_link(&ret_type),
        session,
    );
    let attr: Attribute = parse_quote! { #[doc = #s] };
    impl_fn.attrs.push(attr);
    impl_fn.into_token_stream().into()
}

// return type [, session = type] [, run = path]
struct ActionReturnArgs {
    ret_type: Type,
    session_type: Option<Type>,
    run: Option<Path>,
}

impl Parse for ActionReturnArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ret_type = input.parse()?;

        let mut session_type = None;
        let mut run = None;
        for _ in 0..2 {
            if input.parse::<Option<Token![,]>>()?.is_none() {
                break;
            }
            let ident = input.parse::<Ident>()?;
            match ident.to_string().as_str() {
                "session" => {
                    input.parse::<Token![=]>()?;
                    session_type = Some(input.parse()?);
                }
                "run" => {
                    input.parse::<Token![=]>()?;
                    run = Some(input.parse()?);
                }
                _ => {
                    return Err(Error::new(
                        ident.span(),
                        format!("expected 'session' or 'run', got '{}'", ident),
                    ))
                }
            }
        }

        Ok(Self {
            ret_type,
            session_type,
            run,
        })
    }
}

fn doc_link<T: ToTokens>(value: &T) -> String {
    // Convert to text..
    let text = value
        .to_token_stream()
        .to_string()
        //.. and strip whitespace
        .split_ascii_whitespace()
        .collect::<String>();

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
                if let Some((_, short)) = segment.rsplit_once("::") {
                    out.extend(["[", short, "](", segment, ")"]);
                } else {
                    out.extend(["[", segment, "]"]);
                }
            }
        }
    }
    out.concat()
}
