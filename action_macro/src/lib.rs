extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{braced, parenthesized, parse::{Parse, ParseStream}, parse_macro_input, parse_quote, punctuated::Punctuated, spanned::Spanned, Block, Generics, Ident, Lifetime, LifetimeParam, Token, Type};

#[proc_macro]
pub fn action_impl_2(input: TokenStream) -> TokenStream {
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
        arg_mut: sync_arg_mut,
        arg: sync_arg,
        sync_output,
        sync_body,
     } = sync_wrap.unwrap_or_else(|| {
        SyncWrap {
            arg_mut: None,
            arg: Ident::new("out", Span::call_site()),
            sync_output: exec_output.clone(),
            sync_body: parse_quote!({ out }),
        }
    });

    quote! {
        impl #generics crate::action::Action for #action {
            type Output = #exec_output;
        }

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

/*
impl<generics> Action for ActionType {
    type Future = FutureName;
    async fn execute([mut] self) -> OutType { <exec body> }
    [SyncWrap]
}
*/
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
            None => Lifetime::new("'static'", generics.span()),
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
    arg_mut: Option<Token![mut]>,
    arg: Ident,
    sync_output: Type,
    sync_body: Block,
}

impl Parse for SyncWrap {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        input.parse::<Token![fn]>()?;
        parse_name(input, "sync_wrap")?;
        let args_input;
        parenthesized!(args_input in input);
        let arg_mut = args_input.parse()?;
        let arg = args_input.parse()?;
        if !args_input.is_empty() {
            return Err(args_input.error("unexpected token"));
        }
        input.parse::<Token![->]>()?;
        let sync_output = input.parse()?;
        let sync_body = input.parse()?;

        Ok(SyncWrap {
            arg_mut,
            arg,
            sync_output,
            sync_body,
        })
    }
}

/// Parse an identifier with a specific expected value.
fn parse_name(input: ParseStream, name: &str) -> syn::Result<()> {
    if input.parse::<Ident>()?.to_string() != name {
        return Err(input.error(format!("expected '{}'", name)));
    }
    Ok(())
}