extern crate proc_macro;

use quote::quote;
use syn::{
    braced,
    parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input,
    parse_quote_spanned,
    spanned::Spanned,
    Block,
    Generics,
    Ident,
    Lifetime,
    Token,
    Type,
};

use crate::parse_name;

pub(crate) fn action_impl(
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
