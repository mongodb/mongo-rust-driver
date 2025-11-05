extern crate proc_macro;

use quote::{quote, ToTokens};
use syn::{
    bracketed,
    parse::{Parse, ParseStream},
    parse_macro_input,
    parse_quote,
    punctuated::Punctuated,
    spanned::Spanned,
    Error,
    Expr,
    Ident,
    ImplItemFn,
    ItemImpl,
    Lit,
    Meta,
    PathArguments,
    Token,
};

use crate::{macro_error, parse_name};

pub(crate) fn deeplink(
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
                None => macro_error!(attr.span(), "unterminated d["),
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
            } else if let Some(sub) = s.strip_suffix(['<', '>']) {
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

pub(crate) fn options_doc(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
    custom_tokens: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let setters = parse_macro_input!(attr as ItemImpl);
    let mut impl_fn = parse_macro_input!(item as ImplItemFn);
    let args = parse_macro_input!(custom_tokens as OptionsDocArgs);

    // Collect a list of names from the setters impl
    let mut setter_names = vec![];
    for item in &setters.items {
        match item {
            syn::ImplItem::Fn(item) if matches!(item.vis, syn::Visibility::Public(..)) => {
                setter_names.push(item.sig.ident.to_token_stream().to_string());
            }
            _ => continue,
        }
    }

    // Get the rustdoc path to the action type, i.e. the type with generic arguments stripped
    let mut doc_path = match &*setters.self_ty {
        syn::Type::Path(p) => p.path.clone(),
        t => macro_error!(t.span(), "invalid options doc argument"),
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
        args.term
            .as_ref()
            .map(|(_, b)| b.as_str())
            .unwrap_or(".await")
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

pub(crate) struct OptionsDocArgs {
    foreign_path: syn::Path,
    term: Option<(Token![,], String)>,
}

impl Parse for OptionsDocArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let foreign_path = input.parse()?;
        let term = if input.is_empty() {
            None
        } else {
            let (comma, lit) = (input.parse()?, input.parse::<syn::LitStr>()?);
            Some((comma, lit.value()))
        };

        Ok(Self { foreign_path, term })
    }
}

impl ToTokens for OptionsDocArgs {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        tokens.extend(self.foreign_path.to_token_stream());
        if let Some((comma, lit)) = &self.term {
            tokens.extend(comma.to_token_stream());
            tokens.extend(lit.to_token_stream());
        }
    }
}

impl macro_magic::mm_core::ForeignPath for OptionsDocArgs {
    fn foreign_path(&self) -> &syn::Path {
        &self.foreign_path
    }
}

pub(crate) fn export_doc(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let args = parse_macro_input!(attr as ExportDocArgs);
    let impl_in = parse_macro_input!(item as ItemImpl);

    let mut doc_impl = impl_in.clone();
    // Synthesize a fn entry for each extra listed so it'll get a rustdoc entry
    if let Some(extra) = args.extra {
        for name in &extra {
            doc_impl.items.push(parse_quote! {
                pub fn #name(&self) {}
            });
        }
    }

    // All done.
    let doc_name = args.name;
    quote! {
        #impl_in

        #[macro_magic::export_tokens_no_emit(#doc_name)]
        #doc_impl
    }
    .into()
}

struct ExportDocArgs {
    name: Ident,
    extra: Option<Vec<Ident>>, // extra = [ident, ..]
}

impl Parse for ExportDocArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name = input.parse()?;
        let mut out = Self { name, extra: None };
        if input.parse::<Option<Token![,]>>()?.is_none() || input.is_empty() {
            return Ok(out);
        }

        parse_name(input, "extra")?;
        input.parse::<Token![=]>()?;
        let content;
        bracketed!(content in input);
        let punc = Punctuated::<Ident, Token![,]>::parse_terminated(&content)?;
        out.extra = Some(punc.into_pairs().map(|p| p.into_value()).collect());

        Ok(out)
    }
}
