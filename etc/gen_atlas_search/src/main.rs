use convert_case::{Case, Casing};
use proc_macro2::TokenStream;
use quote::format_ident;
use serde::Deserialize;
use syn::parse_quote;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Operator {
    name: String,
    #[expect(dead_code)]
    link: String,
    #[serde(rename = "type")]
    #[expect(dead_code)]
    type_: Vec<OperatorType>,
    #[expect(dead_code)]
    encode: EncodeType,
    description: String,
    arguments: Vec<Argument>,
    tests: Vec<serde_yaml::Value>,
}

impl Operator {
    fn clear_tests(mut self) -> Self {
        self.tests.clear();
        self
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum OperatorType {
    SearchOperator,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum EncodeType {
    Object,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Argument {
    name: String,
    #[serde(default)]
    optional: Option<bool>,
    #[serde(rename = "type")]
    type_: Vec<ArgumentType>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum ArgumentType {
    String,
    Object,
    SearchScore,
    SearchPath,
}

enum ArgumentRustType {
    String,
    Document,
    StringOrArray,
    TokenOrder,
    MatchCriteria,
}

static QUERY: &str = "query";
static TOKEN_ORDER: &str = "tokenOrder";
static MATCH_CRITERIA: &str = "matchCriteria";

impl Argument {
    fn rust_type(&self) -> ArgumentRustType {
        if self.name == QUERY {
            return ArgumentRustType::StringOrArray;
        }
        if self.name == TOKEN_ORDER {
            return ArgumentRustType::TokenOrder;
        }
        if self.name == MATCH_CRITERIA {
            return ArgumentRustType::MatchCriteria;
        }
        if self.type_.len() != 1 {
            panic!("Unexpected argument types: {:?}", self.type_);
        }
        match &self.type_[0] {
            ArgumentType::String => ArgumentRustType::String,
            ArgumentType::Object => ArgumentRustType::Document,
            ArgumentType::SearchScore => ArgumentRustType::Document,
            ArgumentType::SearchPath => ArgumentRustType::StringOrArray,
        }
    }
}

impl ArgumentRustType {
    fn tokens(&self) -> syn::Type {
        match self {
            Self::String => parse_quote! { impl AsRef<str> },
            Self::Document => parse_quote! { Document },
            Self::StringOrArray => parse_quote! { impl StringOrArray },
            Self::TokenOrder => parse_quote! { TokenOrder },
            Self::MatchCriteria => parse_quote! { MatchCriteria },
        }
    }

    fn bson_expr(&self, ident: &syn::Ident) -> syn::Expr {
        match self {
            Self::String => parse_quote! { #ident.as_ref() },
            Self::StringOrArray => parse_quote! { #ident.to_bson() },
            Self::TokenOrder | Self::MatchCriteria => parse_quote! { #ident.name() },
            Self::Document => parse_quote! { #ident },
        }
    }
}

// Type inference helper: TokenStream impls Extend for both TokenTree and TokenStream, so calling
// `stream.extend(parse_quote! { blah })` is ambiguous, where `stream.push(...)` is not.
trait TokenStreamExt {
    fn push(&mut self, other: TokenStream);
}

impl TokenStreamExt for TokenStream {
    fn push(&mut self, other: TokenStream) {
        self.extend(other);
    }
}

fn gen_from_yaml(p: impl AsRef<std::path::Path>) -> TokenStream {
    let contents = std::fs::read_to_string(p).unwrap();
    let parsed = serde_yaml::from_str::<Operator>(&contents)
        .unwrap()
        .clear_tests();

    let name_text = parsed.name;
    let name_ident = format_ident!("{}", name_text.to_case(Case::Pascal));
    let constr_ident = format_ident!("{}", name_text.to_case(Case::Snake));

    let mut required_args = TokenStream::new();
    let mut init_doc = TokenStream::new();
    let mut setters = TokenStream::new();

    for arg in parsed.arguments {
        let ident = format_ident!("{}", arg.name.to_case(Case::Snake));
        let rust_type = arg.rust_type();
        let type_ = rust_type.tokens();
        let arg_name = &arg.name;
        let init_expr = rust_type.bson_expr(&ident);

        if arg.optional.unwrap_or(false) {
            setters.push(parse_quote! {
                #[allow(missing_docs)]
                pub fn #ident(mut self, #ident: #type_) -> Self {
                    self.stage.insert(#arg_name, #init_expr);
                    self
                }
            });
        } else {
            required_args.push(parse_quote! { #ident : #type_, });
            init_doc.push(parse_quote! { #arg_name : #init_expr, });
        }
    }

    let desc = parsed.description;
    parse_quote! {
        #[allow(missing_docs)]
        pub struct #name_ident;

        impl AtlasSearch<#name_ident> {
            #[doc = #desc]
            pub fn #constr_ident(#required_args) -> Self {
                AtlasSearch {
                    name: #name_text,
                    stage: doc! { #init_doc },
                    _t: PhantomData::default(),
                }
            }
            #setters
        }
    }
}

fn main() {
    let mut operators = TokenStream::new();
    for path in ["yaml/search/autocomplete.yaml", "yaml/search/text.yaml"] {
        operators.push(gen_from_yaml(path));
    }

    let file = parse_quote! {
        //! This file was autogenerated.  Do not manually edit.
        use super::*;

        #operators
    };
    let text = prettyplease::unparse(&file);
    println!("{text}");
}
