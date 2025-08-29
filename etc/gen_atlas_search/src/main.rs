use std::path::PathBuf;

use convert_case::{Case, Casing};
use proc_macro2::TokenStream;
use quote::format_ident;
use serde::Deserialize;
use syn::parse_quote;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Operator {
    name: String,
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

impl Operator {
    fn clear_tests(mut self) -> Self {
        self.tests.clear();
        self
    }

    fn gen_helper(&self) -> TokenStream {
        let name_text = &self.name;
        let ident_base = match name_text.as_str() {
            "in" => "searchIn",
            _ => name_text,
        };
        let name_ident = format_ident!("{}", ident_base.to_case(Case::Pascal));
        let constr_ident = format_ident!("{}", ident_base.to_case(Case::Snake));

        let mut required_args = TokenStream::new();
        let mut required_arg_names = TokenStream::new();
        let mut init_doc = TokenStream::new();
        let mut setters = TokenStream::new();

        for arg in &self.arguments {
            let ident = format_ident!(
                "{}",
                match arg.name.as_str() {
                    // `box` is a reserved word
                    "box" => "geo_box".to_owned(),
                    _ => arg.name.to_case(Case::Snake),
                }
            );
            let rust_type = arg.rust_type(&self.name);
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
                required_arg_names.push(parse_quote! { #ident, });
                init_doc.push(parse_quote! { #arg_name : #init_expr, });
            }
        }

        let desc = &self.description;
        let link = format!(
            "For more details, see the [{name_text} operator reference]({}).",
            self.link
        );
        let meta: TokenStream = match self.name.as_str() {
            "facet" => parse_quote! { true },
            _ => parse_quote! { false },
        };
        parse_quote! {
            #[allow(missing_docs)]
            pub struct #name_ident;

            #[doc = #desc]
            #[doc = ""]
            #[doc = #link]
            pub fn #constr_ident(#required_args) -> AtlasSearch<#name_ident> {
                AtlasSearch {
                    name: #name_text,
                    stage: doc! { #init_doc },
                    meta: #meta,
                    _t: PhantomData,
                }
            }

            impl AtlasSearch<#name_ident> {
                #setters
            }
        }
    }
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
    Any,
    Array,
    BinData,
    Bool,
    Date,
    Geometry,
    Int,
    Null,
    Number,
    Object,
    ObjectId,
    SearchOperator,
    SearchPath,
    SearchScore,
    String,
}

impl Argument {
    fn rust_type(&self, operator: &str) -> ArgumentRustType {
        match (operator, self.name.as_str()) {
            ("autocomplete" | "text", "query") => return ArgumentRustType::StringOrArray,
            ("autocomplete", "tokenOrder") => return ArgumentRustType::TokenOrder,
            ("text", "matchCriteria") => return ArgumentRustType::MatchCriteria,
            ("equals", "value") => return ArgumentRustType::IntoBson,
            ("geoShape", "relation") => return ArgumentRustType::Relation,
            ("range", "gt" | "gte" | "lt" | "lte") => return ArgumentRustType::IntoBson,
            ("near", "origin") => return ArgumentRustType::NearOrigin,
            _ => (),
        }
        use ArgumentType::*;
        match self.type_.as_slice() {
            [String] => ArgumentRustType::String,
            [Object] => ArgumentRustType::Document,
            [SearchScore] => ArgumentRustType::Document,
            [SearchPath] => ArgumentRustType::StringOrArray,
            [SearchOperator] => ArgumentRustType::SearchOperator,
            [SearchOperator, Array] => ArgumentRustType::SeachOperatorIter,
            [Int] => ArgumentRustType::I32,
            [Geometry] => ArgumentRustType::Document,
            [Any, Array] => ArgumentRustType::IntoBson,
            [Object, Array] => ArgumentRustType::DocumentOrArray,
            [Number] => ArgumentRustType::BsonNumber,
            _ => panic!("Unexpected argument types: {:?}", self.type_),
        }
    }
}

enum ArgumentRustType {
    BsonNumber,
    Document,
    DocumentOrArray,
    I32,
    IntoBson,
    MatchCriteria,
    NearOrigin,
    Relation,
    SearchOperator,
    SeachOperatorIter,
    String,
    StringOrArray,
    TokenOrder,
}

impl ArgumentRustType {
    fn tokens(&self) -> syn::Type {
        match self {
            Self::BsonNumber => parse_quote! { impl BsonNumber },
            Self::Document => parse_quote! { Document },
            Self::DocumentOrArray => parse_quote! { impl DocumentOrArray },
            Self::I32 => parse_quote! { i32 },
            Self::IntoBson => parse_quote! { impl Into<Bson> },
            Self::MatchCriteria => parse_quote! { MatchCriteria },
            Self::NearOrigin => parse_quote! { impl NearOrigin },
            Self::Relation => parse_quote! { Relation },
            Self::SearchOperator => parse_quote! { impl SearchOperator },
            Self::SeachOperatorIter => {
                parse_quote! { impl IntoIterator<Item = impl SearchOperator> }
            }
            Self::String => parse_quote! { impl AsRef<str> },
            Self::StringOrArray => parse_quote! { impl StringOrArray },
            Self::TokenOrder => parse_quote! { TokenOrder },
        }
    }

    fn bson_expr(&self, ident: &syn::Ident) -> syn::Expr {
        match self {
            Self::Document | Self::I32 => parse_quote! { #ident },
            Self::IntoBson => parse_quote! { #ident.into() },
            Self::SeachOperatorIter => {
                parse_quote! { #ident.into_iter().map(|o| o.to_bson()).collect::<Vec<_>>() }
            }
            Self::String => parse_quote! { #ident.as_ref() },
            Self::StringOrArray
            | Self::DocumentOrArray
            | Self::SearchOperator
            | Self::NearOrigin
            | Self::BsonNumber => {
                parse_quote! { #ident.to_bson() }
            }
            Self::TokenOrder | Self::MatchCriteria | Self::Relation => {
                parse_quote! { #ident.name() }
            }
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

fn main() {
    let mut operators = TokenStream::new();
    for name in [
        "autocomplete",
        "compound",
        "embeddedDocument",
        "equals",
        "exists",
        "facet",
        "range",
        "geoShape",
        "geoWithin",
        "in",
        "moreLikeThis",
        "near",
        "text",
    ] {
        let mut path = PathBuf::from("yaml/search");
        path.push(name);
        path.set_extension("yaml");
        let contents = std::fs::read_to_string(path).unwrap();
        let parsed = serde_yaml::from_str::<Operator>(&contents)
            .unwrap()
            .clear_tests();
        operators.push(parsed.gen_helper());
    }

    let file = parse_quote! {
        //! This file was autogenerated.  Do not manually edit.
        use super::*;

        #operators
    };
    let text = prettyplease::unparse(&file);
    println!("{text}");
}
