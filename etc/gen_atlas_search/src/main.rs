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

impl Argument {
    fn type_(&self) -> syn::Type {
        if self.type_.len() != 1 {
            panic!("Unexpected argument types: {:?}", self.type_);
        }
        match &self.type_[0] {
            ArgumentType::String => parse_quote! { impl AsRef<str> },
            ArgumentType::Object => parse_quote! { Document },
            ArgumentType::SearchScore => parse_quote! { Document },
            ArgumentType::SearchPath => parse_quote! { impl StringOrArray },
        }
    }

    fn bson_expr(&self, ident: &syn::Ident) -> syn::Expr {
        if self.type_.len() != 1 {
            panic!("Unexpected argument types: {:?}", self.type_);
        }
        match &self.type_[0] {
            ArgumentType::String => parse_quote! { #ident.as_ref() },
            ArgumentType::SearchPath => parse_quote! { #ident.to_bson() },
            _ => parse_quote! { #ident },
        }
    }
}

trait TokenStreamExt {
    fn push(&mut self, other: TokenStream);
}

impl TokenStreamExt for TokenStream {
    fn push(&mut self, other: TokenStream) {
        self.extend(other);
    }
}

fn gen_from_yaml(p: impl AsRef<std::path::Path>) -> String {
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
        let type_ = arg.type_();
        let arg_name = &arg.name;
        let init_expr = arg.bson_expr(&ident);

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
    let output: syn::File = parse_quote! {
        use super::*;

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
    };

    prettyplease::unparse(&output)
}

fn main() {
    let text = gen_from_yaml("yaml/search/autocomplete.yaml");
    println!("{text}");
}
