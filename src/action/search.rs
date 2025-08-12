use std::marker::PhantomData;

use crate::bson::{doc, Bson, Document};

pub struct AtlasSearch<T> {
    name: &'static str,
    stage: Document,
    _t: PhantomData<T>,
}

impl<T> Into<Document> for AtlasSearch<T> {
    fn into(self) -> Document {
        doc! {
            "$search": {
                self.name: self.stage
            }
        }
    }
}

pub struct Autocomplete;
pub struct Compound;
pub struct Text;
pub struct Built;

impl<T> AtlasSearch<T> {
    pub fn build(self) -> AtlasSearch<Built> {
        AtlasSearch {
            name: self.name,
            stage: self.stage,
            _t: PhantomData::default(),
        }
    }

    pub fn on_index(self, index: impl AsRef<str>) -> Document {
        let mut out: Document = self.into();
        // unwrap safety: AtlasSearch::into<Document> always produces a "$search" value
        out.get_document_mut("$search")
            .unwrap()
            .insert("index", index.as_ref());
        out
    }
}

impl<T> IntoIterator for AtlasSearch<T> {
    type Item = AtlasSearch<T>;

    type IntoIter = std::iter::Once<AtlasSearch<T>>;

    fn into_iter(self) -> Self::IntoIter {
        std::iter::once(self)
    }
}

impl AtlasSearch<Autocomplete> {
    /// Perform a search for a word or phrase that contains a sequence of characters from an
    /// incomplete input string.
    pub fn autocomplete(query: impl StringOrArray, path: impl AsRef<str>) -> Self {
        AtlasSearch {
            name: "autocomplete",
            stage: doc! {
                "query": query.to_bson(),
                "path": path.as_ref(),
            },
            _t: PhantomData::default(),
        }
    }

    /// Enable fuzzy search. Find strings which are similar to the search term or terms.
    pub fn fuzzy(mut self, options: Document) -> Self {
        self.stage.insert("fuzzy", options);
        self
    }

    /// Score to assign to the matching search term results.
    pub fn score(mut self, options: Document) -> Self {
        self.stage.insert("score", options);
        self
    }

    /// Order in which to search for tokens.
    pub fn token_order(mut self, order: TokenOrder) -> Self {
        self.stage.insert("tokenOrder", order.name());
        self
    }
}

/// Order in which to search for tokens.
pub enum TokenOrder {
    /// Indicates tokens in the query can appear in any order in the documents.
    Any,
    /// Indicates tokens in the query must appear adjacent to each other or in the order specified
    /// in the query in the documents.
    Sequential,
}

impl TokenOrder {
    fn name(&self) -> &'static str {
        match self {
            Self::Any => "any",
            Self::Sequential => "sequential",
        }
    }
}

impl AtlasSearch<Compound> {
    pub fn compound() -> Self {
        AtlasSearch {
            name: "compound",
            stage: doc! {},
            _t: PhantomData::default(),
        }
    }

    pub fn must<T>(mut self, clauses: impl IntoIterator<Item = AtlasSearch<T>>) -> Self {
        self.stage.insert(
            "must",
            clauses.into_iter().map(|sq| sq.stage).collect::<Vec<_>>(),
        );
        self
    }

    pub fn must_not<T>(mut self, clauses: impl IntoIterator<Item = AtlasSearch<T>>) -> Self {
        self.stage.insert(
            "mustNot",
            clauses.into_iter().map(|sq| sq.stage).collect::<Vec<_>>(),
        );
        self
    }

    pub fn should<T>(mut self, clauses: impl IntoIterator<Item = AtlasSearch<T>>) -> Self {
        self.stage.insert(
            "should",
            clauses.into_iter().map(|sq| sq.stage).collect::<Vec<_>>(),
        );
        self
    }
}

impl AtlasSearch<Text> {
    pub fn text(query: impl StringOrArray, path: impl StringOrArray) -> Self {
        AtlasSearch {
            name: "text",
            stage: doc! {
                "query": query.to_bson(),
                "path": path.to_bson(),
            },
            _t: PhantomData::default(),
        }
    }

    pub fn fuzzy(mut self, options: Document) -> Self {
        self.stage.insert("fuzzy", options);
        self
    }

    pub fn match_criteria(mut self, criteria: MatchCriteria) -> Self {
        self.stage.insert("matchCriteria", criteria.name());
        self
    }
}

pub enum MatchCriteria {
    Any,
    All,
}

impl MatchCriteria {
    fn name(&self) -> &'static str {
        match self {
            Self::Any => "any",
            Self::All => "all",
        }
    }
}

pub trait StringOrArray {
    fn to_bson(self) -> Bson;
}

impl StringOrArray for &str {
    fn to_bson(self) -> Bson {
        Bson::String(self.to_owned())
    }
}

impl StringOrArray for String {
    fn to_bson(self) -> Bson {
        Bson::String(self)
    }
}

impl StringOrArray for &String {
    fn to_bson(self) -> Bson {
        Bson::String(self.clone())
    }
}

impl StringOrArray for &[&str] {
    fn to_bson(self) -> Bson {
        Bson::Array(self.iter().map(|&s| Bson::String(s.to_owned())).collect())
    }
}

impl<const N: usize> StringOrArray for &[&str; N] {
    fn to_bson(self) -> Bson {
        Bson::Array(self.iter().map(|&s| Bson::String(s.to_owned())).collect())
    }
}

impl StringOrArray for &[String] {
    fn to_bson(self) -> Bson {
        Bson::Array(self.iter().map(|s| Bson::String(s.clone())).collect())
    }
}

impl<const N: usize> StringOrArray for &[String; N] {
    fn to_bson(self) -> Bson {
        Bson::Array(self.iter().map(|s| Bson::String(s.clone())).collect())
    }
}

impl<const N: usize> StringOrArray for [String; N] {
    fn to_bson(self) -> Bson {
        Bson::Array(self.into_iter().map(Bson::String).collect())
    }
}

impl StringOrArray for &[&String] {
    fn to_bson(self) -> Bson {
        Bson::Array(self.iter().map(|&s| Bson::String(s.clone())).collect())
    }
}

impl<const N: usize> StringOrArray for &[&String; N] {
    fn to_bson(self) -> Bson {
        Bson::Array(self.iter().map(|&s| Bson::String(s.clone())).collect())
    }
}

impl StringOrArray for Vec<&str> {
    fn to_bson(self) -> Bson {
        Bson::Array(
            self.into_iter()
                .map(|s| Bson::String(s.to_owned()))
                .collect(),
        )
    }
}

impl StringOrArray for Vec<String> {
    fn to_bson(self) -> Bson {
        Bson::Array(self.into_iter().map(Bson::String).collect())
    }
}

impl StringOrArray for Vec<&String> {
    fn to_bson(self) -> Bson {
        Bson::Array(self.into_iter().map(|s| Bson::String(s.clone())).collect())
    }
}

#[test]
fn api_flow() {
    let coll: crate::Collection<Document> = todo!();
    #[allow(unreachable_code)]
    {
        let _ = coll.aggregate(vec![
            AtlasSearch::autocomplete("pre", "title")
                .fuzzy(doc! { "maxEdits": 1, "prefixLength": 1, "maxExpansions": 256 })
                .into(),
            doc! {
                "$limit": 10,
            },
            doc! {
                "$project": {
                    "_id": 0,
                    "title": 1,
                }
            },
        ]);
        let _ = coll.aggregate(vec![
            AtlasSearch::text("baseball", "plot").into(),
            doc! { "$limit": 3 },
            doc! {
                "$project": {
                    "_id": 0,
                    "title": 1,
                    "plot": 1,
                }
            },
        ]);
        let _ = coll.aggregate(vec![
            AtlasSearch::compound()
                .must(AtlasSearch::text("varieties", "description"))
                .should(AtlasSearch::text("Fuji", "description"))
                .into(),
            doc! {
                "$project": {
                    "score": { "$meta": "searchScore" }
                }
            },
        ]);
    }
}
