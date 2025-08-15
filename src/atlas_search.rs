//! Helpers for building Atlas Search aggregation pipelines.

mod gen;

pub use gen::*;

use std::marker::PhantomData;

use crate::bson::{doc, Bson, Document};

/// A helper to build the aggregation stage for Atlas Search.  Use one of the constructor functions
/// and chain optional value setters, and then convert to a pipeline stage [`Document`] via
/// [`into`](Into::into) or [`on_index`](AtlasSearch::on_index).
///
/// ```no_run
/// # async fn wrapper() -> mongodb::error::Error {
/// # use mongodb::{Collection, bson::{Document, doc}};
/// # let collection: Collection<Document> = todo!()
/// let cursor = coll.aggregate(vec![
///     AtlasSearch::autocomplete("pre", "title")
///         .fuzzy(doc! { "maxEdits": 1, "prefixLength": 1, "maxExpansions": 256 })
///         .into(),
///     doc! {
///         "$limit": 10,
///    },
///    doc! {
///        "$project": {
///            "_id": 0,
///            "title": 1,
///         }
///    },
/// ]).await?;
/// # Ok(())
/// # }
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

#[allow(missing_docs)]
pub struct Built;

impl<T> AtlasSearch<T> {
    /// Finalize this builder.  Not typically needed, but can be useful to include builders of
    /// different types in a single `Vec`.
    pub fn build(self) -> AtlasSearch<Built> {
        AtlasSearch {
            name: self.name,
            stage: self.stage,
            _t: PhantomData::default(),
        }
    }

    /// Like [`into`](Into::into), converts this builder into an aggregate pipeline stage
    /// [`Document`], but also specify the search index to use.
    pub fn on_index(self, index: impl AsRef<str>) -> Document {
        doc! {
            "$search": {
                "index": index.as_ref(),
                self.name: self.stage,
            }
        }
    }
}

impl<T> IntoIterator for AtlasSearch<T> {
    type Item = AtlasSearch<T>;

    type IntoIter = std::iter::Once<AtlasSearch<T>>;

    fn into_iter(self) -> Self::IntoIter {
        std::iter::once(self)
    }
}

/// Order in which to search for tokens.
#[derive(Clone, PartialEq)]
pub enum TokenOrder {
    /// Indicates tokens in the query can appear in any order in the documents.
    Any,
    /// Indicates tokens in the query must appear adjacent to each other or in the order specified
    /// in the query in the documents.
    Sequential,
    /// Fallback for future compatibility.
    Other(String),
}

impl TokenOrder {
    fn name(&self) -> &str {
        match self {
            Self::Any => "any",
            Self::Sequential => "sequential",
            Self::Other(s) => s.as_str(),
        }
    }
}

/// Criteria to use to match the terms in the query.
pub enum MatchCriteria {
    /// Return documents that contain any of the terms from the query field.
    Any,
    /// Only return documents that contain all of the terms from the query field.
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

/// An Atlas Search operator parameter that can be either a string or array of strings.
pub trait StringOrArray {
    #[allow(missing_docs)]
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

/*
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
*/
