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
/// # async fn wrapper() -> mongodb::error::Result<()> {
/// # use mongodb::{Collection, atlas_search::AtlasSearch, bson::{Document, doc}};
/// # let collection: Collection<Document> = todo!();
/// let cursor = collection.aggregate(vec![
///     AtlasSearch::autocomplete("title", "pre")
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

impl<T> From<AtlasSearch<T>> for Document {
    fn from(value: AtlasSearch<T>) -> Self {
        doc! {
            "$search": {
                value.name: value.stage
            }
        }
    }
}

impl<T> AtlasSearch<T> {
    /// Erase the type of this builder.  Not typically needed, but can be useful to include builders
    /// of different types in a single `Vec`.
    pub fn unit(self) -> AtlasSearch<()> {
        AtlasSearch {
            name: self.name,
            stage: self.stage,
            _t: PhantomData,
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
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
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
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum MatchCriteria {
    /// Return documents that contain any of the terms from the query field.
    Any,
    /// Only return documents that contain all of the terms from the query field.
    All,
    /// Fallback for future compatibility.
    Other(String),
}

impl MatchCriteria {
    fn name(&self) -> &str {
        match self {
            Self::Any => "any",
            Self::All => "all",
            Self::Other(s) => s.as_str(),
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

#[tokio::test]
async fn api_flow() {
    // This is currently intended as a testbed for how the API works, not as an actual test.
    return;

    #[allow(unreachable_code)]
    {
        #[allow(unused_variables)]
        let coll: crate::Collection<Document> = todo!();
        let _ = coll
            .aggregate(vec![
                AtlasSearch::autocomplete("title", "pre")
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
            ])
            .await;
        let _ = coll
            .aggregate(vec![
                AtlasSearch::text("plot", "baseball").into(),
                doc! { "$limit": 3 },
                doc! {
                    "$project": {
                        "_id": 0,
                        "title": 1,
                        "plot": 1,
                    }
                },
            ])
            .await;
        let _ = coll
            .aggregate(vec![
                AtlasSearch::compound()
                    .must(AtlasSearch::text("description", "varieties"))
                    .should(AtlasSearch::text("description", "Fuji"))
                    .into(),
                doc! {
                    "$project": {
                        "score": { "$meta": "searchScore" }
                    }
                },
            ])
            .await;
    }
}
