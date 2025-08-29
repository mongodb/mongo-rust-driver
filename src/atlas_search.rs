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
/// # use mongodb::{Collection, bson::{Document, doc}};
/// # let collection: Collection<Document> = todo!();
/// use mongodb::atlas_search;
/// let cursor = collection.aggregate(vec![
///     atlas_search::autocomplete("title", "pre")
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
    meta: bool,
    _t: PhantomData<T>,
}

impl<T> AtlasSearch<T> {
    /// Erase the type of this builder.  Not typically needed, but can be useful to include builders
    /// of different types in a single `Vec`:
    /// ```no_run
    /// # async fn wrapper() -> mongodb::error::Result<()> {
    /// # use mongodb::{Collection, bson::{Document, doc}};
    /// # let collection: Collection<Document> = todo!();
    /// use mongodb::atlas_search;
    /// let cursor = collection.aggregate(vec![
    ///     atlas_search::compound()
    ///         .must(vec![
    ///             atlas_search::text("description", "varieties").unit(),
    ///             atlas_search::compound()
    ///                 .should(atlas_search::text("description", "Fuji"))
    ///                 .unit(),
    ///         ])
    ///         .into(),
    /// ]).await?;
    /// # }
    /// ```
    pub fn unit(self) -> AtlasSearch<()> {
        AtlasSearch {
            name: self.name,
            stage: self.stage,
            meta: self.meta,
            _t: PhantomData,
        }
    }

    /// Converts this builder into an aggregate pipeline stage [`Document`].
    pub fn stage(self) -> Document {
        let key = if self.meta { "$searchMeta" } else { "$search" };
        doc! {
            key: {
                self.name: self.stage
            }
        }
    }

    /// Like [`stage`](AtlasSearch::stage), converts this builder into an aggregate pipeline stage
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

mod private {
    use crate::bson::{doc, Bson};

    /// An Atlas Search operator parameter that can accept multiple types.
    pub trait Parameter {
        fn to_bson(self) -> Bson;
    }

    impl<T: Into<Bson>> Parameter for T {
        fn to_bson(self) -> Bson {
            self.into()
        }
    }

    impl<T> Parameter for super::AtlasSearch<T> {
        fn to_bson(self) -> Bson {
            Bson::Document(doc! { self.name: self.stage })
        }
    }
}

/// An Atlas Search operator parameter that can be either a string or array of strings.
pub trait StringOrArray: private::Parameter {}
impl StringOrArray for &str {}
impl StringOrArray for String {}
#[cfg(feature = "bson-3")]
impl<const N: usize> StringOrArray for [&str; N] {}
impl StringOrArray for &[&str] {}
impl StringOrArray for &[String] {}

/// An Atlas Search operator parameter that is itself a search operator.
pub trait SearchOperator: private::Parameter {}
impl<T> SearchOperator for AtlasSearch<T> {}
impl SearchOperator for Document {}

impl AtlasSearch<Facet> {
    /// Use the `$search` stage instead of the default `$searchMeta` stage.
    pub fn search(mut self) -> Self {
        self.meta = false;
        self
    }
}

/// Facet definitions.  These can be used when constructing a facet definition doc:
/// ```
/// use mongodb::atlas_search::facet;
/// let search = facet(doc! {
///   "directorsFacet": facet::string("directors").num_buckets(7),
///   "yearFacet": facet::number("year", [2000, 2005, 2010, 2015]),
/// });
/// ```
pub mod facet {
    use crate::bson::{doc, Bson, Document};
    use std::marker::PhantomData;

    /// A facet definition.
    pub struct Facet<T> {
        inner: Document,
        _t: PhantomData<T>,
    }

    impl<T> From<Facet<T>> for Bson {
        fn from(value: Facet<T>) -> Self {
            Bson::Document(value.inner)
        }
    }

    #[allow(missing_docs)]
    pub struct String;
    /// String facets allow you to narrow down Atlas Search results based on the most frequent
    /// string values in the specified string field.
    pub fn string(path: impl AsRef<str>) -> Facet<String> {
        Facet {
            inner: doc! {
                "type": "string",
                "path": path.as_ref(),
            },
            _t: PhantomData,
        }
    }
    impl Facet<String> {
        #[allow(missing_docs)]
        pub fn num_buckets(mut self, num: i32) -> Self {
            self.inner.insert("numBuckets", num);
            self
        }
    }

    #[allow(missing_docs)]
    pub struct Number;
    /// Numeric facets allow you to determine the frequency of numeric values in your search results
    /// by breaking the results into separate ranges of numbers.
    pub fn number(
        path: impl AsRef<str>,
        boundaries: impl IntoIterator<Item = impl Into<Bson>>,
    ) -> Facet<Number> {
        Facet {
            inner: doc! {
                "type": "number",
                "path": path.as_ref(),
                "boundaries": boundaries.into_iter().map(Into::into).collect::<Vec<_>>(),
            },
            _t: PhantomData,
        }
    }
    impl Facet<Number> {
        #[allow(missing_docs)]
        pub fn default(mut self, bucket: impl AsRef<str>) -> Self {
            self.inner.insert("default", bucket.as_ref());
            self
        }
    }

    #[allow(missing_docs)]
    pub struct Date;
    /// Date facets allow you to narrow down search results based on a date.
    pub fn date(
        path: impl AsRef<str>,
        boundaries: impl IntoIterator<Item = crate::bson::DateTime>,
    ) -> Facet<Date> {
        Facet {
            inner: doc! {
                "type": "date",
                "path": path.as_ref(),
                "boundaries": boundaries.into_iter().collect::<Vec<_>>(),
            },
            _t: PhantomData,
        }
    }
    impl Facet<Date> {
        #[allow(missing_docs)]
        pub fn default(mut self, bucket: impl AsRef<str>) -> Self {
            self.inner.insert("default", bucket.as_ref());
            self
        }
    }
}

/// Relation of the query shape geometry to the indexed field geometry.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Relation {
    /// Indicates that the indexed geometry contains the query geometry.
    Contains,
    /// Indicates that both the query and indexed geometries have nothing in common.
    Disjoint,
    /// Indicates that both the query and indexed geometries intersect.
    Intersects,
    /// Indicates that the indexed geometry is within the query geometry. You can't use within with
    /// LineString or Point.
    Within,
    /// Fallback for future compatibility.
    Other(String),
}

impl Relation {
    fn name(&self) -> &str {
        match self {
            Self::Contains => "contains",
            Self::Disjoint => "disjoint",
            Self::Intersects => "intersects",
            Self::Within => "within",
            Self::Other(s) => &s,
        }
    }
}

/// An Atlas Search operator parameter that can be either a document or array of documents.
pub trait DocumentOrArray: private::Parameter {}
impl DocumentOrArray for Document {}
//impl<const N: usize> DocumentOrArray for [Document; N] {}
impl DocumentOrArray for &[Document] {}

/// An Atlas Search operator parameter that can be any BSON numeric type.
pub trait BsonNumber {}
