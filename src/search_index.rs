use self::options::*;
use crate::{bson::Document, coll::options::AggregateOptions, error::Result, Collection, Cursor};

use bson::doc;
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

impl<T> Collection<T>
where
    T: Send + Sync,
{
    /// Gets index information for one or more search indexes in the collection.
    ///
    /// If name is not specified, information for all indexes on the specified collection will be
    /// returned.
    pub async fn list_search_indexes(
        &self,
        name: impl Into<Option<&str>>,
        aggregation_options: impl Into<Option<AggregateOptions>>,
        _list_index_options: impl Into<Option<ListSearchIndexOptions>>,
    ) -> Result<Cursor<Document>> {
        let mut inner = doc! {};
        if let Some(name) = name.into() {
            inner.insert("name", name.to_string());
        }
        self.clone_unconcerned()
            .aggregate(vec![doc! {
                "$listSearchIndexes": inner,
            }])
            .with_options(aggregation_options)
            .await
    }
}

/// Specifies the options for a search index.
#[derive(Debug, Clone, Default, TypedBuilder, Serialize, Deserialize)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct SearchIndexModel {
    /// The definition for this index.
    pub definition: Document,

    /// The name for this index, if present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

pub mod options {
    #[cfg(docsrs)]
    use crate::Collection;
    use serde::Deserialize;
    use typed_builder::TypedBuilder;

    /// Options for [Collection::create_search_index].  Present to allow additional options to be
    /// added in the future as a non-breaking change.
    #[derive(Clone, Debug, Default, TypedBuilder, Deserialize)]
    #[builder(field_defaults(default, setter(into)))]
    #[non_exhaustive]
    pub struct CreateSearchIndexOptions {}

    /// Options for [Collection::update_search_index].  Present to allow additional options to be
    /// added in the future as a non-breaking change.
    #[derive(Clone, Debug, Default, TypedBuilder, Deserialize)]
    #[builder(field_defaults(default, setter(into)))]
    #[non_exhaustive]
    pub struct UpdateSearchIndexOptions {}

    /// Options for [Collection::list_search_indexes].  Present to allow additional options to be
    /// added in the future as a non-breaking change.
    #[derive(Clone, Debug, Default, TypedBuilder, Deserialize)]
    #[builder(field_defaults(default, setter(into)))]
    #[non_exhaustive]
    pub struct ListSearchIndexOptions {}

    /// Options for [Collection::drop_search_index].  Present to allow additional options to be
    /// added in the future as a non-breaking change.
    #[derive(Clone, Debug, Default, TypedBuilder, Deserialize)]
    #[builder(field_defaults(default, setter(into)))]
    #[non_exhaustive]
    pub struct DropSearchIndexOptions {}
}
