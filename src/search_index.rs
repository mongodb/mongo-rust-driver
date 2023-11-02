use crate::{bson::Document, Collection, error::Result, coll::options::AggregateOptions, Cursor};
use self::options::*;

use typed_builder::TypedBuilder;

impl<T> Collection<T> {
    /// Convenience method for creating a single search index.
    pub async fn create_search_index(model: &SearchIndexModel, options: impl Into<Option<CreateSearchIndexOptions>>) -> Result<String> {
        todo!()
    }

    /// Creates multiple search indexes on the collection.
    pub async fn create_search_indexes(models: impl IntoIterator<Item=&SearchIndexModel>, options: impl Into<Option<CreateSearchIndexOptions>>) -> Result<Vec<String>> {
        todo!()
    }

    /// Updates the search index with the given name to use the provided definition.
    pub async fn update_search_index(name: &str, definition: &Document, options: impl Into<Option<UpdateSearchIndexOptions>>) -> Result<()> {
        todo!()
    }

    /// Drops the search index with the given name.
    pub async fn drop_search_index(name: &str, options: impl Into<Option<DropSearchIndexOptions>>) -> Result<()> {
        todo!()
    }

    /// Gets index information for one or more search indexes in the collection.
    ///
    /// If name is not specified, information for all indexes on the specified collection will be returned.
    pub async fn list_search_indexes(name: impl Into<Option<&str>>, aggregation_options: impl Into<Option<AggregateOptions>>, list_index_options: impl Into<Option<ListSearchIndexOptions>>) -> Result<Cursor<Document>> {
        todo!()
    }
}

#[derive(Debug, Clone, Default, TypedBuilder)]
#[non_exhaustive]
pub struct SearchIndexModel {
    /// The definition for this index.
    pub definition: Document,

    /// The name for this index, if present.
    pub name: Option<String>,
}

pub mod options {
    use typed_builder::TypedBuilder;

    /// Options for [Collection::create_search_index].  Present to allow additional options to be added in the future as a non-breaking change.
    #[derive(Clone, Debug, Default, TypedBuilder)]
    #[non_exhaustive]
    pub struct CreateSearchIndexOptions { }

    /// Options for [Collection::update_search_index].  Present to allow additional options to be added in the future as a non-breaking change.
    #[derive(Clone, Debug, Default, TypedBuilder)]
    #[non_exhaustive]
    pub struct UpdateSearchIndexOptions { }

    /// Options for [Collection::list_search_indexes].  Present to allow additional options to be added in the future as a non-breaking change.
    #[derive(Clone, Debug, Default, TypedBuilder)]
    #[non_exhaustive]
    pub struct ListSearchIndexOptions { }

    /// Options for [Collection::drop_search_index].  Present to allow additional options to be added in the future as a non-breaking change.
    #[derive(Clone, Debug, Default, TypedBuilder)]
    #[non_exhaustive]
    pub struct DropSearchIndexOptions { }
}