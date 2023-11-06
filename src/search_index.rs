use crate::{bson::Document, Collection, error::{Error, Result}, coll::options::AggregateOptions, Cursor, operation::{CreateSearchIndexes, UpdateSearchIndex, DropSearchIndex}};
use self::options::*;

use serde::Serialize;
use typed_builder::TypedBuilder;

impl<T> Collection<T> {
    /// Convenience method for creating a single search index.
    pub async fn create_search_index(&self, model: &SearchIndexModel, options: impl Into<Option<CreateSearchIndexOptions>>) -> Result<String> {
        let mut names = self.create_search_indexes(Some(model), options).await?;
        match names.len() {
            1 => Ok(names.pop().unwrap()),
            n => Err(Error::internal(format!("expected 1 index name, got {}", names.len()))),
        }
    }

    /// Creates multiple search indexes on the collection.
    pub async fn create_search_indexes(&self, models: impl IntoIterator<Item=&SearchIndexModel>, _options: impl Into<Option<CreateSearchIndexOptions>>) -> Result<Vec<String>> {
        let op = CreateSearchIndexes::new(self.namespace(), models.into_iter().cloned().collect());
        self.client().execute_operation(op, None).await
    }

    /// Updates the search index with the given name to use the provided definition.
    pub async fn update_search_index(&self, name: &str, definition: &Document, _options: impl Into<Option<UpdateSearchIndexOptions>>) -> Result<()> {
        let op = UpdateSearchIndex::new(self.namespace(), name.to_string(), definition.clone());
        self.client().execute_operation(op, None).await
    }

    /// Drops the search index with the given name.
    pub async fn drop_search_index(&self, name: &str, _options: impl Into<Option<DropSearchIndexOptions>>) -> Result<()> {
        let op = DropSearchIndex::new(self.namespace(), name.to_string());
        self.client().execute_operation(op, None).await
    }

    /// Gets index information for one or more search indexes in the collection.
    ///
    /// If name is not specified, information for all indexes on the specified collection will be returned.
    pub async fn list_search_indexes(&self, name: impl Into<Option<&str>>, aggregation_options: impl Into<Option<AggregateOptions>>, list_index_options: impl Into<Option<ListSearchIndexOptions>>) -> Result<Cursor<Document>> {
        todo!()
    }
}

#[derive(Debug, Clone, Default, TypedBuilder, Serialize)]
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