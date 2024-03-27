use std::marker::PhantomData;

use bson::Document;

use super::{action_impl, deeplink, option_setters, CollRef, Multiple, Single};
use crate::{
    error::{Error, Result},
    operation,
    search_index::options::{
        CreateSearchIndexOptions,
        DropSearchIndexOptions,
        UpdateSearchIndexOptions,
    },
    Collection,
    SearchIndexModel,
};

impl<T> Collection<T>
where
    T: Send + Sync,
{
    /// Creates multiple search indexes on the collection.
    ///
    /// `await` will return d[`Result<Vec<String>>`].
    #[deeplink]
    pub fn create_search_indexes(
        &self,
        models: impl IntoIterator<Item = SearchIndexModel>,
    ) -> CreateSearchIndex<Multiple> {
        CreateSearchIndex {
            coll: CollRef::new(self),
            models: models.into_iter().collect(),
            options: None,
            _mode: PhantomData,
        }
    }

    /// Convenience method for creating a single search index.
    ///
    /// `await` will return d[`Result<String>`].
    #[deeplink]
    pub fn create_search_index(&self, model: SearchIndexModel) -> CreateSearchIndex<Single> {
        CreateSearchIndex {
            coll: CollRef::new(self),
            models: vec![model],
            options: None,
            _mode: PhantomData,
        }
    }

    /// Updates the search index with the given name to use the provided definition.
    ///
    /// `await` will return [`Result<()>`].
    pub fn update_search_index(
        &self,
        name: impl Into<String>,
        definition: Document,
    ) -> UpdateSearchIndex {
        UpdateSearchIndex {
            coll: CollRef::new(self),
            name: name.into(),
            definition,
            options: None,
        }
    }

    /// Drops the search index with the given name.
    ///
    /// `await` will return [`Result<()>`].
    pub fn drop_search_index(&self, name: impl Into<String>) -> DropSearchIndex {
        DropSearchIndex {
            coll: CollRef::new(self),
            name: name.into(),
            options: None,
        }
    }
}

#[cfg(feature = "sync")]
impl<T> crate::sync::Collection<T>
where
    T: Send + Sync,
{
    /// Creates multiple search indexes on the collection.
    ///
    /// [`run`](CreateSearchIndex::run) will return d[`Result<Vec<String>>`].
    #[deeplink]
    pub fn create_search_indexes(
        &self,
        models: impl IntoIterator<Item = SearchIndexModel>,
    ) -> CreateSearchIndex<Multiple> {
        self.async_collection.create_search_indexes(models)
    }

    /// Convenience method for creating a single search index.
    ///
    /// [`run`](CreateSearchIndex::run) will return d[`Result<String>`].
    #[deeplink]
    pub fn create_search_index(&self, model: SearchIndexModel) -> CreateSearchIndex<Single> {
        self.async_collection.create_search_index(model)
    }

    /// Updates the search index with the given name to use the provided definition.
    ///
    /// [`run`](UpdateSearchIndex::run) will return [`Result<()>`].
    pub fn update_search_index(
        &self,
        name: impl Into<String>,
        definition: Document,
    ) -> UpdateSearchIndex {
        self.async_collection.update_search_index(name, definition)
    }

    /// Drops the search index with the given name.
    ///
    /// `await` will return [`Result<()>`].
    pub fn drop_search_index(&self, name: impl Into<String>) -> DropSearchIndex {
        self.async_collection.drop_search_index(name)
    }
}

/// Create search indexes on a collection.  Construct with [`Collection::create_search_index`] or
/// [`Collection::create_search_indexes`].
#[must_use]
pub struct CreateSearchIndex<'a, Mode> {
    coll: CollRef<'a>,
    models: Vec<SearchIndexModel>,
    options: Option<CreateSearchIndexOptions>,
    _mode: PhantomData<Mode>,
}

impl<'a, Mode> CreateSearchIndex<'a, Mode> {
    option_setters! { options: CreateSearchIndexOptions;
    }
}

action_impl! {
    impl<'a> Action for CreateSearchIndex<'a, Multiple> {
        type Future = CreateSearchIndexesFuture;

        async fn execute(self) -> Result<Vec<String>> {
            let op = operation::CreateSearchIndexes::new(self.coll.namespace(), self.models);
            self.coll.client().execute_operation(op, None).await
        }
    }
}

action_impl! {
    impl<'a> Action for CreateSearchIndex<'a, Single> {
        type Future = CreateSearchIndexFuture;

        async fn execute(self) -> Result<String> {
            let mut names = self.coll
                .create_search_indexes(self.models)
                .with_options(self.options).await?;
            match names.len() {
                1 => Ok(names.pop().unwrap()),
                n => Err(Error::internal(format!("expected 1 index name, got {}", n))),
            }
        }
    }
}

/// Updates a specific search index to use a new definition.  Construct with
/// [`Collection::update_search_index`].
#[must_use]
pub struct UpdateSearchIndex<'a> {
    coll: CollRef<'a>,
    name: String,
    definition: Document,
    options: Option<UpdateSearchIndexOptions>,
}

impl<'a> UpdateSearchIndex<'a> {
    option_setters! { options: UpdateSearchIndexOptions; }
}

action_impl! {
    impl<'a> Action for UpdateSearchIndex<'a> {
        type Future = UpdateSearchIndexFuture;

        async fn execute(self) -> Result<()> {
            let op = operation::UpdateSearchIndex::new(
                self.coll.namespace(),
                self.name,
                self.definition,
            );
            self.coll.client().execute_operation(op, None).await
        }
    }
}

/// Drops a specific search index.  Construct with [`Collection::drop_search_index`].
#[must_use]
pub struct DropSearchIndex<'a> {
    coll: CollRef<'a>,
    name: String,
    options: Option<DropSearchIndexOptions>,
}

impl<'a> DropSearchIndex<'a> {
    option_setters! { options: DropSearchIndexOptions; }
}

action_impl! {
    impl<'a> Action for DropSearchIndex<'a> {
        type Future = DropSearchIndexFuture;

        async fn execute(self) -> Result<()> {
            let op = operation::DropSearchIndex::new(
                self.coll.namespace(),
                self.name,
            );
            self.coll.client().execute_operation(op, None).await
        }
    }
}
