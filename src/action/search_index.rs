use std::marker::PhantomData;

use super::{action_impl, deeplink, option_setters, CollRef, Multiple, Single};
use crate::{
    error::{Error, Result},
    operation,
    search_index::options::CreateSearchIndexOptions,
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
    pub fn create_search_indexes_2(
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
    pub fn create_search_index_2(&self, model: SearchIndexModel) -> CreateSearchIndex<Single> {
        CreateSearchIndex {
            coll: CollRef::new(self),
            models: vec![model],
            options: None,
            _mode: PhantomData,
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
    pub fn create_search_indexes_2(
        &self,
        models: impl IntoIterator<Item = SearchIndexModel>,
    ) -> CreateSearchIndex<Multiple> {
        self.async_collection.create_search_indexes_2(models)
    }

    /// Convenience method for creating a single search index.
    ///
    /// [`run`](CreateSearchIndex::run) will return d[`Result<String>`].
    #[deeplink]
    pub fn create_search_index_2(&self, model: SearchIndexModel) -> CreateSearchIndex<Single> {
        self.async_collection.create_search_index_2(model)
    }
}

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
                .create_search_indexes_2(self.models)
                .with_options(self.options).await?;
            match names.len() {
                1 => Ok(names.pop().unwrap()),
                n => Err(Error::internal(format!("expected 1 index name, got {}", n))),
            }
        }
    }
}
