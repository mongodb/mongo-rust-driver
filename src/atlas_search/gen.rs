use super::*;
#[allow(missing_docs)]
pub struct Autocomplete;
impl AtlasSearch<Autocomplete> {
    /**The autocomplete operator performs a search for a word or phrase that
    contains a sequence of characters from an incomplete input string. The
    fields that you intend to query with the autocomplete operator must be
    indexed with the autocomplete data type in the collection's index definition.
    */
    pub fn autocomplete(path: impl StringOrArray, query: impl AsRef<str>) -> Self {
        AtlasSearch {
            name: "autocomplete",
            stage: doc! {
                "path" : path.to_bson(), "query" : query.as_ref(),
            },
            _t: PhantomData::default(),
        }
    }
    #[allow(missing_docs)]
    pub fn token_order(mut self, token_order: impl AsRef<str>) -> Self {
        self.stage.insert("tokenOrder", token_order.as_ref());
        self
    }
    #[allow(missing_docs)]
    pub fn fuzzy(mut self, fuzzy: Document) -> Self {
        self.stage.insert("fuzzy", fuzzy);
        self
    }
    #[allow(missing_docs)]
    pub fn score(mut self, score: Document) -> Self {
        self.stage.insert("score", score);
        self
    }
}
