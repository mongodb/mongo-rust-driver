use std::collections::HashMap;

/// A set that provides removal tokens when an item is added.
#[derive(Debug, Clone)]
pub(crate) struct IdSet<T> {
    values: HashMap<u32, T>,
    // Incrementing a counter is not the best source of tokens - it can
    // cause poor hash behavior - but efficiency is not an immediate concern.
    next_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Id(u32);

impl<T> IdSet<T> {
    pub(crate) fn new() -> Self {
        Self {
            values: HashMap::new(),
            next_id: 0,
        }
    }

    pub(crate) fn insert(&mut self, value: T) -> Id {
        let id = self.next_id;
        self.next_id += 1;
        self.values.insert(id, value);
        Id(id)
    }

    pub(crate) fn remove(&mut self, id: &Id) {
        self.values.remove(&id.0);
    }

    #[cfg(all(test, mongodb_internal_tracking_arc))]
    pub(crate) fn values(&self) -> impl Iterator<Item = &T> {
        self.values.values()
    }

    pub(crate) fn extract(&mut self) -> Vec<T> {
        self.values.drain().map(|(_, v)| v).collect()
    }
}
