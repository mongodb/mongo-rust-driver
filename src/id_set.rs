/// A container that provides removal tokens when an item is added.
#[derive(Debug, Clone)]
pub(crate) struct IdSet<T> {
    values: Vec<Entry<T>>,
    free: Vec<usize>,
}

#[derive(Debug, Clone)]
struct Entry<T> {
    generation: u32,
    value: Option<T>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Id {
    index: usize,
    generation: u32,
}

impl<T> IdSet<T> {
    pub(crate) fn new() -> Self {
        Self {
            values: vec![],
            free: vec![],
        }
    }

    pub(crate) fn insert(&mut self, value: T) -> Id {
        let value = Some(value);
        if let Some(index) = self.free.pop() {
            let generation = self.values[index].generation + 1;
            self.values[index] = Entry { generation, value };
            Id { index, generation }
        } else {
            let generation = 0;
            self.values.push(Entry { generation, value });
            Id {
                index: self.values.len() - 1,
                generation,
            }
        }
    }

    pub(crate) fn remove(&mut self, id: &Id) {
        if let Some(entry) = self.values.get_mut(id.index) {
            if entry.generation == id.generation {
                entry.value = None;
                self.free.push(id.index);
            }
        }
    }

    #[cfg(all(test, mongodb_internal_tracking_arc))]
    pub(crate) fn values(&self) -> impl Iterator<Item = &T> {
        self.values.iter().filter_map(|e| e.value.as_ref())
    }

    pub(crate) fn extract(&mut self) -> Vec<T> {
        self.free.clear();
        self.values.drain(..).filter_map(|e| e.value).collect()
    }
}
