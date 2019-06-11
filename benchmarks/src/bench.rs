mod small_doc;

use mongodb::{Client, Collection};

pub trait Benchmark {
    fn setup() -> Result<Self>;

    fn before_task(&self) {}

    fn do_task(&self);

    fn after_task(&self) {}

    fn teardown(&self);
}
