use mongodb::{Client, Collection};

pub trait Benchmark {
    fn setup(&mut self);

    fn client(&self) -> &Client;

    fn before_task(&self) {}
}
