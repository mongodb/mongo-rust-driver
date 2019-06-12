// #[macro_use]
// extern crate bson;

mod bench;
mod error;

use crate::bench::Benchmark;

fn main() {
    let small_insert_one_test =
        bench::run_benchmark(bench::small_insert_one::InsertOneBenchmark::setup().unwrap())
            .unwrap();

    println!("{:#?}", small_insert_one_test);
}
