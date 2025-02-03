use std::time::Duration;

use mongodb::bson::Document;
use serde::Serialize;

const SCORE_VALUE_NAME: &str = "score";

#[derive(Debug, Clone)]
pub(crate) struct BenchmarkScore {
    /// The name of the benchmark (e.g. "Large doc bulk insert")
    name: &'static str,

    /// The median of all the sampled timings for this benchmark.
    median_iteration_time: Duration,

    /// MB/s throughput achieved during the median of sampled timings.
    score: f64,
}

impl From<BenchmarkScore> for BenchmarkResult {
    fn from(score: BenchmarkScore) -> BenchmarkResult {
        BenchmarkResult {
            info: BenchmarkInfo {
                test_name: score.name,
                args: Document::new(),
            },
            metrics: vec![
                BenchmarkMetric {
                    name: "median_iteration_time_seconds",
                    value: score.median_iteration_time.as_secs_f64(),
                },
                BenchmarkMetric {
                    name: SCORE_VALUE_NAME,
                    value: score.score,
                },
            ],
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CompositeScore {
    /// The name of the suite of benchmarks (e.g. "Single doc")
    name: &'static str,

    /// List of all the benchmark scores that comprise this suite.
    benchmarks: Vec<BenchmarkScore>,
}

impl std::fmt::Display for CompositeScore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} Score = {:.3} MB/s", self.name, self.avg())
    }
}

impl std::ops::AddAssign<CompositeScore> for CompositeScore {
    fn add_assign(&mut self, mut rhs: CompositeScore) {
        self.benchmarks.append(&mut rhs.benchmarks);
    }
}

impl std::ops::AddAssign<BenchmarkScore> for CompositeScore {
    fn add_assign(&mut self, rhs: BenchmarkScore) {
        self.benchmarks.push(rhs);
    }
}

impl CompositeScore {
    pub(crate) fn new(name: &'static str) -> Self {
        Self {
            name,
            benchmarks: Vec::new(),
        }
    }

    pub(crate) fn count(&self) -> usize {
        self.benchmarks.len()
    }

    pub(crate) fn avg(&self) -> f64 {
        self.benchmarks.iter().map(|b| b.score).sum::<f64>() / self.benchmarks.len() as f64
    }

    /// Return a new `CompositeScore` that contains results only from benchmarks included in the
    /// provided array of benchmark names.
    pub(crate) fn filter(
        &self,
        score_name: &'static str,
        names_to_include: &[&'static str],
    ) -> CompositeScore {
        CompositeScore {
            name: score_name,
            benchmarks: self
                .benchmarks
                .iter()
                .filter(|b| names_to_include.contains(&b.name))
                .cloned()
                .collect(),
        }
    }

    /// Convert this composite result into a single result (e.g. for reporting a composite score).
    pub(crate) fn into_single_result(self) -> BenchmarkResult {
        let score = self.avg();
        BenchmarkResult {
            info: BenchmarkInfo {
                test_name: self.name,
                args: Document::new(),
            },
            metrics: vec![BenchmarkMetric {
                name: SCORE_VALUE_NAME,
                value: score,
            }],
        }
    }

    /// Get a vec of the individual benchmark results.
    pub(crate) fn to_individual_results(&self) -> Vec<BenchmarkResult> {
        self.benchmarks
            .clone()
            .into_iter()
            .map(BenchmarkResult::from)
            .collect()
    }
}

fn get_nth_percentile(durations: &[Duration], n: f64) -> Duration {
    let index = (durations.len() as f64 * (n / 100.0)) as usize;
    durations[std::cmp::max(index, 1) - 1]
}

pub(crate) fn score_test(
    durations: Vec<Duration>,
    name: &'static str,
    task_size: f64,
    more_info: bool,
) -> BenchmarkScore {
    let median = get_nth_percentile(&durations, 50.0);
    let score = task_size / median.as_secs_f64();
    println!(
        "TEST: {} -- Score: {:.3} MB/s, Median Iteration Time: {:.3}s\n",
        name,
        score,
        median.as_secs_f64()
    );

    if more_info {
        println!(
            "10th percentile: {:#?}",
            get_nth_percentile(&durations, 10.0),
        );
        println!(
            "25th percentile: {:#?}",
            get_nth_percentile(&durations, 25.0),
        );
        println!(
            "50th percentile: {:#?}",
            get_nth_percentile(&durations, 50.0),
        );
        println!(
            "75th percentile: {:#?}",
            get_nth_percentile(&durations, 75.0),
        );
        println!(
            "90th percentile: {:#?}",
            get_nth_percentile(&durations, 90.0),
        );
        println!(
            "95th percentile: {:#?}",
            get_nth_percentile(&durations, 95.0),
        );
        println!(
            "98th percentile: {:#?}",
            get_nth_percentile(&durations, 98.0),
        );
        println!(
            "99th percentile: {:#?}\n",
            get_nth_percentile(&durations, 99.0),
        );
    }

    BenchmarkScore {
        name,
        median_iteration_time: median,
        score,
    }
}

/// Struct modeling the JSON format that Evergreen ingests for performance monitoring.
#[derive(Debug, Serialize)]
pub(crate) struct BenchmarkResult {
    info: BenchmarkInfo,
    metrics: Vec<BenchmarkMetric>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BenchmarkInfo {
    test_name: &'static str,
    args: Document,
}

#[derive(Debug, Serialize)]
pub(crate) struct BenchmarkMetric {
    name: &'static str,
    value: f64,
}
