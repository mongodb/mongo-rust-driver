# Introduction

[![Crates.io](https://img.shields.io/crates/v/mongodb.svg)](https://crates.io/crates/mongodb) [![docs.rs](https://docs.rs/mongodb/badge.svg)](https://docs.rs/mongodb) [![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

This is the manual for the officially supported MongoDB Rust driver, a client side library that can be used to interact with MongoDB deployments in Rust applications. It uses the [`bson`](https://docs.rs/bson/latest) crate for BSON support. The driver contains a fully async API that supports either [`tokio`](https://crates.io/crates/tokio) (default) or [`async-std`](https://crates.io/crates/async-std), depending on the feature flags set. The driver also has a sync API that may be enabled via feature flag.