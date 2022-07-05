# Web Framework Examples

## Actix
The driver can be used easily with the Actix web framework by storing a `Client` in Actix application data. A full example application for using MongoDB with Actix can be found [here](https://github.com/actix/examples/tree/master/databases/mongodb).

## Rocket
The Rocket web framework provides built-in support for MongoDB via the Rust driver. The documentation for the [`rocket_db_pools`](https://api.rocket.rs/v0.5-rc/rocket_db_pools/index.html) crate contains instructions for using MongoDB with your Rocket application.