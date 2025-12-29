# Bevy MongoDB Assets

[![Crates.io](https://img.shields.io/crates/v/bevy_mongodb_asset.svg)](https://crates.io/crates/bevy_mongodb_asset)
[![docs.rs](https://docs.rs/bevy_mongodb_asset/badge.svg)](https://docs.rs/bevy_mongodb_asset)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/mongodb/mongo-rust-driver/blob/main/LICENSE)

Load assets from MongoDB.

## Initialization

The asset plugin requires a `mongodb::Client` and must be constructed in the same `tokio` runtime as that client:
```rust
let tokio_rt = tokio::runtime::Runtime::new().unwrap();
let plugin = tokio_rt.block_on(async {
    let client = mongodb::Client::with_uri_str(<URI>).await.unwrap();
    MongodbAssetPlugin::new(&client).await
});
bevy::app::App::new()
    .add_plugins(plugin)
    ...
    .run();
```

## Usage

Assets can be loaded either from BSON documents or from GridFS.

BSON document assets use the path structure `mongodb://document/<database>/<collection>/<name>`, and fetch data from a BSON object with the shape:
```
{
    name: <String>,
    data: <Binary>
}
```
Bevy metadata is optionally loaded from an object with the same shape in the collection named with an additional `.meta` suffix.

GridFS assets use the path structure `mongodb://gridfs/<database>/<bucket>/<name>`.  Bevy metadata is optionally loaded from the GridFS file metadata document field `bevyAsset`.

## Bevy Compatibility

|Bevy|bevy_mongodb_asset|
|---|---|
|0.17|0.1|
