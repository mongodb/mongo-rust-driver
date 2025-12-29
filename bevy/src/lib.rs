#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

#[cfg(test)]
mod tests;

use bevy::asset::AssetApp;

mod reader;

/// A Bevy plugin to allow reading assets from MongoDB.  See the crate documentation for more
/// information.
pub struct MongodbAssetPlugin {
    read_worker: reader::Worker,
}

impl MongodbAssetPlugin {
    /// Construct a new instance of the plugin.  Must be run in the same Tokio runtime that was used
    /// for construction of the [client][mongodb::Client].
    pub async fn new(client: &mongodb::Client) -> Self {
        Self {
            read_worker: reader::Worker::new(client).await,
        }
    }
}

impl bevy::app::Plugin for MongodbAssetPlugin {
    fn build(&self, app: &mut bevy::app::App) {
        app.register_asset_source("mongodb", self.read_worker.asset_source());
    }
}
