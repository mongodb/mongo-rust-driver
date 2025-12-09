use std::path::Path;

use bevy::{
    asset::{
        AssetApp as _,
        io::{AssetReaderError, AsyncSeekForward, PathStream, Reader},
    },
    tasks::futures_lite::AsyncRead,
};

pub struct MongodbAssetPlugin(());

impl bevy::app::Plugin for MongodbAssetPlugin {
    fn build(&self, app: &mut bevy::app::App) {
        app.register_asset_source(
            "mongodb",
            bevy::asset::io::AssetSource::build().with_reader(|| Box::new(MongodbAssetReader)),
        );
    }
}

struct MongodbAssetReader;

impl MongodbAssetReader {
    async fn read_bson<'a>(&'a self, path: &'a Path) -> Result<impl Reader + 'a, AssetReaderError> {
        Ok(BsonReader)
    }
}

impl bevy::asset::io::AssetReader for MongodbAssetReader {
    async fn read<'a>(&'a self, path: &'a Path) -> Result<impl Reader + 'a, AssetReaderError> {
        Ok(BsonReader)
    }

    async fn read_meta<'a>(&'a self, path: &'a Path) -> Result<impl Reader + 'a, AssetReaderError> {
        Ok(BsonReader)
    }

    async fn is_directory<'a>(&'a self, _path: &'a Path) -> Result<bool, AssetReaderError> {
        Ok(false)
    }

    async fn read_directory<'a>(
        &'a self,
        path: &'a Path,
    ) -> Result<Box<PathStream>, AssetReaderError> {
        todo!()
    }
}

struct BsonReader;

impl AsyncRead for BsonReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        todo!()
    }
}

impl AsyncSeekForward for BsonReader {
    fn poll_seek_forward(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        offset: u64,
    ) -> std::task::Poll<Result<u64, bevy::tasks::futures_lite::io::Error>> {
        todo!()
    }
}

impl Reader for BsonReader {}
