use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use bevy::asset::{
    AssetApp as _,
    io::{AssetReaderError, PathStream, Reader, VecReader},
};
use tokio::sync::{mpsc, oneshot};

pub struct MongodbAssetPlugin(mpsc::Sender<AssetMessage>);

impl MongodbAssetPlugin {
    pub async fn new(client: &mongodb::Client) -> Self {
        let (tx, rx) = mpsc::channel::<AssetMessage>(1);
        let client = client.clone();
        tokio::spawn(async move {
            let mut rx = rx;
            while let Some(message) = rx.recv().await {
                tokio::spawn(message.process(client.clone()));
            }
        });
        Self(tx)
    }
}

enum AssetRequest {
    Read { path: PathBuf, is_meta: bool },
}

type AssetResponse = mongodb::error::Result<Vec<u8>>;

struct AssetMessage {
    request: AssetRequest,
    response: oneshot::Sender<AssetResponse>,
}

impl AssetMessage {
    fn new(request: AssetRequest) -> (Self, oneshot::Receiver<AssetResponse>) {
        let (response, receiver) = oneshot::channel();
        (Self { request, response }, receiver)
    }

    async fn process(self, client: mongodb::Client) {}
}

impl bevy::app::Plugin for MongodbAssetPlugin {
    fn build(&self, app: &mut bevy::app::App) {
        let sender = self.0.clone();
        app.register_asset_source(
            "mongodb",
            bevy::asset::io::AssetSource::build()
                .with_reader(move || Box::new(MongodbAssetReader(sender.clone()))),
        );
    }
}

struct MongodbAssetReader(mpsc::Sender<AssetMessage>);

impl MongodbAssetReader {
    async fn send_request(&self, request: AssetRequest) -> Result<VecReader, AssetReaderError> {
        let (message, response) = AssetMessage::new(request);
        self.0.send(message).await.map_err(io_error)?;
        let bytes = response.await.map_err(io_error)?.map_err(io_error)?;
        Ok(VecReader::new(bytes))
    }
}

impl bevy::asset::io::AssetReader for MongodbAssetReader {
    async fn read<'a>(&'a self, path: &'a Path) -> Result<impl Reader + 'a, AssetReaderError> {
        self.send_request(AssetRequest::Read {
            path: path.to_owned(),
            is_meta: false,
        })
        .await
    }

    async fn read_meta<'a>(&'a self, path: &'a Path) -> Result<impl Reader + 'a, AssetReaderError> {
        self.send_request(AssetRequest::Read {
            path: path.to_owned(),
            is_meta: true,
        })
        .await
    }

    async fn is_directory<'a>(&'a self, _path: &'a Path) -> Result<bool, AssetReaderError> {
        Ok(false)
    }

    async fn read_directory<'a>(
        &'a self,
        path: &'a Path,
    ) -> Result<Box<PathStream>, AssetReaderError> {
        Err(AssetReaderError::NotFound(path.to_owned()))
    }
}

fn io_error<E>(err: E) -> AssetReaderError
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    AssetReaderError::Io(Arc::new(std::io::Error::other(err)))
}
