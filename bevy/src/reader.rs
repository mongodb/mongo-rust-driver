use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use bevy::asset::{
    AssetApp as _,
    io::{AssetReaderError, PathStream, Reader, VecReader},
};
use mongodb::bson::doc;
use tokio::sync::{mpsc, oneshot};

pub(crate) struct Factory(mpsc::Sender<AssetMessage>);

impl Factory {
    pub(crate) async fn new(client: &mongodb::Client) -> Self {
        let (tx, rx) = mpsc::channel::<AssetMessage>(1);
        let client = client.clone();
        tokio::spawn(async move {
            let mut rx = rx;
            while let Some(message) = rx.recv().await {
                let client = client.clone();
                tokio::spawn(async move {
                    let _ = dbg!(message.response.send(message.request.process(client).await));
                });
            }
        });
        Self(tx)
    }

    pub(crate) fn register(&self, app: &mut bevy::app::App) {
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
        dbg!(self.0.send(message).await).map_err(std::io::Error::other)?;
        let bytes = dbg!(response.await).map_err(std::io::Error::other)??;
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

#[derive(Debug)]
enum AssetRequest {
    Read { path: PathBuf, is_meta: bool },
}

impl AssetRequest {
    async fn process(self, client: mongodb::Client) -> AssetResponse {
        dbg!(&self);
        match self {
            AssetRequest::Read { path, is_meta } => {
                let asset_path =
                    AssetPath::parse(&path).map_err(|e| AssetReaderError::Io(Arc::new(e)))?;

                let coll = client
                    .database(&asset_path.namespace.db)
                    .collection::<mongodb::bson::RawDocumentBuf>(&asset_path.namespace.coll);

                let query =
                    doc! { "name": { "$eq": &asset_path.name }, "meta": { "$eq": is_meta } };
                let Some(doc) = dbg!(coll.find_one(query).await).map_err(mdb_io_error)? else {
                    return Err(AssetReaderError::NotFound(path));
                };
                let bin = doc.get_binary("data").map_err(std::io::Error::other)?;
                return Ok(bin.bytes.to_owned());
            }
        }
    }
}

fn mdb_io_error(err: mongodb::error::Error) -> AssetReaderError {
    AssetReaderError::Io(match *err.kind {
        mongodb::error::ErrorKind::Io(inner) => inner,
        _ => Arc::new(std::io::Error::other(err)),
    })
}

struct AssetMessage {
    request: AssetRequest,
    response: oneshot::Sender<AssetResponse>,
}

impl AssetMessage {
    fn new(request: AssetRequest) -> (Self, oneshot::Receiver<AssetResponse>) {
        let (response, receiver) = oneshot::channel();
        (Self { request, response }, receiver)
    }
}

type AssetResponse = std::result::Result<Vec<u8>, AssetReaderError>;

struct AssetPath {
    namespace: mongodb::Namespace,
    name: String,
}

impl AssetPath {
    fn parse(path: &Path) -> std::io::Result<Self> {
        let Some(parts) = path.iter().map(|p| p.to_str()).collect::<Option<Vec<_>>>() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidFilename,
                "non-utf8 path",
            ));
        };
        return match parts.as_slice() {
            ["document", db, coll, name] => Ok(Self {
                namespace: mongodb::Namespace::new(*db, *coll),
                name: name.to_string(),
            }),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidFilename,
                format!(
                    "expected \"document/<database>/<collection>/<asset>\", got {:?}",
                    path
                ),
            )),
        };
    }
}

impl std::fmt::Display for AssetPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "document/{}/{}/{}",
            self.namespace.db, self.namespace.coll, self.name
        )
    }
}
