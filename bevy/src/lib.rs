use std::{path::Path, sync::Arc};

use bevy::asset::{
    AssetApp as _,
    io::{AssetReaderError, PathStream, Reader, VecReader},
};
use mongodb::bson::doc;
use tokio::sync::{mpsc, oneshot};

pub struct MongodbAssetPlugin(mpsc::Sender<AssetMessage>);

impl MongodbAssetPlugin {
    pub async fn new(client: &mongodb::Client) -> Self {
        let (tx, rx) = mpsc::channel::<AssetMessage>(1);
        let client = client.clone();
        tokio::spawn(async move {
            let mut rx = rx;
            while let Some(message) = rx.recv().await {
                let client = client.clone();
                tokio::spawn(async move {
                    let _ = message.response.send(message.request.process(client).await);
                });
            }
        });
        Self(tx)
    }
}

enum AssetRequest {
    Read { path: AssetPath, is_meta: bool },
}

impl AssetRequest {
    async fn process(self, client: mongodb::Client) -> AssetResponse {
        match self {
            AssetRequest::Read { path, is_meta } => {
                let coll = client
                    .database(&path.namespace.db)
                    .collection::<mongodb::bson::RawDocumentBuf>(&path.namespace.coll);

                let not_found = |text: &str| -> AssetResponse {
                    Err(Arc::new(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("{} (meta = {}): {}", path, is_meta, text),
                    )))
                };

                let query = doc! { "name": { "$eq": &path.name }, "meta": { "$eq": is_meta } };
                let Some(doc) = coll.find_one(query).await.map_err(mdb_io_error)? else {
                    return not_found("no document found");
                };
                let Some(data) = doc.get("data").map_err(std::io::Error::other)? else {
                    return not_found("no document 'data' field");
                };
                let Some(bin) = data.as_binary() else {
                    return Err(Arc::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "{} (meta = {}): invalid 'data' field, expected binary, got {:?}",
                            path,
                            is_meta,
                            data.element_type()
                        ),
                    )));
                };
                return Ok(bin.bytes.to_owned());
            }
        }
    }
}

fn mdb_io_error(err: mongodb::error::Error) -> Arc<std::io::Error> {
    match *err.kind {
        mongodb::error::ErrorKind::Io(inner) => inner,
        _ => Arc::new(std::io::Error::other(err)),
    }
}

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

type AssetResponse = std::result::Result<Vec<u8>, Arc<std::io::Error>>;

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
        self.0.send(message).await.map_err(std::io::Error::other)?;
        let bytes = response
            .await
            .map_err(std::io::Error::other)?
            .map_err(AssetReaderError::Io)?;
        Ok(VecReader::new(bytes))
    }
}

impl bevy::asset::io::AssetReader for MongodbAssetReader {
    async fn read<'a>(&'a self, path: &'a Path) -> Result<impl Reader + 'a, AssetReaderError> {
        self.send_request(AssetRequest::Read {
            path: AssetPath::parse(path)?,
            is_meta: false,
        })
        .await
    }

    async fn read_meta<'a>(&'a self, path: &'a Path) -> Result<impl Reader + 'a, AssetReaderError> {
        self.send_request(AssetRequest::Read {
            path: AssetPath::parse(path)?,
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

#[cfg(test)]
mod tests {
    use crate::MongodbAssetPlugin;

    #[test]
    fn use_plugin() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let plugin = rt.block_on(async {
            let client = mongodb::Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap();

            MongodbAssetPlugin::new(&client).await
        });

        bevy::app::App::new().add_plugins(plugin).run();
    }
}
