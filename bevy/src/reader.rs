use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use bevy::asset::io::{AssetReaderError, PathStream, Reader, VecReader};
use mongodb::{
    bson::doc,
    error::{ErrorKind as MdbErrorKind, GridFsErrorKind},
    options::GridFsBucketOptions,
};
use tokio::sync::{mpsc, oneshot};

pub(crate) struct Worker(mpsc::Sender<AssetMessage>);

impl Worker {
    pub(crate) async fn new(client: &mongodb::Client) -> Self {
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

    pub(crate) fn asset_source(&self) -> bevy::asset::io::AssetSourceBuilder {
        let sender = self.0.clone();
        bevy::asset::io::AssetSource::build()
            .with_reader(move || Box::new(MongodbAssetReader(sender.clone())))
    }
}

struct MongodbAssetReader(mpsc::Sender<AssetMessage>);

impl MongodbAssetReader {
    async fn send_request(&self, request: AssetRequest) -> Result<VecReader, AssetReaderError> {
        let (message, response) = AssetMessage::new(request);
        self.0.send(message).await.map_err(std::io::Error::other)?;
        let bytes = response.await.map_err(std::io::Error::other)??;
        Ok(VecReader::new(bytes))
    }
}

impl bevy::asset::io::AssetReader for MongodbAssetReader {
    async fn read<'a>(&'a self, path: &'a Path) -> Result<impl Reader + 'a, AssetReaderError> {
        self.send_request(AssetRequest {
            path: path.to_owned(),
            is_meta: false,
        })
        .await
    }

    async fn read_meta<'a>(&'a self, path: &'a Path) -> Result<impl Reader + 'a, AssetReaderError> {
        self.send_request(AssetRequest {
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
struct AssetRequest {
    path: PathBuf,
    is_meta: bool,
}

impl AssetRequest {
    async fn process(self, client: mongodb::Client) -> AssetResponse {
        let asset_path =
            AssetPath::parse(&self.path).map_err(|e| AssetReaderError::Io(Arc::new(e)))?;
        match asset_path {
            AssetPath::Document { namespace, name } => {
                self.process_document(client, namespace, name).await
            }
            AssetPath::GridFs { db, bucket, name } => {
                self.process_gridfs(client, db, bucket, name).await
            }
        }
    }

    async fn process_document(
        self,
        client: mongodb::Client,
        namespace: mongodb::Namespace,
        name: String,
    ) -> AssetResponse {
        let Self { path, is_meta } = self;

        let coll = client
            .database(&namespace.db)
            .collection::<mongodb::bson::RawDocumentBuf>(&namespace.coll);

        let meta_query = if is_meta {
            doc! { "$eq": true }
        } else {
            doc! { "$exists": false }
        };
        let query = doc! { "name": { "$eq": &name }, "meta": meta_query };
        let Some(doc) = coll.find_one(query).await.map_err(mdb_io_error)? else {
            return Err(AssetReaderError::NotFound(path));
        };

        let bin = doc.get_binary("data").map_err(std::io::Error::other)?;
        Ok(bin.bytes.to_owned())
    }

    async fn process_gridfs(
        self,
        client: mongodb::Client,
        db: String,
        bucket: String,
        name: String,
    ) -> AssetResponse {
        use futures_util::io::AsyncReadExt;

        let Self { path, is_meta } = self;

        let bucket = client
            .database(&db)
            .gridfs_bucket(GridFsBucketOptions::builder().bucket_name(bucket).build());

        let map_not_found = |e: mongodb::error::Error| {
            if matches!(
                &*e.kind,
                MdbErrorKind::GridFs(
                    GridFsErrorKind::FileNotFound { .. } | GridFsErrorKind::RevisionNotFound { .. },
                )
            ) {
                AssetReaderError::NotFound(path.clone())
            } else {
                mdb_io_error(e)
            }
        };

        if is_meta {
            let coll_doc = bucket
                .find_one(doc! { "filename": name })
                .await
                .map_err(map_not_found)?
                .ok_or_else(|| AssetReaderError::NotFound(path.clone()))?;
            let gridfs_meta = coll_doc
                .metadata
                .ok_or_else(|| AssetReaderError::NotFound(path.clone()))?;
            let bevy_meta = gridfs_meta
                .get("bevyAsset")
                .ok_or_else(|| AssetReaderError::NotFound(path.clone()))?;
            return match bevy_meta {
                mongodb::bson::Bson::Binary(binary) => Ok(binary.bytes.to_owned()),
                _ => Err(AssetReaderError::Io(Arc::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "{:?}: expected Binary, got {:?}",
                        path,
                        bevy_meta.element_type()
                    ),
                )))),
            };
        }

        let mut stream = bucket
            .open_download_stream_by_name(name)
            .await
            .map_err(map_not_found)?;
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).await?;

        Ok(bytes)
    }
}

fn mdb_io_error(err: mongodb::error::Error) -> AssetReaderError {
    AssetReaderError::Io(match *err.kind {
        MdbErrorKind::Io(inner) => inner,
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

enum AssetPath {
    Document {
        namespace: mongodb::Namespace,
        name: String,
    },
    GridFs {
        db: String,
        bucket: String,
        name: String,
    },
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
            ["document", db, coll, name] => Ok(Self::Document {
                namespace: mongodb::Namespace::new(*db, *coll),
                name: name.to_string(),
            }),
            ["gridfs", db, bucket, name] => Ok(Self::GridFs {
                db: db.to_string(),
                bucket: bucket.to_string(),
                name: name.to_string(),
            }),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidFilename,
                format!(
                    "expected \"document/<database>/<collection>/<asset>\" or \
                     \"gridfs/<database>/<bucket>/<asset>\", got {:?}",
                    path
                ),
            )),
        };
    }
}

impl std::fmt::Display for AssetPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Document { namespace, name } => {
                write!(f, "document/{}/{}/{}", namespace.db, namespace.coll, name)
            }
            Self::GridFs {
                db: database,
                bucket,
                name,
            } => write!(f, "gridfs/{}/{}/{}", database, bucket, name),
        }
    }
}
