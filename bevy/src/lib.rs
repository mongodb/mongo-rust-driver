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
                    let _ = dbg!(message.response.send(message.request.process(client).await));
                });
            }
        });
        Self(tx)
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

type AssetResponse = std::result::Result<Vec<u8>, AssetReaderError>;

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

#[cfg(test)]
mod tests {
    use std::{num::NonZero, time::Duration};

    use crate::MongodbAssetPlugin;
    use bevy::{
        app::{AppExit, PluginGroup, ScheduleRunnerPlugin},
        asset::{AssetServer, Handle, LoadState},
        diagnostic::FrameCount,
        ecs::{
            message::MessageWriter,
            resource::Resource,
            system::{Commands, Res},
        },
        image::Image,
    };
    use mongodb::bson::rawdoc;

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

    #[test]
    fn load_image() {
        static PNM_IMAGE_DATA: &[u8] = &[
            0x50, 0x34, // ASCII "P4", magic number format identifier
            0x01, 0x01, // dimensions: 1x1
            0x00, // pixel value (white)
        ];

        static MAX_FRAMES: u32 = 60 * 60 * 5;

        #[derive(Debug)]
        #[repr(u8)]
        enum Failure {
            NotLoaded = 1u8,
            LoadFailed,
            TimedOut,
            Max,
        }

        static FAILURE_MAX: NonZero<u8> = NonZero::new(Failure::Max as u8).unwrap();

        impl Into<AppExit> for Failure {
            fn into(self) -> AppExit {
                AppExit::Error(NonZero::new(self as u8).unwrap())
            }
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        let plugin = rt.block_on(async {
            let options = mongodb::options::ClientOptions::parse("mongodb://localhost:27017")
                .await
                .unwrap();
            /*
            options.command_event_handler = Some(EventHandler::callback(|cev| {
                dbg!(cev);
            }));
            */
            let client = mongodb::Client::with_options(options).unwrap();

            let doc = rawdoc! {
                "name": "pixel.pbm",
                "data": mongodb::bson::Binary {
                    subtype: mongodb::bson::spec::BinarySubtype::Generic,
                    bytes: PNM_IMAGE_DATA.to_owned(),
                },
                "meta": false,
            };

            client
                .database("bevy_test")
                .collection::<mongodb::bson::RawDocumentBuf>("images")
                .insert_one(doc)
                .await
                .unwrap();

            MongodbAssetPlugin::new(&client).await
        });

        #[derive(Resource)]
        struct TestImage(Handle<Image>);

        fn load_image(mut commands: Commands, asset_server: Res<AssetServer>) {
            let handle =
                asset_server.load::<Image>("mongodb://document/bevy_test/images/pixel.pbm");
            commands.insert_resource(TestImage(handle));
        }

        fn wait_for_image(
            mut exit_writer: MessageWriter<AppExit>,
            image: Res<TestImage>,
            asset_server: Res<AssetServer>,
            frames: Res<FrameCount>,
        ) {
            match asset_server.load_state(&image.0) {
                LoadState::NotLoaded => {
                    exit_writer.write(Failure::NotLoaded.into());
                }
                LoadState::Loading => {
                    if frames.0 > MAX_FRAMES {
                        exit_writer.write(Failure::TimedOut.into());
                    }
                }
                LoadState::Loaded => {
                    dbg!(frames.0);
                    exit_writer.write(AppExit::Success);
                }
                LoadState::Failed(err) => {
                    dbg!(err);
                    exit_writer.write(Failure::LoadFailed.into());
                }
            }
        }

        let status = bevy::app::App::new()
            .add_plugins(plugin)
            .add_plugins(bevy::MinimalPlugins.set(ScheduleRunnerPlugin::run_loop(
                Duration::from_secs_f64(1.0 / 60.0), // run 60 updates per second
            )))
            .add_plugins(bevy::asset::AssetPlugin::default())
            .add_plugins(bevy::render::texture::TexturePlugin)
            .add_plugins(bevy::image::ImagePlugin::default())
            .add_systems(bevy::app::Startup, load_image)
            .add_systems(bevy::app::Update, wait_for_image)
            .run();

        let failure = match status {
            AppExit::Error(code) if code < FAILURE_MAX => unsafe {
                std::mem::transmute::<u8, Failure>(code.into())
            },
            _ => Failure::Max,
        };
        assert_eq!(AppExit::Success, status, "{:?}", failure,);
    }
}
