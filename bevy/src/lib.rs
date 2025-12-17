#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

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

#[cfg(test)]
mod tests {
    use std::{cell::LazyCell, num::NonZero, time::Duration};

    use crate::MongodbAssetPlugin;
    use bevy::{
        app::{AppExit, PluginGroup, ScheduleRunnerPlugin},
        asset::{AssetServer, AsyncWriteExt, Handle, LoadState},
        diagnostic::FrameCount,
        ecs::{
            message::MessageWriter,
            resource::Resource,
            system::{Commands, Res},
        },
        image::Image,
    };
    use mongodb::{bson::rawdoc, options::GridFsBucketOptions};

    const MONGODB_URI: LazyCell<String> = LazyCell::new(|| std::env::var("MONGODB_URI").unwrap());

    #[test]
    fn use_plugin() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let plugin = rt.block_on(async {
            let client = mongodb::Client::with_uri_str(&*MONGODB_URI).await.unwrap();

            MongodbAssetPlugin::new(&client).await
        });

        bevy::app::App::new().add_plugins(plugin).run();
    }

    static PNM_IMAGE_DATA: &[u8] = b"P1\n1 1\n0";

    #[test]
    fn load_image_document() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let plugin = rt.block_on(async {
            let client = mongodb::Client::with_uri_str(&*MONGODB_URI).await.unwrap();

            let doc = rawdoc! {
                "name": "pixel.pbm",
                "data": mongodb::bson::Binary {
                    subtype: mongodb::bson::spec::BinarySubtype::Generic,
                    bytes: PNM_IMAGE_DATA.to_owned(),
                },
            };

            let coll = client
                .database("bevy_test")
                .collection::<mongodb::bson::RawDocumentBuf>("doc_images");
            coll.drop().await.unwrap();

            coll.insert_one(doc).await.unwrap();

            MongodbAssetPlugin::new(&client).await
        });

        load_image(plugin, "mongodb://document/bevy_test/doc_images/pixel.pbm");
    }

    #[test]
    fn load_image_gridfs() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let plugin = rt.block_on(async {
            let client = mongodb::Client::with_uri_str(&*MONGODB_URI).await.unwrap();

            let bucket = client.database("bevy_test").gridfs_bucket(
                GridFsBucketOptions::builder()
                    .bucket_name("gridfs_images".to_owned())
                    .build(),
            );
            bucket.drop().await.unwrap();
            let mut upload = bucket.open_upload_stream("pixel.pbm").await.unwrap();
            upload.write_all(PNM_IMAGE_DATA).await.unwrap();
            upload.close().await.unwrap();

            MongodbAssetPlugin::new(&client).await
        });

        load_image(plugin, "mongodb://gridfs/bevy_test/gridfs_images/pixel.pbm");
    }

    fn load_image(plugin: MongodbAssetPlugin, path: &'static str) {
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

        #[derive(Resource)]
        struct TestImage(Handle<Image>);

        let load_image = move |mut commands: Commands, asset_server: Res<AssetServer>| {
            let handle = asset_server.load::<Image>(path);
            commands.insert_resource(TestImage(handle));
        };

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
                    exit_writer.write(AppExit::Success);
                }
                LoadState::Failed(err) => {
                    eprintln!("{err:?}");
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
            // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
            AppExit::Error(code) if code < FAILURE_MAX => unsafe {
                std::mem::transmute::<u8, Failure>(code.into())
            },
            _ => Failure::Max,
        };
        assert_eq!(AppExit::Success, status, "{:?}", failure,);
    }
}
