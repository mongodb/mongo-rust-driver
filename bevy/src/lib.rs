mod reader;

pub struct MongodbAssetPlugin {
    rf: reader::Factory,
}

impl MongodbAssetPlugin {
    pub async fn new(client: &mongodb::Client) -> Self {
        Self {
            rf: reader::Factory::new(client).await,
        }
    }
}

impl bevy::app::Plugin for MongodbAssetPlugin {
    fn build(&self, app: &mut bevy::app::App) {
        self.rf.register(app);
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
        static PNM_IMAGE_DATA: &[u8] = b"P1\n1 1\n0";

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

            let coll = client
                .database("bevy_test")
                .collection::<mongodb::bson::RawDocumentBuf>("images");
            coll.drop().await.unwrap();

            coll.insert_one(doc).await.unwrap();

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
