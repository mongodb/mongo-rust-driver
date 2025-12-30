use std::{cell::LazyCell, sync::Arc, time::Duration};

use crate::MongodbAssetPlugin;
use bevy::{
    app::{AppExit, PluginGroup, ScheduleRunnerPlugin},
    asset::{AssetLoadError, AssetServer, AsyncWriteExt, Handle, LoadState, meta::AssetMetaDyn},
    diagnostic::FrameCount,
    ecs::{
        message::MessageWriter,
        resource::Resource,
        system::{Commands, Res},
    },
    image::Image,
};
use mongodb::{
    bson::{RawDocumentBuf, doc, rawdoc, spec::BinarySubtype},
    options::GridFsBucketOptions,
};

const MONGODB_URI: LazyCell<String> = LazyCell::new(|| std::env::var("MONGODB_URI").expect("MONGODB_URI environment variable"));

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
        let db = client.database("bevy_test");

        let doc = rawdoc! {
            "name": "pixel.pbm",
            "data": mongodb::bson::Binary {
                subtype: BinarySubtype::Generic,
                bytes: PNM_IMAGE_DATA.to_owned(),
            },
        };
        let coll = db.collection::<RawDocumentBuf>("doc_images");
        coll.drop().await.unwrap();
        coll.insert_one(doc.clone()).await.unwrap();

        let meta_coll = db.collection::<RawDocumentBuf>("doc_images.meta");
        meta_coll.drop().await.unwrap();

        MongodbAssetPlugin::new(&client).await
    });

    let result = test_load_image(plugin, "mongodb://document/bevy_test/doc_images/pixel.pbm");
    assert!(result.is_ok(), "{:?}", result);
}

#[test]
fn load_image_document_with_meta() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let plugin = rt.block_on(async {
        let client = mongodb::Client::with_uri_str(&*MONGODB_URI).await.unwrap();
        let db = client.database("bevy_test");

        let doc = rawdoc! {
            "name": "pixel.pbm",
            "data": mongodb::bson::Binary {
                subtype: BinarySubtype::Generic,
                bytes: PNM_IMAGE_DATA.to_owned(),
            },
        };
        let coll = db.collection::<RawDocumentBuf>("doc_images2");
        coll.drop().await.unwrap();
        coll.insert_one(doc).await.unwrap();

        let meta =
            bevy::asset::meta::AssetMeta::<(), ()>::new(bevy::asset::meta::AssetAction::Ignore);
        let meta_doc = rawdoc! {
            "name": "pixel.pbm",
            "data": mongodb::bson::Binary {
                subtype: BinarySubtype::Generic,
                bytes: meta.serialize(),
            },
        };
        let meta_coll = db.collection::<RawDocumentBuf>("doc_images2.meta");
        meta_coll.drop().await.unwrap();
        meta_coll.insert_one(meta_doc).await.unwrap();

        MongodbAssetPlugin::new(&client).await
    });

    let result = test_load_image(plugin, "mongodb://document/bevy_test/doc_images2/pixel.pbm");
    let Err(ImageLoadFailure::LoadFailed(inner)) = result else {
        panic!("unexpected result: {:?}", result);
    };
    assert!(
        matches!(&*inner, AssetLoadError::CannotLoadIgnoredAsset { .. }),
        "{:?}",
        inner
    );
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

    let result = test_load_image(plugin, "mongodb://gridfs/bevy_test/gridfs_images/pixel.pbm");
    assert!(result.is_ok(), "{:?}", result);
}

#[test]
fn load_image_gridfs_with_meta() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let plugin = rt.block_on(async {
        let client = mongodb::Client::with_uri_str(&*MONGODB_URI).await.unwrap();

        let bucket = client.database("bevy_test").gridfs_bucket(
            GridFsBucketOptions::builder()
                .bucket_name("gridfs_images2".to_owned())
                .build(),
        );
        bucket.drop().await.unwrap();
        let meta =
            bevy::asset::meta::AssetMeta::<(), ()>::new(bevy::asset::meta::AssetAction::Ignore);
        let meta_doc = doc! {
            "bevyAsset": mongodb::bson::Binary {
                subtype: BinarySubtype::Generic,
                bytes: meta.serialize(),
            },
        };
        let mut upload = bucket
            .open_upload_stream("pixel.pbm")
            .metadata(meta_doc)
            .await
            .unwrap();
        upload.write_all(PNM_IMAGE_DATA).await.unwrap();
        upload.close().await.unwrap();

        MongodbAssetPlugin::new(&client).await
    });

    let result = test_load_image(
        plugin,
        "mongodb://gridfs/bevy_test/gridfs_images2/pixel.pbm",
    );
    let Err(ImageLoadFailure::LoadFailed(inner)) = result else {
        panic!("unexpected result: {:?}", result);
    };
    assert!(
        matches!(&*inner, AssetLoadError::CannotLoadIgnoredAsset { .. }),
        "{:?}",
        inner
    );
}

#[derive(Debug)]
enum ImageLoadFailure {
    NotLoaded,
    LoadFailed(Arc<AssetLoadError>),
    TimedOut,
}

fn test_load_image(plugin: MongodbAssetPlugin, path: &'static str) -> Result<(), ImageLoadFailure> {
    static MAX_FRAMES: u32 = 60 * 60 * 5;
    #[derive(Resource)]
    struct TestImage(Handle<Image>);

    let load_images = move |mut commands: Commands, asset_server: Res<AssetServer>| {
        let handle = asset_server.load::<Image>(path);
        commands.insert_resource(TestImage(handle));
    };

    let failure = Arc::new(std::sync::OnceLock::<ImageLoadFailure>::new());

    let wait_for_image = {
        let failure = Arc::clone(&failure);
        move |mut exit_writer: MessageWriter<AppExit>,
              image: Res<TestImage>,
              asset_server: Res<AssetServer>,
              frames: Res<FrameCount>| {
            match asset_server.load_state(&image.0) {
                LoadState::NotLoaded => {
                    failure.set(ImageLoadFailure::NotLoaded).unwrap();
                    exit_writer.write(AppExit::error());
                }
                LoadState::Loading => {
                    if frames.0 > MAX_FRAMES {
                        failure.set(ImageLoadFailure::TimedOut).unwrap();
                        exit_writer.write(AppExit::error());
                    }
                }
                LoadState::Loaded => {
                    exit_writer.write(AppExit::Success);
                }
                LoadState::Failed(err) => {
                    failure.set(ImageLoadFailure::LoadFailed(err)).unwrap();
                    exit_writer.write(AppExit::error());
                }
            }
        }
    };

    let status = bevy::app::App::new()
        .add_plugins(plugin)
        .add_plugins(bevy::MinimalPlugins.set(ScheduleRunnerPlugin::run_loop(
            Duration::from_secs_f64(1.0 / 60.0), // run 60 updates per second
        )))
        .add_plugins(bevy::asset::AssetPlugin::default())
        .add_plugins(bevy::render::texture::TexturePlugin)
        .add_plugins(bevy::image::ImagePlugin::default())
        .add_systems(bevy::app::Startup, load_images)
        .add_systems(bevy::app::Update, wait_for_image)
        .run();

    match status {
        AppExit::Success => Ok(()),
        AppExit::Error(_) => Err(Arc::into_inner(failure).unwrap().take().unwrap()),
    }
}
