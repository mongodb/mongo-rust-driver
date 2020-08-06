use std::time::Duration;

use futures::StreamExt;

use crate::{
    bson::doc,
    change_stream::document::{ChangeStreamEventSource, OperationType, UpdateDescription},
    options::{ChangeStreamOptions, FullDocumentType, UpdateOptions},
    selection_criteria::{ReadPreference, SelectionCriteria},
    test::{util::drop_collection, TestClient, LOCK},
    Namespace,
    RUNTIME,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn empty_change_stream() {
    let _guard = LOCK.run_concurrently().await;

    let client = TestClient::new().await;
    if client.server_version_lt(4, 0) || client.is_standalone() {
        return;
    }
    let pipeline = vec![doc! { "$match": { "x": 3 } }];
    let options = ChangeStreamOptions::builder()
        .selection_criteria(SelectionCriteria::ReadPreference(ReadPreference::Primary))
        .max_await_time(Duration::from_millis(5))
        .batch_size(10)
        .build();
    let mut change_stream = client.watch(pipeline, Some(options)).await.unwrap();
    let result = RUNTIME
        .timeout(Duration::from_millis(5), change_stream.next())
        .await;
    assert!(!result.is_ok());
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn change_stream_insert_one() {
    let _guard = LOCK.run_concurrently().await;

    let ns = Namespace {
        db: function_name!().to_string(),
        coll: function_name!().to_string(),
    };

    let client = TestClient::new().await;
    if client.server_version_lt(4, 0) || client.is_standalone() {
        return;
    }

    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;
    client
        .database(function_name!())
        .create_collection(function_name!(), None)
        .await
        .unwrap();

    let mut change_stream = coll.watch(None, None).await.unwrap();

    let coll_ref = coll.clone();
    RUNTIME.spawn(async move {
        coll_ref
            .insert_one(doc! { "_id": 1, "x": 1 }, None)
            .await
            .unwrap();
    });

    let document = change_stream.next().await.unwrap().unwrap();

    assert_eq!(document.operation_type, OperationType::Insert);
    assert_eq!(document.ns.unwrap(), ChangeStreamEventSource::Namespace(ns));
    assert_eq!(document.to, None);
    assert_eq!(document.document_key, Some(doc! { "_id": 1 }));
    assert_eq!(document.update_description, None);
    assert_eq!(document.full_document, Some(doc! { "_id": 1, "x": 1 }));
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn change_stream_insert_many() {
    let _guard = LOCK.run_concurrently().await;

    let ns = Namespace {
        db: function_name!().to_string(),
        coll: function_name!().to_string(),
    };

    let client = TestClient::new().await;
    if client.server_version_lt(4, 0) || client.is_standalone() {
        return;
    }

    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    client
        .database(function_name!())
        .create_collection(function_name!(), None)
        .await
        .unwrap();

    let mut change_stream = coll.watch(None, None).await.unwrap();

    let coll_ref = coll.clone();
    RUNTIME.spawn(async move {
        let docs = vec![
            doc! { "_id": 1, "title": "1984", "author": "George Orwell" },
            doc! { "_id": 2, "title": "Animal Farm", "author": "George Orwell" },
            doc! { "_id": 3, "title": "The Great Gatsby", "author": "F. Scott Fitzgerald" },
        ];
        coll_ref.insert_many(docs, None).await.unwrap();
    });

    for id in 1..4 {
        let document = change_stream.next().await.unwrap().unwrap();
        assert_eq!(document.operation_type, OperationType::Insert);
        assert_eq!(
            document.ns.unwrap(),
            ChangeStreamEventSource::Namespace(ns.clone())
        );
        assert_eq!(document.document_key, Some(doc! { "_id": id }));
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn change_stream_update_one() {
    let _guard = LOCK.run_concurrently().await;

    let ns = Namespace {
        db: function_name!().to_string(),
        coll: function_name!().to_string(),
    };

    let client = TestClient::new().await;
    if client.server_version_lt(4, 0) || client.is_standalone() {
        return;
    }

    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    client
        .database(function_name!())
        .create_collection(function_name!(), None)
        .await
        .unwrap();

    let mut change_stream = coll.watch(None, None).await.unwrap();

    let coll_ref = coll.clone();
    RUNTIME.spawn(async move {
        let original_doc = doc! { "_id": 1, "x": 1 };
        coll_ref
            .insert_one(original_doc.clone(), None)
            .await
            .unwrap();
        coll_ref
            .update_one(original_doc, doc! { "$set": { "x": 5 } }, None)
            .await
            .unwrap();
    });

    let _insert_document = change_stream.next().await.unwrap().unwrap();
    let update_document = change_stream.next().await.unwrap().unwrap();

    assert_eq!(update_document.operation_type, OperationType::Update);
    assert_eq!(
        update_document.ns.unwrap(),
        ChangeStreamEventSource::Namespace(ns)
    );
    assert_eq!(update_document.to, None);
    assert_eq!(update_document.document_key, Some(doc! { "_id": 1 }));
    assert_eq!(
        update_document.update_description,
        Some(UpdateDescription {
            updated_fields: doc! { "x": 5 },
            removed_fields: Vec::new()
        })
    );
    assert_eq!(update_document.full_document, None);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn change_stream_update_lookup() {
    let _guard = LOCK.run_concurrently().await;

    let client = TestClient::new().await;
    if client.server_version_lt(4, 0) || client.is_standalone() {
        return;
    }

    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    client
        .database(function_name!())
        .create_collection(function_name!(), None)
        .await
        .unwrap();

    let change_stream_options = ChangeStreamOptions::builder()
        .full_document(FullDocumentType::UpdateLookup)
        .build();

    let mut change_stream = coll.watch(None, Some(change_stream_options)).await.unwrap();

    let coll_ref = coll.clone();
    RUNTIME.spawn(async move {
        let original_doc = doc! { "_id": 1, "x": 1 };
        coll_ref
            .insert_one(original_doc.clone(), None)
            .await
            .unwrap();
        coll_ref
            .update_one(original_doc, doc! {"$set": { "x": 5 }}, None)
            .await
            .unwrap();
    });

    let _insert_document = change_stream.next().await.unwrap().unwrap();
    let update_document = change_stream.next().await.unwrap().unwrap();
    assert_eq!(
        update_document.full_document,
        Some(doc! { "_id": 1, "x": 5 })
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn change_stream_upsert() {
    let _guard = LOCK.run_concurrently().await;

    let ns = Namespace {
        db: function_name!().to_string(),
        coll: function_name!().to_string(),
    };

    let client = TestClient::new().await;
    if client.server_version_lt(4, 0) || client.is_standalone() {
        return;
    }

    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    client
        .database(function_name!())
        .create_collection(function_name!(), None)
        .await
        .unwrap();

    let mut change_stream = coll.watch(None, None).await.unwrap();

    let coll_ref = coll.clone();
    RUNTIME.spawn(async move {
        let original_doc = doc! { "_id": 1, "x": 1 };
        coll_ref
            .insert_one(original_doc.clone(), None)
            .await
            .unwrap();
        let filter = doc! { "x": { "$gt": 1 } };
        let options = UpdateOptions {
            upsert: Some(true),
            ..Default::default()
        };
        coll_ref
            .update_one(filter, doc! { "$set": { "_id": 2, "x": 5 } }, options)
            .await
            .unwrap();
    });

    let _insert_document = change_stream.next().await.unwrap().unwrap();
    let upsert_document = change_stream.next().await.unwrap().unwrap();
    assert_eq!(upsert_document.operation_type, OperationType::Insert);
    assert_eq!(
        upsert_document.ns.unwrap(),
        ChangeStreamEventSource::Namespace(ns)
    );
    assert_eq!(upsert_document.to, None);
    assert_eq!(upsert_document.document_key, Some(doc! { "_id": 2 }));
    assert_eq!(upsert_document.update_description, None);
    assert_eq!(
        upsert_document.full_document,
        Some(doc! { "_id": 2, "x": 5 })
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn change_stream_update_many() {
    let _guard = LOCK.run_concurrently().await;

    let ns = Namespace {
        db: function_name!().to_string(),
        coll: function_name!().to_string(),
    };

    let client = TestClient::new().await;
    if client.server_version_lt(4, 0) || client.is_standalone() {
        return;
    }

    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    client
        .database(function_name!())
        .create_collection(function_name!(), None)
        .await
        .unwrap();

    let change_stream_options = ChangeStreamOptions::builder()
        .full_document(FullDocumentType::UpdateLookup)
        .build();

    let mut change_stream = coll.watch(None, Some(change_stream_options)).await.unwrap();

    let coll_ref = coll.clone();
    RUNTIME.spawn(async move {
        let docs = vec![
            doc! { "_id": 1, "x": 1, "y": 1 },
            doc! { "_id": 2, "x": 2, "y": 2 },
        ];
        coll_ref.insert_many(docs, None).await.unwrap();

        let filter = doc! { "x": { "$gt": 0 } };

        coll_ref
            .update_many(filter, doc! { "$set": { "y": 3 } }, None)
            .await
            .unwrap();
    });

    for _i in 0..2 {
        let _insert_document = change_stream.next().await.unwrap().unwrap();
    }

    let first_update = change_stream.next().await.unwrap().unwrap();
    assert_eq!(first_update.operation_type, OperationType::Update);
    assert_eq!(
        first_update.ns.unwrap(),
        ChangeStreamEventSource::Namespace(ns.clone())
    );
    assert_eq!(first_update.document_key, Some(doc! { "_id": 1 }));
    assert_eq!(
        first_update.update_description,
        Some(UpdateDescription {
            updated_fields: doc! { "y": 3 },
            removed_fields: Vec::new()
        })
    );
    assert_eq!(
        first_update.full_document,
        Some(doc! { "_id": 1, "x": 1, "y": 3 })
    );

    let second_update = change_stream.next().await.unwrap().unwrap();
    assert_eq!(second_update.operation_type, OperationType::Update);
    assert_eq!(second_update.operation_type, OperationType::Update);
    assert_eq!(
        second_update.ns.unwrap(),
        ChangeStreamEventSource::Namespace(ns)
    );
    assert_eq!(second_update.document_key, Some(doc! { "_id": 2 }));
    assert_eq!(
        second_update.update_description,
        Some(UpdateDescription {
            updated_fields: doc! { "y": 3 },
            removed_fields: Vec::new()
        })
    );
    assert_eq!(
        second_update.full_document,
        Some(doc! { "_id": 2, "x": 2, "y": 3 })
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn change_stream_replace() {
    let _guard = LOCK.run_concurrently().await;

    let ns = Namespace {
        db: function_name!().to_string(),
        coll: function_name!().to_string(),
    };

    let client = TestClient::new().await;
    if client.server_version_lt(4, 0) || client.is_standalone() {
        return;
    }

    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    client
        .database(function_name!())
        .create_collection(function_name!(), None)
        .await
        .unwrap();

    let change_stream_options = ChangeStreamOptions::builder()
        .full_document(FullDocumentType::UpdateLookup)
        .build();

    let mut change_stream = coll.watch(None, Some(change_stream_options)).await.unwrap();

    let coll_ref = coll.clone();
    RUNTIME.spawn(async move {
        coll_ref
            .insert_one(doc! { "_id": 0, "x": 0 }, None)
            .await
            .unwrap();

        let filter = doc! { "x": 0 };
        coll_ref
            .replace_one(filter, doc! { "x": 1 }, None)
            .await
            .unwrap();
    });

    let _insert_document = change_stream.next().await.unwrap().unwrap();
    let replace_document = change_stream.next().await.unwrap().unwrap();
    assert_eq!(replace_document.operation_type, OperationType::Replace);
    assert_eq!(
        replace_document.ns.unwrap(),
        ChangeStreamEventSource::Namespace(ns.clone())
    );
    assert_eq!(replace_document.to, None);
    assert_eq!(replace_document.document_key, Some(doc! { "_id": 0 }));
    assert_eq!(replace_document.update_description, None);
    assert_eq!(
        replace_document.full_document,
        Some(doc! { "_id": 0, "x": 1 })
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn change_stream_delete() {
    let _guard = LOCK.run_concurrently().await;

    let ns = Namespace {
        db: function_name!().to_string(),
        coll: function_name!().to_string(),
    };

    let client = TestClient::new().await;
    if client.server_version_lt(4, 0) || client.is_standalone() {
        return;
    }

    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    client
        .database(function_name!())
        .create_collection(function_name!(), None)
        .await
        .unwrap();

    let change_stream_options = ChangeStreamOptions::builder()
        .full_document(FullDocumentType::UpdateLookup)
        .build();

    let mut change_stream = coll.watch(None, Some(change_stream_options)).await.unwrap();

    let coll_ref = coll.clone();
    RUNTIME.spawn(async move {
        coll_ref
            .insert_one(doc! { "_id": 0, "x": 0 }, None)
            .await
            .unwrap();

        let filter = doc! { "x": 0 };
        coll_ref.delete_one(filter, None).await.unwrap();
    });

    let _insert_document = change_stream.next().await.unwrap().unwrap();
    let delete_document = change_stream.next().await.unwrap().unwrap();
    assert_eq!(delete_document.operation_type, OperationType::Delete);
    assert_eq!(
        delete_document.ns.unwrap(),
        ChangeStreamEventSource::Namespace(ns.clone())
    );
    assert_eq!(delete_document.to, None);
    assert_eq!(delete_document.document_key, Some(doc! { "_id": 0 }));
    assert_eq!(delete_document.update_description, None);
    assert_eq!(delete_document.full_document, None);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn change_stream_drop() {
    let _guard = LOCK.run_concurrently().await;

    let ns = Namespace {
        db: function_name!().to_string(),
        coll: function_name!().to_string(),
    };

    let client = TestClient::new().await;
    if client.server_version_lt(4, 0) || client.is_standalone() {
        return;
    }

    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    client
        .database(function_name!())
        .create_collection(function_name!(), None)
        .await
        .unwrap();

    let change_stream_options = ChangeStreamOptions::builder()
        .full_document(FullDocumentType::UpdateLookup)
        .build();

    let mut change_stream = coll.watch(None, Some(change_stream_options)).await.unwrap();

    let coll_ref = coll.clone();
    RUNTIME.spawn(async move {
        drop_collection(&coll_ref).await;
    });

    let drop_document = change_stream.next().await.unwrap().unwrap();
    assert_eq!(drop_document.operation_type, OperationType::Drop);
    assert_eq!(
        drop_document.ns.unwrap(),
        ChangeStreamEventSource::Namespace(ns.clone())
    );
    assert_eq!(drop_document.to, None);
    assert_eq!(drop_document.document_key, None);
    assert_eq!(drop_document.update_description, None);
    assert_eq!(drop_document.full_document, None);

    let invalidate_document = change_stream.next().await.unwrap().unwrap();
    assert_eq!(
        invalidate_document.operation_type,
        OperationType::Invalidate
    );
    assert_eq!(invalidate_document.ns, None);
    assert_eq!(invalidate_document.to, None);
    assert_eq!(invalidate_document.document_key, None);
    assert_eq!(invalidate_document.update_description, None);
    assert_eq!(invalidate_document.full_document, None);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn change_stream_rename() {
    let _guard = LOCK.run_concurrently().await;

    let ns = Namespace {
        db: function_name!().to_string(),
        coll: function_name!().to_string(),
    };

    let to = Namespace {
        db: function_name!().to_string(),
        coll: "new_coll".to_string(),
    };

    let client = TestClient::new().await;
    if client.server_version_lt(4, 0) || client.is_standalone() {
        return;
    }

    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    client.init_db_and_coll(function_name!(), "new_coll").await;

    client
        .database(function_name!())
        .create_collection(function_name!(), None)
        .await
        .unwrap();

    let mut change_stream = coll.watch(None, None).await.unwrap();

    RUNTIME.spawn(async move {
        client
            .database("admin")
            .run_command(
                doc! {
                    "renameCollection": format!("{}.{}", function_name!().to_string(), function_name!().to_string()),
                    "to": format!("{}.{}", function_name!().to_string(), "new_coll".to_string()),
                },
                None,
            )
            .await
            .unwrap();
    });

    let rename_document = change_stream.next().await.unwrap().unwrap();
    assert_eq!(rename_document.operation_type, OperationType::Rename);
    assert_eq!(
        rename_document.ns.unwrap(),
        ChangeStreamEventSource::Namespace(ns.clone())
    );
    assert_eq!(rename_document.to.unwrap(), to.clone());
    assert_eq!(rename_document.document_key, None);
    assert_eq!(rename_document.update_description, None);
    assert_eq!(rename_document.full_document, None);

    let invalidate_document = change_stream.next().await.unwrap().unwrap();
    assert_eq!(
        invalidate_document.operation_type,
        OperationType::Invalidate
    );
    assert_eq!(invalidate_document.ns, None);
    assert_eq!(invalidate_document.to, None);
    assert_eq!(invalidate_document.document_key, None);
    assert_eq!(invalidate_document.update_description, None);
    assert_eq!(invalidate_document.full_document, None);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn change_stream_drop_database() {
    let _guard = LOCK.run_concurrently().await;

    let ns = Namespace {
        db: function_name!().to_string(),
        coll: function_name!().to_string(),
    };

    let client = TestClient::new().await;
    if client.server_version_lt(4, 0) || client.is_standalone() {
        return;
    }
    let db = client.database(function_name!());
    client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    client
        .database(function_name!())
        .create_collection(function_name!(), None)
        .await
        .unwrap();

    let mut change_stream = db.watch(None, None).await.unwrap();

    RUNTIME.spawn(async move {
        client.database(function_name!()).drop(None).await.unwrap();
    });

    let drop_document = change_stream.next().await.unwrap().unwrap();
    assert_eq!(drop_document.operation_type, OperationType::Drop);
    assert_eq!(
        drop_document.ns.unwrap(),
        ChangeStreamEventSource::Namespace(ns.clone())
    );
    assert_eq!(drop_document.to, None);
    assert_eq!(drop_document.document_key, None);
    assert_eq!(drop_document.update_description, None);
    assert_eq!(drop_document.full_document, None);

    let drop_database_document = change_stream.next().await.unwrap().unwrap();
    assert_eq!(
        drop_database_document.operation_type,
        OperationType::DropDatabase
    );
    assert_eq!(
        drop_database_document.ns.unwrap(),
        ChangeStreamEventSource::Database(function_name!().to_string())
    );
    assert_eq!(drop_database_document.to, None);
    assert_eq!(drop_database_document.document_key, None);
    assert_eq!(drop_database_document.update_description, None);
    assert_eq!(drop_database_document.full_document, None);

    let invalidate_document = change_stream.next().await.unwrap().unwrap();
    assert_eq!(
        invalidate_document.operation_type,
        OperationType::Invalidate
    );
    assert_eq!(invalidate_document.ns, None);
    assert_eq!(invalidate_document.to, None);
    assert_eq!(invalidate_document.document_key, None);
    assert_eq!(invalidate_document.update_description, None);
    assert_eq!(invalidate_document.full_document, None);
}
