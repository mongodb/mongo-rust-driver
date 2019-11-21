use std::time::Duration;

use bson::{bson, doc, oid::ObjectId, Bson};

use crate::{
    bson_util,
    cmap::{CommandResponse, StreamDescription},
    coll::options::ReturnDocument,
    operation::{test, FindAndModify, Operation},
    options::{
        FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions,
        UpdateModifications,
    },
    Namespace,
};

// delete tests

fn empty_delete() -> FindAndModify {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let filter = doc! {};
    FindAndModify::with_delete(ns, filter.clone(), None)
}

#[test]
fn build_with_delete() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let filter = doc! { "x": { "$gt": 1 } };
    let max_time = Duration::from_millis(2u64);
    let options = FindOneAndDeleteOptions {
        max_time: Some(max_time),
        ..Default::default()
    };

    let op = FindAndModify::with_delete(ns, filter.clone(), Some(options));

    let description = StreamDescription::new_testing();
    let mut cmd = op.build(&description).unwrap();

    assert_eq!(cmd.name.as_str(), "findAndModify");
    assert_eq!(cmd.target_db.as_str(), "test_db");
    assert_eq!(cmd.read_pref.as_ref(), None);

    let mut expected_body = doc! {
        "findAndModify": "test_coll",
        "query": filter,
        "maxTimeMS": max_time.as_millis() as i64,
        "remove": true
    };

    bson_util::sort_document(&mut cmd.body);
    bson_util::sort_document(&mut expected_body);

    assert_eq!(cmd.body, expected_body);
}

#[test]
fn handle_success_delete() {
    let op = empty_delete();
    let value = doc! {
        "_id" : Bson::ObjectId(ObjectId::new().unwrap()),
        "name" : "Tom",
        "state" : "active",
        "rating" : 100,
        "score" : 5
    };
    let ok_response = CommandResponse::with_document(doc! {
        "lastErrorObject" : {
            "connectionId" : 1,
            "updatedExisting" : true,
            "n" : 1,
            "syncMillis" : 0,
            "writtenTo" : null,
            "err" : null,
            "ok" : 1
         },
        "value" : value.clone(),
        "ok" : 1
    });

    let result = op.handle_response(ok_response);
    assert_eq!(
        result.expect("handle failed").expect("result was None"),
        value
    );
}

#[test]
fn handle_null_value_delete() {
    let op = empty_delete();

    let null_value = CommandResponse::with_document(doc! { "ok": 1.0, "value": Bson::Null});
    let result = op.handle_response(null_value);
    assert!(result.is_ok());
    assert_eq!(result.expect("handle failed"), None);
}

#[test]
fn handle_no_value_delete() {
    let op = empty_delete();

    let no_value = CommandResponse::with_document(doc! { "ok": 1.0 });
    assert!(op.handle_response(no_value).is_err());
}

#[test]
fn handle_command_error_delete() {
    test::handle_command_error(empty_delete())
}

// replace tests

fn empty_replace() -> FindAndModify {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let filter = doc! {};
    let replacement = doc! { "x": { "inc": 1 } };
    FindAndModify::with_replace(ns, filter.clone(), replacement.clone(), None).unwrap()
}

#[test]
fn build_with_replace() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let filter = doc! { "x": { "$gt": 1 } };
    let replacement = doc! { "x": { "inc": 1 } };
    let options = FindOneAndReplaceOptions {
        upsert: Some(false),
        bypass_document_validation: Some(true),
        return_document: Some(ReturnDocument::After),
        ..Default::default()
    };

    let op = FindAndModify::with_replace(ns, filter.clone(), replacement.clone(), Some(options))
        .unwrap();

    let description = StreamDescription::new_testing();
    let mut cmd = op.build(&description).unwrap();

    assert_eq!(cmd.name.as_str(), "findAndModify");
    assert_eq!(cmd.target_db.as_str(), "test_db");
    assert_eq!(cmd.read_pref.as_ref(), None);

    let mut expected_body = doc! {
        "findAndModify": "test_coll",
        "query": filter,
        "update": replacement,
        "upsert": false,
        "bypassDocumentValidation": true,
        "new": true
    };

    bson_util::sort_document(&mut cmd.body);
    bson_util::sort_document(&mut expected_body);

    assert_eq!(cmd.body, expected_body);
}

#[test]
fn handle_success_replace() {
    let op = empty_replace();
    let value = doc! {
        "_id" : Bson::ObjectId(ObjectId::new().unwrap()),
        "name" : "Tom",
        "state" : "active",
        "rating" : 100,
        "score" : 5
    };
    let ok_response = CommandResponse::with_document(doc! {
        "lastErrorObject" : {
            "connectionId" : 1,
            "updatedExisting" : true,
            "n" : 1,
            "syncMillis" : 0,
            "writtenTo" : null,
            "err" : null,
            "ok" : 1
         },
        "value" : value.clone(),
        "ok" : 1
    });

    let result = op.handle_response(ok_response);
    assert_eq!(
        result.expect("handle failed").expect("result was None"),
        value
    );
}

#[test]
fn handle_null_value_replace() {
    let op = empty_replace();

    let null_value = CommandResponse::with_document(doc! { "ok": 1.0, "value": Bson::Null});
    let result = op.handle_response(null_value);
    assert!(result.is_ok());
    assert_eq!(result.expect("handle failed"), None);
}

#[test]
fn handle_no_value_replace() {
    let op = empty_replace();

    let no_value = CommandResponse::with_document(doc! { "ok": 1.0 });
    assert!(op.handle_response(no_value).is_err());
}

#[test]
fn handle_command_error_replace() {
    test::handle_command_error(empty_replace())
}

// update tests

fn empty_update() -> FindAndModify {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let filter = doc! {};
    let update = UpdateModifications::Document(doc! { "$x": { "$inc": 1 } });
    FindAndModify::with_update(ns, filter.clone(), update.clone(), None).unwrap()
}

#[test]
fn build_with_update() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let filter = doc! { "x": { "$gt": 1 } };
    let update = UpdateModifications::Document(doc! { "$x": { "$inc": 1 } });
    let options = FindOneAndUpdateOptions {
        upsert: Some(false),
        bypass_document_validation: Some(true),
        ..Default::default()
    };

    let op = FindAndModify::with_update(ns, filter.clone(), update.clone(), Some(options)).unwrap();

    let description = StreamDescription::new_testing();
    let mut cmd = op.build(&description).unwrap();

    assert_eq!(cmd.name.as_str(), "findAndModify");
    assert_eq!(cmd.target_db.as_str(), "test_db");
    assert_eq!(cmd.read_pref.as_ref(), None);

    let mut expected_body = doc! {
        "findAndModify": "test_coll",
        "query": filter,
        "update": update.to_bson(),
        "upsert": false,
        "bypassDocumentValidation": true
    };

    bson_util::sort_document(&mut cmd.body);
    bson_util::sort_document(&mut expected_body);

    assert_eq!(cmd.body, expected_body);
}

#[test]
fn handle_success_update() {
    let op = empty_update();
    let value = doc! {
        "_id" : Bson::ObjectId(ObjectId::new().unwrap()),
        "name" : "Tom",
        "state" : "active",
        "rating" : 100,
        "score" : 5
    };
    let ok_response = CommandResponse::with_document(doc! {
        "lastErrorObject" : {
            "connectionId" : 1,
            "updatedExisting" : true,
            "n" : 1,
            "syncMillis" : 0,
            "writtenTo" : null,
            "err" : null,
            "ok" : 1
         },
        "value" : value.clone(),
        "ok" : 1
    });

    let result = op.handle_response(ok_response);
    assert_eq!(
        result.expect("handle failed").expect("result was None"),
        value
    );
}

#[test]
fn handle_null_value_update() {
    let op = empty_update();

    let null_value = CommandResponse::with_document(doc! { "ok": 1.0, "value": Bson::Null});
    let result = op.handle_response(null_value);
    assert!(result.is_ok());
    assert_eq!(result.expect("handle failed"), None);
}

#[test]
fn handle_no_value_update() {
    let op = empty_update();

    let no_value = CommandResponse::with_document(doc! { "ok": 1.0 });
    assert!(op.handle_response(no_value).is_err());
}

#[test]
fn handle_command_error_update() {
    test::handle_command_error(empty_update())
}
