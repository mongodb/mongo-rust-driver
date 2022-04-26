use bson::{doc, Document, Timestamp};
use serde::Deserialize;

use crate::{
    client::ClusterTime,
    cmap::{RawCommandResponse, StreamDescription},
    error::{Result, TRANSIENT_TRANSACTION_ERROR},
    operation::{CommandErrorBody, CommandResponse, Operation},
    options::{ReadPreference, SelectionCriteria},
};

pub(crate) fn handle_response_test<T: Operation>(op: &T, response_doc: Document) -> Result<T::O> {
    let raw = RawCommandResponse::with_document(response_doc).unwrap();
    op.handle_response(raw, &StreamDescription::new_testing())
}

pub(crate) fn op_selection_criteria<F, T>(constructor: F)
where
    T: Operation,
    F: Fn(Option<SelectionCriteria>) -> T,
{
    let op = constructor(None);
    assert_eq!(op.selection_criteria(), None);

    let read_pref: SelectionCriteria = ReadPreference::Secondary {
        options: Default::default(),
    }
    .into();

    let op = constructor(Some(read_pref.clone()));
    assert_eq!(op.selection_criteria(), Some(&read_pref));
}

#[test]
fn response_success() {
    let cluster_timestamp = Timestamp {
        time: 123,
        increment: 345,
    };
    let doc = doc! {
        "ok": 1,
        "some": "field",
        "other": true,
        "$clusterTime": {
            "clusterTime": cluster_timestamp,
            "signature": {}
        }
    };
    let raw = RawCommandResponse::with_document(doc.clone()).unwrap();
    let response: CommandResponse<Document> = raw.body().unwrap();

    assert!(response.is_success());
    assert_eq!(
        response.cluster_time(),
        Some(&ClusterTime {
            cluster_time: cluster_timestamp,
            signature: doc! {},
        })
    );
    assert_eq!(response.body, doc! { "some": "field", "other": true });

    #[derive(Deserialize, Debug, PartialEq)]
    struct Body {
        some: String,
        #[serde(rename = "other")]
        o: bool,
        #[serde(default)]
        default: Option<i32>,
    }

    let raw = RawCommandResponse::with_document(doc).unwrap();
    let response: CommandResponse<Body> = raw.body().unwrap();

    assert!(response.is_success());
    assert_eq!(
        response.cluster_time(),
        Some(&ClusterTime {
            cluster_time: cluster_timestamp,
            signature: doc! {},
        })
    );
    assert_eq!(
        response.body,
        Body {
            some: "field".to_string(),
            o: true,
            default: None,
        }
    );
}

#[test]
fn response_failure() {
    let cluster_timestamp = Timestamp {
        time: 123,
        increment: 345,
    };
    let doc = doc! {
        "ok": 0,
        "code": 123,
        "codeName": "name",
        "errmsg": "some message",
        "errorLabels": [TRANSIENT_TRANSACTION_ERROR],
        "$clusterTime": {
            "clusterTime": cluster_timestamp,
            "signature": {}
        }
    };
    let raw = RawCommandResponse::with_document(doc.clone()).unwrap();
    let response: CommandResponse<Document> = raw.body().unwrap();

    assert!(!response.is_success());
    assert_eq!(
        response.cluster_time(),
        Some(&ClusterTime {
            cluster_time: cluster_timestamp,
            signature: doc! {},
        })
    );
    assert_eq!(
        response.body,
        doc! {
            "code": 123,
            "codeName": "name",
            "errmsg": "some message",
            "errorLabels": [TRANSIENT_TRANSACTION_ERROR],
        }
    );

    let raw = RawCommandResponse::with_document(doc).unwrap();
    let response: CommandResponse<CommandErrorBody> = raw.body().unwrap();

    assert!(!response.is_success());
    assert_eq!(
        response.cluster_time(),
        Some(&ClusterTime {
            cluster_time: cluster_timestamp,
            signature: doc! {},
        })
    );
    let command_error = response.body;
    assert_eq!(command_error.command_error.code, 123);
    assert_eq!(command_error.command_error.code_name, "name");
    assert_eq!(command_error.command_error.message, "some message");
    assert_eq!(
        command_error.error_labels,
        Some(vec![TRANSIENT_TRANSACTION_ERROR.to_string()])
    );
}
