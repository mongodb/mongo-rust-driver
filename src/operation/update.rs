use bson::{bson, doc, Bson, Document};
use serde::Deserialize;

use crate::{
    bson_util,
    cmap::{Command, CommandResponse, StreamDescription},
    concern::WriteConcern,
    error::{convert_bulk_errors, Result},
    operation::{Operation, WriteResponseBody},
    options::{UpdateModifications, UpdateOptions},
    results::UpdateResult,
    Namespace,
};

#[derive(Debug)]
pub(crate) struct Update {
    ns: Namespace,
    filter: Document,
    update: UpdateModifications,
    multi: Option<bool>,
    write_concern: Option<WriteConcern>,
    options: Option<UpdateOptions>,
}

impl Update {
    #[allow(dead_code)]
    fn empty() -> Self {
        Update {
            ns: Namespace {
                db: "".to_string(),
                coll: "".to_string(),
            },
            filter: Document::new(),
            update: UpdateModifications::Document(Document::new()),
            multi: None,
            write_concern: None,
            options: None,
        }
    }

    pub(crate) fn new(
        ns: Namespace,
        filter: Document,
        update: UpdateModifications,
        multi: bool,
        coll_write_concern: Option<WriteConcern>,
        options: Option<UpdateOptions>,
    ) -> Self {
        Self {
            ns,
            filter,
            update,
            multi: if multi { Some(true) } else { None },
            write_concern: coll_write_concern, // TODO: check wc from options
            options,
        }
    }
}

impl Operation for Update {
    type O = UpdateResult;
    const NAME: &'static str = "update";

    fn build(&self, description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
        };

        let mut update = doc! {
            "q": self.filter.clone(),
            "u": self.update.to_bson(),
        };

        if let Some(ref options) = self.options {
            if let Some(upsert) = options.upsert {
                update.insert("upsert", upsert);
            }

            if let Some(ref array_filters) = options.array_filters {
                update.insert("arrayFilters", bson_util::to_bson_array(array_filters));
            }

            if let Some(ref hint) = options.hint {
                update.insert("hint", hint.to_bson());
            }

            if let Some(bypass_doc_validation) = options.bypass_document_validation {
                body.insert("bypassDocumentValidation", bypass_doc_validation);
            }
        };

        if let Some(multi) = self.multi {
            update.insert("multi", multi);
        }

        if let Some(ref write_concern) = self.write_concern {
            body.insert("writeConcern", write_concern.to_bson());
        }

        body.insert("updates", vec![Bson::Document(update)]);

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(&self, response: CommandResponse) -> Result<Self::O> {
        let body = response.body::<WriteResponseBody<UpdateBody>>()?;
        body.validate().map_err(convert_bulk_errors)?;

        let matched_count = body.n;
        let modified_count = body.n_modified;
        let upserted_id = body
            .upserted
            .as_ref()
            .and_then(|v| v.first())
            .and_then(|doc| doc.get("_id"))
            .map(Clone::clone);
        Ok(UpdateResult {
            matched_count,
            modified_count,
            upserted_id,
        })
    }
}

#[derive(Deserialize)]
struct UpdateBody {
    #[serde(rename = "nModified")]
    n_modified: i64,
    upserted: Option<Vec<Document>>,
}

#[cfg(test)]
mod test {
    use bson::{bson, doc, Bson};

    use crate::{
        bson_util,
        cmap::{CommandResponse, StreamDescription},
        concern::{Acknowledgment, WriteConcern},
        error::{ErrorKind, WriteConcernError, WriteError, WriteFailure},
        operation::{test, Operation, Update},
        options::{UpdateModifications, UpdateOptions},
        Namespace,
    };

    #[test]
    fn build() {
        let ns = Namespace {
            db: "test_db".to_string(),
            coll: "test_coll".to_string(),
        };
        let filter = doc! { "x": { "$gt": 1 } };
        let update = UpdateModifications::Document(doc! { "x": { "$inc": 1 } });
        let wc = WriteConcern {
            w: Some(Acknowledgment::Majority),
            ..Default::default()
        };
        let options = UpdateOptions {
            upsert: Some(false),
            bypass_document_validation: Some(true),
            ..Default::default()
        };

        let op = Update::new(
            ns,
            filter.clone(),
            update.clone(),
            false,
            Some(wc),
            Some(options),
        );

        let description = StreamDescription::new_42();
        let mut cmd = op.build(&description).unwrap();

        assert_eq!(cmd.name.as_str(), "update");
        assert_eq!(cmd.target_db.as_str(), "test_db");
        assert_eq!(cmd.read_pref.as_ref(), None);

        let mut expected_body = doc! {
            "update": "test_coll",
            "updates": [
                {
                    "q": filter,
                    "u": update.to_bson(),
                    "upsert": false,
                }
            ],
            "writeConcern": {
                "w": "majority"
            },
            "bypassDocumentValidation": true
        };

        bson_util::sort_document(&mut cmd.body);
        bson_util::sort_document(&mut expected_body);

        assert_eq!(cmd.body, expected_body);
    }

    #[test]
    fn build_many() {
        let ns = Namespace {
            db: "test_db".to_string(),
            coll: "test_coll".to_string(),
        };
        let filter = doc! { "x": { "$gt": 1 } };
        let update = UpdateModifications::Document(doc! { "x": { "$inc": 1 } });

        let op = Update::new(ns, filter.clone(), update.clone(), true, None, None);

        let description = StreamDescription::new_42();
        let mut cmd = op.build(&description).unwrap();

        assert_eq!(cmd.name.as_str(), "update");
        assert_eq!(cmd.target_db.as_str(), "test_db");
        assert_eq!(cmd.read_pref.as_ref(), None);

        let mut expected_body = doc! {
            "update": "test_coll",
            "updates": [
                {
                    "q": filter,
                    "u": update.to_bson(),
                    "multi": true,
                }
            ],
        };

        bson_util::sort_document(&mut cmd.body);
        bson_util::sort_document(&mut expected_body);

        assert_eq!(cmd.body, expected_body);
    }

    #[test]
    fn handle_success() {
        let op = Update::empty();

        let ok_response = CommandResponse::from_document(doc! {
            "ok": 1.0,
            "n": 3,
            "nModified": 1,
            "upserted": [
                { "index": 0, "_id": 1 }
            ]
        });

        let ok_result = op.handle_response(ok_response);
        assert!(ok_result.is_ok());

        let update_result = ok_result.unwrap();
        assert_eq!(update_result.matched_count, 3);
        assert_eq!(update_result.modified_count, 1);
        assert_eq!(update_result.upserted_id, Some(Bson::I32(1)));
    }

    #[test]
    fn handle_success_no_upsert() {
        let op = Update::empty();

        let ok_response = CommandResponse::from_document(doc! {
            "ok": 1.0,
            "n": 5,
            "nModified": 2
        });

        let ok_result = op.handle_response(ok_response);
        assert!(ok_result.is_ok());

        let update_result = ok_result.unwrap();
        assert_eq!(update_result.matched_count, 5);
        assert_eq!(update_result.modified_count, 2);
        assert_eq!(update_result.upserted_id, None);
    }

    #[test]
    fn handle_invalid_response() {
        let op = Update::empty();

        let invalid_response =
            CommandResponse::from_document(doc! { "ok": 1.0, "asdfadsf": 123123 });
        assert!(op.handle_response(invalid_response).is_err());
    }

    #[test]
    fn handle_command_error() {
        test::handle_command_error(Update::empty())
    }

    #[test]
    fn handle_write_failure() {
        let op = Update::empty();

        let write_error_response = CommandResponse::from_document(doc! {
            "ok": 1.0,
            "n": 12,
            "nModified": 0,
            "writeErrors": [
                {
                    "index": 0,
                    "code": 1234,
                    "errmsg": "my error string"
                }
            ]
        });
        let write_error_result = op.handle_response(write_error_response);
        assert!(write_error_result.is_err());
        match *write_error_result.unwrap_err().kind {
            ErrorKind::WriteError {
                inner: WriteFailure::WriteError(ref error),
            } => {
                let expected_err = WriteError {
                    code: 1234,
                    code_name: None,
                    message: "my error string".to_string(),
                };
                assert_eq!(error, &expected_err);
            }
            ref e => panic!("expected write error, got {:?}", e),
        };
    }

    #[test]
    fn handle_write_concern_failure() {
        let op = Update::empty();

        let wc_error_response = CommandResponse::from_document(doc! {
            "ok": 1.0,
            "n": 0,
            "nModified": 0,
            "writeConcernError": {
                "code": 456,
                "codeName": "wcError",
                "errmsg": "some message"
            }
        });

        let wc_error_result = op.handle_response(wc_error_response);
        assert!(wc_error_result.is_err());

        match *wc_error_result.unwrap_err().kind {
            ErrorKind::WriteError {
                inner: WriteFailure::WriteConcernError(ref wc_error),
            } => {
                let expected_wc_err = WriteConcernError {
                    code: 456,
                    code_name: "wcError".to_string(),
                    message: "some message".to_string(),
                };
                assert_eq!(wc_error, &expected_wc_err);
            }
            ref e => panic!("expected write concern error, got {:?}", e),
        }
    }
}
