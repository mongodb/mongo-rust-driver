use bson::bson;
use bson::doc;

use crate::{
    cmap::{Command, CommandResponse, StreamDescription},
    concern::WriteConcern,
    error::Result,
    operation::{Operation, WriteConcernOnlyBody},
    Namespace,
};

#[derive(Debug)]
pub(crate) struct Drop {
    ns: Namespace,
    write_concern: Option<WriteConcern>,
}

impl Drop {
    pub(crate) fn new(ns: Namespace, coll_write_concern: Option<WriteConcern>) -> Self {
        Self {
            ns,
            write_concern: coll_write_concern, // TODO: check options first
        }
    }

    #[allow(dead_code)]
    fn empty() -> Self {
        Self {
            ns: Namespace {
                db: "".to_string(),
                coll: "".to_string(),
            },
            write_concern: None,
        }
    }
}

impl Operation for Drop {
    type O = ();
    const NAME: &'static str = "drop";

    fn build(&self, description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
        };
        if let Some(ref wc) = self.write_concern {
            body.insert("writeConcern", wc.to_bson());
        }

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(&self, response: CommandResponse) -> Result<Self::O> {
        response.body::<WriteConcernOnlyBody>()?.validate()
    }
}

#[cfg(test)]
mod test {
    use bson::{bson, doc};

    use crate::{
        cmap::{CommandResponse, StreamDescription},
        concern::{Acknowledgment, WriteConcern},
        error::{ErrorKind, WriteFailure},
        operation::{test, Drop, Operation},
        Namespace,
    };

    #[test]
    fn build() {
        let op = Drop::new(
            Namespace {
                db: "test_db".to_string(),
                coll: "test_coll".to_string(),
            },
            Some(WriteConcern {
                w: Some(Acknowledgment::Tag("abc".to_string())),
                ..Default::default()
            }),
        );

        let description = StreamDescription::new_42();
        let cmd = op.build(&description).unwrap();

        assert_eq!(cmd.name.as_str(), "drop");
        assert_eq!(cmd.target_db.as_str(), "test_db");
        assert_eq!(cmd.read_pref.as_ref(), None);
        assert_eq!(
            cmd.body,
            doc! {
                "drop": "test_coll",
                "writeConcern": { "w": "abc" }
            }
        );
    }

    #[test]
    fn handle_success() {
        let op = Drop::empty();

        let ok_response = CommandResponse::from_document(doc! { "ok": 1.0 });
        assert!(op.handle_response(ok_response).is_ok());
        let ok_extra = CommandResponse::from_document(doc! { "ok": 1.0, "hello": "world" });
        assert!(op.handle_response(ok_extra).is_ok());
    }

    #[test]
    fn handle_command_error() {
        test::handle_command_error(Drop::empty());
    }

    #[test]
    fn handle_write_concern_error() {
        let op = Drop::empty();

        let response = CommandResponse::from_document(doc! {
            "writeConcernError": {
                "code": 100,
                "codeName": "hello world",
                "errmsg": "12345"
            },
            "ok": 1
        });

        let result = op.handle_response(response);
        assert!(result.is_err());

        match *result.unwrap_err().kind {
            ErrorKind::WriteError {
                inner: WriteFailure::WriteConcernError(ref wc_err),
            } => {
                assert_eq!(wc_err.code, 100);
                assert_eq!(wc_err.code_name, "hello world");
                assert_eq!(wc_err.message, "12345");
            }
            ref e => panic!("expected write concern error, got {:?}", e),
        }
    }
}
