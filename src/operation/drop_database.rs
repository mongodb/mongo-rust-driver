use bson::bson;
use bson::doc;

use crate::{
    cmap::{Command, CommandResponse, StreamDescription},
    concern::WriteConcern,
    error::Result,
    operation::{Operation, WriteConcernOnlyBody},
};

#[derive(Debug)]
pub(crate) struct DropDatabase {
    target_db: String,
    write_concern: Option<WriteConcern>,
}

impl DropDatabase {
    #[allow(dead_code)]
    fn empty() -> Self {
        Self {
            target_db: "".to_string(),
            write_concern: None,
        }
    }

    pub(crate) fn new(target_db: String, write_concern: Option<WriteConcern>) -> Self {
        // TODO: use write concern from options ?
        Self {
            target_db,
            write_concern,
        }
    }
}

impl Operation for DropDatabase {
    type O = ();
    const NAME: &'static str = "dropDatabase";

    fn build(&self, description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: 1,
        };
        if let Some(ref wc) = self.write_concern {
            body.insert("writeConcern", wc.to_bson());
        }

        Ok(Command::new(
            Self::NAME.to_string(),
            self.target_db.clone(),
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
        operation::{test, DropDatabase, Operation},
    };

    #[test]
    fn build() {
        let op = DropDatabase {
            target_db: "test_db".to_string(),
            write_concern: Some(WriteConcern {
                w: Some(Acknowledgment::Tag("abc".to_string())),
                ..Default::default()
            }),
        };

        let description = StreamDescription::new_42();
        let cmd = op.build(&description).unwrap();

        assert_eq!(cmd.name.as_str(), "dropDatabase");
        assert_eq!(cmd.target_db.as_str(), "test_db");
        assert_eq!(cmd.read_pref.as_ref(), None);
        assert_eq!(
            cmd.body,
            doc! {
                "dropDatabase": 1,
                "writeConcern": { "w": "abc" }
            }
        );
    }

    #[test]
    fn handle_success() {
        let op = DropDatabase::empty();

        let ok_response = CommandResponse::from_document(doc! { "ok": 1.0 });
        assert!(op.handle_response(ok_response).is_ok());
        let ok_extra = CommandResponse::from_document(doc! { "ok": 1.0, "hello": "world" });
        assert!(op.handle_response(ok_extra).is_ok());
    }

    #[test]
    fn handle_command_error() {
        test::handle_command_error(DropDatabase::empty());
    }

    #[test]
    fn handle_write_concern_error() {
        let op = DropDatabase::empty();

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
