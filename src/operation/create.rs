use bson::bson;
use bson::doc;

use crate::{
    cmap::{Command, CommandResponse, StreamDescription},
    concern::WriteConcern,
    error::Result,
    operation::{append_options, Operation, WriteConcernOnlyBody},
    options::CreateCollectionOptions,
    Namespace,
};

#[derive(Debug)]
pub(crate) struct Create {
    ns: Namespace,
    write_concern: Option<WriteConcern>,
    options: Option<CreateCollectionOptions>,
}

impl Create {
    #[allow(dead_code)]
    fn empty() -> Self {
        Self {
            ns: Namespace {
                db: "".to_string(),
                coll: "".to_string(),
            },
            write_concern: None,
            options: None,
        }
    }

    pub(crate) fn new(
        ns: Namespace,
        db_write_concern: Option<WriteConcern>,
        options: Option<CreateCollectionOptions>,
    ) -> Self {
        Self {
            ns,
            write_concern: db_write_concern, // TODO: first try options wc
            options,
        }
    }
}

impl Operation for Create {
    type O = ();
    const NAME: &'static str = "create";

    fn build(&self, description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
        };
        append_options(&mut body, self.options.as_ref())?;

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
        concern::WriteConcern,
        error::{ErrorKind, WriteFailure},
        operation::{test, Create, Operation},
        options::{CreateCollectionOptions, ValidationAction, ValidationLevel},
        Namespace,
    };

    #[test]
    fn build() {
        let op = Create::new(
            Namespace {
                db: "test_db".to_string(),
                coll: "test_coll".to_string(),
            },
            Some(WriteConcern {
                journal: Some(true),
                ..Default::default()
            }),
            Some(CreateCollectionOptions {
                validation_level: Some(ValidationLevel::Moderate),
                validation_action: Some(ValidationAction::Warn),
                ..Default::default()
            }),
        );

        let description = StreamDescription::new_42();
        let cmd = op.build(&description).unwrap();

        assert_eq!(cmd.name.as_str(), "create");
        assert_eq!(cmd.target_db.as_str(), "test_db");
        assert_eq!(cmd.read_pref.as_ref(), None);
        assert_eq!(
            cmd.body,
            doc! {
                "create": "test_coll",
                "validationLevel": "moderate",
                "validationAction": "warn",
                "writeConcern": { "j": true },
            }
        );
    }

    #[test]
    fn handle_success() {
        let op = Create::empty();

        let ok_response = CommandResponse::from_document(doc! { "ok": 1.0 });
        assert!(op.handle_response(ok_response).is_ok());
        let ok_extra = CommandResponse::from_document(doc! { "ok": 1.0, "hello": "world" });
        assert!(op.handle_response(ok_extra).is_ok());
    }

    #[test]
    fn handle_command_error() {
        test::handle_command_error(Create::empty());
    }

    #[test]
    fn handle_write_concern_error() {
        let op = Create::empty();

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
