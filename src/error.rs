use std::fmt;

use bson::{Bson, Document};

error_chain! {
    foreign_links {
        BsonOid(bson::oid::Error);
        BsonDecode(bson::DecoderError);
        BsonEncode(bson::EncoderError);
        Io(std::io::Error);
        R2D2(r2d2::Error);
    }

    errors {
        /// A malformed or invalid argument was passed to the driver.
        ArgumentError(msg: String) {
            description("An invalid argument was provided to a database operation")
            display("An invalid argument was provided to a database operation: {}", msg)
        }

        BsonConversionError(msg: String) {
            description("Unable to convert generic BSON value into the desired type")
            display("Unable to convert generic BSON value into the desired type: {}", msg)
        }

        /// The server encountered an error when executing the operation.
        CommandError(inner: CommandError) {
            description("An error occurred when executing a command")
            display("Command failed ({}): {}",
                inner.code_name
                    .clone()
                    .unwrap_or_else(|| format!("{}", inner.code)),
                inner.message)
        }

        /// The driver was unable to send or receive a message to the server.
        InvalidHostname(hostname: String) {
            description("Unable to parse hostname")
            display("Unable to parse hostname: '{}'", hostname)
        }

        OperationError(msg: String) {
            description("A database operation failed to send or receive a reply")
            display("A database operation failed to send or receive a reply: {}", msg)
        }

       /// The response the driver received from the server was not in the form expected.
        ParseError(data_type: String, file_path: String) {
            description("Unable to parse data from file")
            display("Unable to parse {} data from {}", data_type, file_path)
        }

        ResponseError(msg: String) {
            description("A database operation returned an invalid reply")
            display("A database operation returned an invalid reply: {}", msg)
        }

        /// An error occurred during server selection.
        ServerError(operation: String, msg: String) {
            description("An attempted database operation failed")
            display("{} operation failed: {}", operation, msg)
        }

        ServerSelectionError(msg: String) {
            description("An error occurred during server selection")
            display("An error occurred during server selection: {}", msg)
        }

        /// An error occurred when trying to execute a write operation.
        WriteError(inner: WriteFailure) {
            description("An error occurred when trying to execute a write operation:")
            display("{}", inner)
        }

        AuthenticationError(msg: String) {
            description("An error occurred when trying to authenticate a connection.")
            display("{}", msg)
        }
    }
}

impl Error {
    pub(crate) fn from_command_response(mut response: Document) -> Result<Document> {
        if let Some(ok_bson) = response.get("ok") {
            let ok = crate::bson_util::get_int(&ok_bson);

            if ok.is_some() && ok != Some(1) {
                if let Some(Bson::String(message)) = response.remove("errmsg") {
                    if let Some(code_bson) = response.get("code") {
                        if let Some(code) = crate::bson_util::get_int(code_bson) {
                            let mut error = CommandError {
                                code: code as i32,
                                code_name: None,
                                message,
                                labels: Vec::new(),
                            };

                            if let Some(Bson::String(code_name)) = response.remove("codeName") {
                                error.code_name = Some(code_name);
                            }

                            if let Some(Bson::Array(labels)) = response.remove("errorLabels") {
                                error.labels.extend(
                                    labels
                                        .into_iter()
                                        .filter_map(|label| label.as_str().map(|s| s.to_string())),
                                );
                            }

                            return Err(Error::from_kind(ErrorKind::CommandError(error)));
                        }
                    }
                }
            }
        }

        Ok(response)
    }

    /// Creates an `AuthenticationError` for the given mechanism with the provided reason.
    pub(crate) fn authentication_error(mechanism_name: &str, reason: &str) -> Self {
        ErrorKind::AuthenticationError(format!("{} failure: {}", mechanism_name, reason)).into()
    }

    /// Creates an `AuthenticationError` for the given mechanism with a generic "unknown" message.
    pub(crate) fn unknown_authentication_error(mechanism_name: &str) -> Error {
        Error::authentication_error(mechanism_name, "internal error")
    }

    /// Creates an `AuthenticationError` for the given mechanism when the server response is
    /// invalid.
    pub(crate) fn invalid_authentication_response(mechanism_name: &str) -> Error {
        Error::authentication_error(mechanism_name, "invalid server response")
    }
}

/// The server encountered an error when executing the operation.
#[derive(Clone, Debug)]
pub struct CommandError {
    /// Identifies the type of the error.
    pub code: i32,

    /// The name associated with the error code.
    ///
    /// Note that the server will not return this in some cases, hence `code_name` being an
    /// `Option`.
    pub code_name: Option<String>,

    /// A description of the error that occurred.
    pub message: String,

    /// A set of labels describing general categories that the error falls into.
    pub labels: Vec<String>,
}

/// An error that occurred due to not being able to satisfy a write concern.
#[derive(Clone, Debug)]
pub struct WriteConcernError {
    /// Identifies the type of write concern error.
    pub code: i32,

    /// The name associated with the error code.
    pub code_name: String,

    /// A description of the error that occurred.
    pub message: String,
}

/// An error that occurred during a write operation that wasn't due to being unable to satisfy a
/// write concern.
#[derive(Clone, Debug)]
pub struct WriteError {
    /// Identifies the type of write concern error.
    pub code: i32,

    /// The name associated with the error code.
    ///
    /// Note that the server will not return this in some cases, hence `code_name` being an
    /// `Option`.
    pub code_name: Option<String>,

    /// A description of the error that occurred.
    pub message: String,
}

/// An error that occurred when trying to execute a write operation.
#[derive(Clone, Debug)]
pub enum WriteFailure {
    WriteConcernError(WriteConcernError),
    WriteError(WriteError),
}

impl fmt::Display for WriteFailure {
    fn fmt(&self, _fmt: &mut fmt::Formatter) -> fmt::Result {
        unimplemented!()
    }
}
