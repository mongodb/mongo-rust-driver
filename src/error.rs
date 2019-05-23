error_chain! {
    foreign_links {
        BsonOid(bson::oid::Error);
        BsonDecode(bson::DecoderError);
        BsonEncode(bson::EncoderError);
        Io(std::io::Error);
        R2D2(r2d2::Error);
    }

    errors {
        ArgumentError(msg: String) {
            description("An invalid argument was provided to a database operation")
            display("An invalid arugment was provided to a database operation: {}", msg)
        }

        InvalidHostname(hostname: String) {
            description("Unable to parse hostname")
            display("Unable to parse hostname: '{}'", hostname)
        }

        OperationError(msg: String) {
            description("A database operation failed to send or receive a reply")
            display("A database operation failed to send or receive a reply: {}", msg)
        }

        ParseError(data_type: String, file_path: String) {
            description("Unable to parse data from file")
            display("Unable to parse {} data from {}", data_type, file_path)
        }

        ResponseError(msg: String) {
            description("A database operation returned an invalid reply")
            display("A database operation returned an invalid reply: {}", msg)
        }

        ServerError(operation: String, msg: String) {
            description("An attempted database operation failed")
            display("{} operation failed: {}", operation, msg)
        }

        ServerSelectionError(msg: String) {
            description("An error occurred during server selection")
            display("An error occured during server selection: {}", msg)
        }
    }
}
