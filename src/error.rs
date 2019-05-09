error_chain! {
    foreign_links {
        Io(std::io::Error);
    }

    errors {
        /// A malformed or invalid argument was passed to the driver.
        ArgumentError(msg: String) {
            description("An invalid argument was provided to a database operation")
            display("An invalid arugment was provided to a database operation: {}", msg)
        }

        /// The driver was unable to send or receive a message to the server.
        OperationError(msg: String) {
            description("A database operation failed to send or receive a reply")
            display("A database operation failed to send or receive a reply: {}", msg)
        }

       /// The response the driver received from the server was not in the form expected.
        ResponseError(msg: String) {
            description("A database operation returned an invalid reply")
            display("A database operation returned an invalid reply: {}", msg)
        }

        /// The server encountered an error when executing the operation.
        ServerError(operation: String, msg: String) {
            description("The server returned an error for an operation")
            display("{} operation failed: {}", operation, msg)
        }

        /// An error occurred during server selection.
        ServerSelectionError(msg: String) {
            description("An error occurred during server selection")
            display("An error occured during server selection: {}", msg)
        }
    }
}
