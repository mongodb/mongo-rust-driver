use super::Client;
use crate::{cmap::Connection, error::Result, operation::Operation};

impl Client {
    /// Execute the given operation, optionally specifying a connection used to do so.
    /// If no connection is provided, server selection will performed using the criteria specified
    /// on the operation, if any.
    #[allow(dead_code)]
    pub(crate) fn execute_operation<T: Operation>(
        &self,
        op: &T,
        connection: Option<&mut Connection>,
    ) -> Result<T::O> {
        // if no connection provided, select one.
        let mut selected_connection = match connection {
            Some(_) => None,
            None => {
                let server = self.select_server(op.selection_criteria())?;
                Some(server.checkout_connection()?)
            }
        };

        // get a reference to the selected connection if it exists, otherwise get it from the
        // provided connection. exactly one of these will be non-null, so the final unwrap
        // will always succeed.
        let connection_ref = selected_connection.as_mut().unwrap_or(connection.unwrap());

        let cmd = op.build(connection_ref.stream_description()?)?;
        let response = connection_ref.send_command(cmd)?;
        op.handle_response(response)
    }
}
