use super::Client;
use crate::{cmap::Connection, error::Result, operation::Operation};

impl Client {
    /// Executes an operation and returns the connection used to do so along with the result of the
    /// operation. This will be used primarily for the opening of exhaust cursors.
    #[allow(dead_code)]
    pub(crate) fn execute_exhaust_operation<T: Operation>(
        &self,
        op: &T,
    ) -> Result<(T::O, Connection)> {
        let mut conn = self
            .select_server(op.selection_criteria())?
            .checkout_connection()?;
        self.execute_operation_on_connection(op, &mut conn)
            .map(|r| (r, conn))
    }

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
        match connection {
            Some(conn) => self.execute_operation_on_connection(op, conn),
            None => {
                let mut conn = self
                    .select_server(op.selection_criteria())?
                    .checkout_connection()?;
                self.execute_operation_on_connection(op, &mut conn)
            }
        }
    }

    fn execute_operation_on_connection<T: Operation>(
        &self,
        op: &T,
        connection: &mut Connection,
    ) -> Result<T::O> {
        let cmd = op.build(connection.stream_description()?)?;
        let response = connection.send_command(cmd)?;
        op.handle_response(response)
    }
}
