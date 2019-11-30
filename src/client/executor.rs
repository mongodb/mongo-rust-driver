use super::Client;
use crate::{
    cmap::Connection,
    error::Result,
    operation::Operation,
    sdam::{update_topology, ServerDescription},
};

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
                let server = self.select_server(op.selection_criteria())?;
                let mut conn = server.checkout_connection()?;

                let result = self.execute_operation_on_connection(op, &mut conn);

                // If we encounter certain errors, we must update the topology as per the
                // SDAM spec.
                if let Err(ref e) = result {
                    let update = || {
                        let description =
                            ServerDescription::new(conn.address().clone(), Some(Err(e.clone())));
                        update_topology(self.topology(), description);
                    };

                    if e.is_non_timeout_network_error() {
                        update();
                    } else if e.is_recovering() || e.is_not_master() {
                        update();

                        // For "node is recovering" or "not master" errors, we must request a
                        // topology check.
                        server.request_topology_check();

                        let wire_version = conn
                            .stream_description()
                            .map(|sd| sd.max_wire_version)
                            .ok()
                            .and_then(std::convert::identity)
                            .unwrap_or(0);

                        // in 4.2+, we only clear connection pool if we've received a
                        // "node is shutting down" error. Otherwise, we always clear the pool.
                        if wire_version < 8 || e.is_shutting_down() {
                            server.clear_connection_pool();
                        }
                    }
                }
                result
            }
        }
    }

    /// Executes an operation on a given connection.
    fn execute_operation_on_connection<T: Operation>(
        &self,
        op: &T,
        connection: &mut Connection,
    ) -> Result<T::O> {
        let mut cmd = op.build(connection.stream_description()?)?;

        self.topology()
            .read()
            .unwrap()
            .update_command_with_read_pref(connection.address(), &mut cmd, op.selection_criteria());

        let response = connection.send_command(cmd)?;
        op.handle_response(response)
    }
}
