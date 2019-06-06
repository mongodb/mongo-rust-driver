/// User-facing information about a connection to the database.
#[derive(Debug)]
pub struct ConnectionInfo {
    /// A driver-generated identifier that uniquely identifies the connection.
    pub id: u32,

    /// The hostname of the address of the server that the connection is connected to.
    pub hostname: String,

    /// The port of the address of the server that the connection is connected to.
    pub port: Option<u16>,
}
