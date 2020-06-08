use crate::{
    error::{ErrorKind, Result},
    runtime::{AsyncLittleEndianRead, AsyncLittleEndianWrite, AsyncStream},
};

/// The wire protocol op codes.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum OpCode {
    Reply = 1,
    Query = 2004,
    Message = 2013,
}

impl OpCode {
    /// Attempt to infer the op code based on the numeric value.
    fn from_i32(i: i32) -> Result<Self> {
        match i {
            1 => Ok(OpCode::Reply),
            2004 => Ok(OpCode::Query),
            2013 => Ok(OpCode::Message),
            other => Err(ErrorKind::OperationError {
                message: format!("Invalid wire protocol opcode: {}", other),
            }
            .into()),
        }
    }
}

/// The header for any wire protocol message.
#[derive(Debug)]
pub(crate) struct Header {
    pub length: i32,
    pub request_id: i32,
    pub response_to: i32,
    pub op_code: OpCode,
}

impl Header {
    pub(crate) const LENGTH: usize = 4 * std::mem::size_of::<i32>();

    /// Serializes the Header and writes the bytes to `w`.
    pub(crate) async fn write_to(&self, stream: &mut AsyncStream) -> Result<()> {
        stream.write_i32(self.length).await?;
        stream.write_i32(self.request_id).await?;
        stream.write_i32(self.response_to).await?;
        stream.write_i32(self.op_code as i32).await?;

        Ok(())
    }

    /// Reads bytes from `r` and deserializes them into a header.
    pub(crate) async fn read_from(stream: &mut AsyncStream) -> Result<Self> {
        let length = stream.read_i32().await?;
        let request_id = stream.read_i32().await?;
        let response_to = stream.read_i32().await?;
        let op_code = OpCode::from_i32(stream.read_i32().await?)?;
        Ok(Self {
            length,
            request_id,
            response_to,
            op_code,
        })
    }
}
