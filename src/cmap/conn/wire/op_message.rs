use crate::cmap::conn::wire::header::OpCode;
use crate::cmap::conn::wire::message::MessageFlags;
use crate::error::{Error, ErrorKind, Result};
use crate::runtime::{AsyncLittleEndianWrite, SyncLittleEndianRead};
use crate::{Header, Message};
use bson::Document;
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::{
    io::{BufReader, BufWriter},
    AsyncBufReadExt, AsyncReadExt, AsyncWriteExt,
};
use serde::{Deserialize, Serialize};
use std::io::Read;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[allow(missing_docs)]
pub struct ReplyOp {
    pub flags: MessageFlags,
    pub cursor_id: u64,
    pub start_from: u32,
    pub number_returned: u32,
    pub documents: Vec<Document>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[allow(missing_docs)]
pub struct QueryOp {
    pub flags: MessageFlags,
    pub collection: String,
    pub skip: i32,
    pub limit: i32,
    pub query: Document,
    pub selector: Option<Document>,
}
impl ReplyOp {
    async fn read_reply_common(
        mut reader: &[u8],
        _length_remaining: i32,
        _header: &Header,
    ) -> Result<Self> {
        let flags = MessageFlags::from_bits_truncate(reader.read_u32()?);
        //length_remaining -= std::mem::size_of::<u32>() as i32;
        let mut buf: [u8; 8] = [0; 8];
        std::io::Read::read_exact(&mut reader, &mut buf)?;
        let cursor_id = u64::from_le_bytes(buf);

        let start = reader.read_u32()?;
        let numbers = reader.read_u32()?;
        let mut replies = Vec::new();
        for _i in 0..numbers {
            let len = reader.read_i32()?;
            let mut doc_vec = Vec::with_capacity(len as usize);
            doc_vec.write_all(&len.to_le_bytes()).await?;
            std::io::Read::take(reader, len as u64 - 4)
                .read_to_end(&mut doc_vec)
                .unwrap();
            std::io::BufRead::consume(&mut reader, len as usize - 4);
            let rep: Document = bson::from_slice(&doc_vec)?;
            replies.push(rep);
        }

        Ok(Self {
            flags,
            cursor_id,
            start_from: start,
            number_returned: numbers,
            documents: replies,
        })
    }
    /// Serializes the Message to bytes and writes them to `writer`.
    pub async fn write_to<T: AsyncWrite + Unpin + Send>(
        &self,
        mut header: Header,
        stream: &mut T,
    ) -> Result<()> {
        let mut reply_writer = Vec::new();
        reply_writer.write_u32(self.flags.bits()).await?;
        reply_writer
            .write_all(&self.cursor_id.to_le_bytes())
            .await?;
        reply_writer.write_u32(self.start_from).await?;
        reply_writer.write_u32(self.number_returned).await?;
        for doc in &self.documents {
            let mut s = Vec::new();
            doc.to_writer(&mut s)?;
            reply_writer.write_all(&s).await?;
        }
        let total_length = Header::LENGTH + reply_writer.len();
        header.length = total_length as i32;
        let mut writer = BufWriter::new(stream);
        header.write_to(&mut writer).await?;
        writer.write_all(&reply_writer).await?;
        writer.flush().await?;
        Ok(())
    }
}

impl QueryOp {
    async fn read_query_common(
        mut reader: &[u8],
        _length_remaining: i32,
        _header: &Header,
    ) -> Result<Self> {
        let flags = MessageFlags::from_bits_truncate(reader.read_u32()?);
        //length_remaining -= std::mem::size_of::<u32>() as i32;
        let mut collect = Vec::new();
        reader.read_until(0, &mut collect).await?;
        collect.pop().unwrap();
        let collection: String = String::from_utf8(collect).unwrap();
        let skip = reader.read_i32()?;
        let limit = reader.read_i32()?;
        // query
        let len = reader.read_i32()?;
        let mut doc_vec = Vec::with_capacity(len as usize);
        doc_vec.write_all(&len.to_le_bytes()).await?;
        std::io::Read::take(reader, len as u64 - 4)
            .read_to_end(&mut doc_vec)
            .unwrap();
        std::io::BufRead::consume(&mut reader, len as usize - 4);
        let query: Document = bson::from_slice(&doc_vec)?;
        let selector = if !reader.is_empty() {
            let len = reader.read_i32()?;
            let mut doc_vec = Vec::with_capacity(len as usize);
            doc_vec.write_all(&len.to_le_bytes()).await?;
            std::io::Read::take(reader, len as u64 - 4)
                .read_to_end(&mut doc_vec)
                .unwrap();
            let selector: Document = bson::from_slice(&doc_vec)?;

            Some(selector)
        } else {
            None
        };
        Ok(Self {
            flags,
            collection,
            skip,
            limit,
            query,
            selector,
        })
    }
    /// Serializes the Message to bytes and writes them to `writer`.
    pub async fn write_to<T: AsyncWrite + Unpin + Send>(
        &self,
        header: &Header,
        stream: &mut T,
    ) -> Result<()> {
        let mut writer = BufWriter::new(stream);
        header.write_to(&mut writer).await?;
        writer.write_u32(self.flags.bits()).await?;
        writer.write_all(self.collection.as_bytes()).await?;
        writer.write_u8(0).await?;
        writer.write_i32(self.skip).await?;
        writer.write_i32(self.limit).await?;
        let mut query = Vec::new();
        self.query.to_writer(&mut query)?;
        writer.write_all(&query).await?;
        if let Some(selector) = &self.selector {
            let mut s = Vec::new();
            selector.to_writer(&mut s)?;
            writer.write_all(&s).await?;
        }
        writer.flush().await?;
        Ok(())
    }
}
/// all mongo message
#[derive(Debug, Deserialize, Serialize)]
pub enum MongoMsg {
    /// _OP_MSG
    Message(Message),
    /// _OP_QUERY
    Query(QueryOp),
    /// _OP_REPLY
    Reply(ReplyOp),
}
/// decode stream to header and message
pub async fn read_msg<T: AsyncRead + Unpin + Send>(
    buf_reader: &mut BufReader<T>,
) -> Result<(Header, MongoMsg)> {
    let header = Header::read_from(buf_reader).await?;
    // TODO: RUST-616 ensure length is < maxMessageSizeBytes
    let length_remaining = header.length - Header::LENGTH as i32;
    let mut buf = vec![0u8; length_remaining as usize];
    buf_reader.read_exact(&mut buf).await?;
    let reader = buf.as_slice();

    let msg = if header.op_code == OpCode::Message {
        MongoMsg::Message(Message::read_op_common(reader, length_remaining, &header)?)
    } else if header.op_code == OpCode::Query {
        MongoMsg::Query(QueryOp::read_query_common(reader, length_remaining, &header).await?)
    } else if header.op_code == OpCode::Reply {
        MongoMsg::Reply(ReplyOp::read_reply_common(reader, length_remaining, &header).await?)
        //} else if header.op_code == OpCode::Compressed {
        //    MongoMsg::Message(Message::read_from_op_compressed(reader, &header).await?)
    } else {
        return Err(Error::new(
            ErrorKind::InvalidResponse {
                message: format!(
                    "Invalid op code, expected {} or {} and got {}",
                    OpCode::Message as u32,
                    OpCode::Compressed as u32,
                    header.op_code as u32
                ),
            },
            Option::<Vec<String>>::None,
        ));
    };
    Ok((header, msg))
}
