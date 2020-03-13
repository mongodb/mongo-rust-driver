use bson::Document;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    error::Result,
    runtime::{AsyncLittleEndianRead, AsyncLittleEndianWrite},
};

pub(crate) async fn decode_document<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> Result<Document> {
    let length = reader.read_i32().await?;

    let mut bytes = Vec::new();
    bytes.write_i32(length).await?;

    reader
        .take(length as u64 - 4)
        .read_to_end(&mut bytes)
        .await?;

    let document = bson::decode_document(&mut bytes.as_slice())?;
    Ok(document)
}

pub(crate) async fn encode_document<W: AsyncWrite + Unpin + Send>(
    writer: &mut W,
    document: &Document,
) -> Result<()> {
    let mut bytes = Vec::new();

    bson::encode_document(&mut bytes, document)?;

    writer.write_all(&bytes).await?;

    Ok(())
}
