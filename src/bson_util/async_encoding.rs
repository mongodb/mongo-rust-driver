use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    bson::Document,
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

    let document = Document::from_reader(&mut bytes.as_slice())?;
    Ok(document)
}

pub(crate) async fn encode_document<W: AsyncWrite + Unpin + Send>(
    writer: &mut W,
    document: &Document,
) -> Result<()> {
    let mut bytes = Vec::new();

    document.to_writer(&mut bytes)?;

    writer.write_all(&bytes).await?;

    Ok(())
}
