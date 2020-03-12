use bson::{DecoderResult, Document, EncoderResult};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_byteorder::{AsyncReadBytesExt, AsyncWriteBytesExt, LittleEndian};

pub(crate) async fn decode_document<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> DecoderResult<Document> {
    let length = AsyncReadBytesExt::read_i32::<LittleEndian>(reader).await?;

    let mut bytes = Vec::new();
    AsyncWriteBytesExt::write_i32::<LittleEndian>(&mut bytes, length).await?;

    reader
        .take(length as u64 - 4)
        .read_to_end(&mut bytes)
        .await?;

    bson::decode_document(&mut bytes.as_slice())
}

pub(crate) async fn encode_document<W: AsyncWrite + Unpin + Send>(
    writer: &mut W,
    document: &Document,
) -> EncoderResult<()> {
    let mut bytes = Vec::new();

    bson::encode_document(&mut bytes, document)?;

    writer.write_all(&bytes).await?;

    Ok(())
}
