use bson::{Bson, Document, EncoderResult};
use chrono::Timelike;
use futures::future::BoxFuture;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_byteorder::{AsyncWriteBytesExt, LittleEndian};

pub(crate) async fn encode_document<W: AsyncWrite + Unpin + Send>(
    writer: &mut W,
    doc: &Document,
) -> EncoderResult<()> {
    let mut buf = Vec::new();
    for (key, val) in doc.into_iter() {
        encode_bson(&mut buf, key.as_ref(), val).await?;
    }

    write_i32(
        writer,
        (buf.len() + std::mem::size_of::<i32>() + std::mem::size_of::<u8>()) as i32,
    )
    .await?;
    writer.write_all(&buf).await?;
    write_u8(writer, 0).await?;
    Ok(())
}

pub(crate) async fn write_u8<W: AsyncWrite + Unpin + Send>(
    writer: &mut W,
    u: u8,
) -> EncoderResult<()> {
    AsyncWriteBytesExt::write_u8(writer, u)
        .await
        .map_err(From::from)
}

async fn write_string<W: AsyncWrite + Unpin + Send>(writer: &mut W, s: &str) -> EncoderResult<()> {
    write_i32(writer, s.len() as i32 + 1).await?;
    writer.write_all(s.as_bytes()).await?;
    write_u8(writer, 0).await?;
    Ok(())
}

async fn write_cstring<W: AsyncWrite + Unpin + Send>(writer: &mut W, s: &str) -> EncoderResult<()> {
    writer.write_all(s.as_bytes()).await?;
    write_u8(writer, 0).await?;
    Ok(())
}

pub(crate) async fn write_i32<W: AsyncWrite + Unpin + Send>(
    writer: &mut W,
    val: i32,
) -> EncoderResult<()> {
    AsyncWriteBytesExt::write_i32::<LittleEndian>(writer, val)
        .await
        .map_err(From::from)
}

async fn write_i64<W: AsyncWrite + Unpin + Send>(writer: &mut W, val: i64) -> EncoderResult<()> {
    AsyncWriteBytesExt::write_i64::<LittleEndian>(writer, val)
        .await
        .map_err(From::from)
}

async fn write_f64<W: AsyncWrite + Unpin + Send>(writer: &mut W, val: f64) -> EncoderResult<()> {
    writer
        .write_f64::<LittleEndian>(val)
        .await
        .map_err(From::from)
}

async fn encode_array<W: AsyncWrite + Unpin + Send>(
    writer: &mut W,
    arr: &[Bson],
) -> EncoderResult<()> {
    let mut buf = Vec::new();
    for (key, val) in arr.iter().enumerate() {
        encode_bson(&mut buf, &key.to_string(), val).await?;
    }

    write_i32(
        writer,
        (buf.len() + std::mem::size_of::<i32>() + std::mem::size_of::<u8>()) as i32,
    )
    .await?;
    writer.write_all(&buf).await?;
    write_u8(writer, 0).await?;
    Ok(())
}

fn encode_bson<'a, W: AsyncWrite + Unpin + Send>(
    writer: &'a mut W,
    key: &'a str,
    val: &'a Bson,
) -> BoxFuture<'a, EncoderResult<()>> {
    Box::pin(async move {
        write_u8(writer, val.element_type() as u8).await?;
        write_cstring(writer, key).await?;

        match *val {
            Bson::FloatingPoint(v) => write_f64(writer, v).await,
            Bson::String(ref v) => write_string(writer, &v).await,
            Bson::Array(ref v) => encode_array(writer, &v).await,
            Bson::Document(ref v) => encode_document(writer, v).await,
            Bson::Boolean(v) => write_u8(writer, if v { 0x01 } else { 0x00 })
                .await
                .map_err(From::from),
            Bson::RegExp(ref pat, ref opt) => {
                write_cstring(writer, pat).await?;
                write_cstring(writer, opt).await
            }
            Bson::JavaScriptCode(ref code) => write_string(writer, &code).await,
            Bson::ObjectId(ref id) => writer.write_all(&id.bytes()).await.map_err(From::from),
            Bson::JavaScriptCodeWithScope(ref code, ref scope) => {
                let mut buf = Vec::new();
                write_string(&mut buf, code).await?;
                encode_document(&mut buf, scope).await?;

                write_i32(writer, buf.len() as i32 + 4).await?;
                writer.write_all(&buf).await.map_err(From::from)
            }
            Bson::I32(v) => write_i32(writer, v).await,
            Bson::I64(v) => write_i64(writer, v).await,
            Bson::TimeStamp(v) => write_i64(writer, v).await,
            Bson::Binary(subtype, ref data) => {
                write_i32(writer, data.len() as i32).await?;
                write_u8(writer, From::from(subtype)).await?;
                writer.write_all(data).await.map_err(From::from)
            }
            Bson::UtcDatetime(ref v) => {
                write_i64(
                    writer,
                    (v.timestamp() * 1000) + (v.nanosecond() / 1_000_000) as i64,
                )
                .await
            }
            Bson::Null => Ok(()),
            Bson::Symbol(ref v) => write_string(writer, &v).await,
            #[cfg(feature = "decimal128")]
            Bson::Decimal128(ref v) => write_f128(writer, v.clone()).await,
        }
    })
}
