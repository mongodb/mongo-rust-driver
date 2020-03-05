use bson::{oid, Array, Bson, DecoderError, DecoderResult, Document};
use chrono::{
    offset::{LocalResult, TimeZone},
    Utc,
};
use futures::future::BoxFuture;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_byteorder::{AsyncReadBytesExt, LittleEndian};

const MAX_BSON_SIZE: i32 = 16 * 1024 * 1024;

pub(crate) async fn decode_document<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> DecoderResult<Document> {
    let mut doc = Document::new();

    // disregard the length: using Read::take causes infinite type recursion
    read_i32(reader).await?;

    loop {
        let tag = read_u8(reader).await?;

        if tag == 0 {
            break;
        }

        let key = read_cstring(reader).await?;
        let val = decode_bson(reader, tag, false).await?;

        doc.insert(key, val);
    }

    Ok(doc)
}

async fn read_string<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
    utf8_lossy: bool,
) -> DecoderResult<String> {
    let len = read_i32(reader).await?;

    // UTF-8 String must have at least 1 byte (the last 0x00).
    if len < 1 {
        return Err(DecoderError::InvalidLength(
            len as usize,
            format!("invalid length {} for UTF-8 string", len),
        ));
    }

    let s = if utf8_lossy {
        let mut buf = Vec::with_capacity(len as usize - 1);
        reader.take(len as u64 - 1).read_to_end(&mut buf).await?;
        String::from_utf8_lossy(&buf).to_string()
    } else {
        let mut s = String::with_capacity(len as usize - 1);
        reader.take(len as u64 - 1).read_to_string(&mut s).await?;
        s
    };
    read_u8(reader).await?; // The last 0x00

    Ok(s)
}

async fn read_cstring<R: AsyncRead + Unpin + Send>(reader: &mut R) -> DecoderResult<String> {
    let mut v = Vec::new();

    loop {
        let c = read_u8(reader).await?;
        if c == 0 {
            break;
        }
        v.push(c);
    }

    Ok(String::from_utf8(v)?)
}

async fn read_u8<R: AsyncRead + Unpin + Send>(reader: &mut R) -> DecoderResult<u8> {
    AsyncReadBytesExt::read_u8(reader).await.map_err(From::from)
}

pub(crate) async fn read_u32<R: AsyncRead + Unpin + Send>(reader: &mut R) -> DecoderResult<u32> {
    AsyncReadBytesExt::read_u32::<LittleEndian>(reader)
        .await
        .map_err(From::from)
}

pub(crate) async fn read_i32<R: AsyncRead + Unpin + Send>(reader: &mut R) -> DecoderResult<i32> {
    AsyncReadBytesExt::read_i32::<LittleEndian>(reader)
        .await
        .map_err(From::from)
}

async fn read_i64<R: AsyncRead + Unpin + Send>(reader: &mut R) -> DecoderResult<i64> {
    AsyncReadBytesExt::read_i64::<LittleEndian>(reader)
        .await
        .map_err(From::from)
}

async fn decode_array<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
    utf8_lossy: bool,
) -> DecoderResult<Array> {
    let mut arr = Array::new();

    // disregard the length: using Read::take causes infinite type recursion
    read_i32(reader).await?;

    loop {
        let tag = read_u8(reader).await?;
        if tag == 0 {
            break;
        }

        // check that the key is as expected
        let key = read_cstring(reader).await?;
        match key.parse::<usize>() {
            Err(..) => return Err(DecoderError::InvalidArrayKey(arr.len(), key)),
            Ok(idx) => {
                if idx != arr.len() {
                    return Err(DecoderError::InvalidArrayKey(arr.len(), key));
                }
            }
        }

        let val = decode_bson(reader, tag, utf8_lossy).await?;
        arr.push(val)
    }

    Ok(arr)
}

fn decode_bson<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
    tag: u8,
    utf8_lossy: bool,
) -> BoxFuture<DecoderResult<Bson>> {
    use bson::spec::{BinarySubtype, ElementType::*};

    Box::pin(async move {
        match bson::spec::ElementType::from(tag) {
            Some(FloatingPoint) => Ok(Bson::FloatingPoint(
                reader.read_f64::<LittleEndian>().await?,
            )),
            Some(Utf8String) => read_string(reader, utf8_lossy).await.map(Bson::String),
            Some(EmbeddedDocument) => decode_document(reader).await.map(Bson::Document),
            Some(Array) => decode_array(reader, utf8_lossy).await.map(Bson::Array),
            Some(Binary) => {
                let len = read_i32(reader).await?;
                if len < 0 || len > MAX_BSON_SIZE {
                    return Err(DecoderError::InvalidLength(
                        len as usize,
                        format!("Invalid binary length of {}", len),
                    ));
                }
                let subtype = BinarySubtype::from(read_u8(reader).await?);
                let mut data = Vec::with_capacity(len as usize);
                reader.take(len as u64).read_to_end(&mut data).await?;
                Ok(Bson::Binary(subtype, data))
            }
            Some(ObjectId) => {
                let mut objid = [0; 12];
                for x in &mut objid {
                    *x = read_u8(reader).await?;
                }
                Ok(Bson::ObjectId(oid::ObjectId::with_bytes(objid)))
            }
            Some(Boolean) => Ok(Bson::Boolean(read_u8(reader).await? != 0)),
            Some(NullValue) => Ok(Bson::Null),
            Some(RegularExpression) => {
                let pat = read_cstring(reader).await?;
                let opt = read_cstring(reader).await?;
                Ok(Bson::RegExp(pat, opt))
            }
            Some(JavaScriptCode) => read_string(reader, utf8_lossy)
                .await
                .map(Bson::JavaScriptCode),
            Some(JavaScriptCodeWithScope) => {
                // disregard the length:
                //     using Read::take causes infinite type recursion
                read_i32(reader).await?;

                let code = read_string(reader, utf8_lossy).await?;
                let scope = decode_document(reader).await?;
                Ok(Bson::JavaScriptCodeWithScope(code, scope))
            }
            Some(Integer32Bit) => read_i32(reader).await.map(Bson::I32),
            Some(Integer64Bit) => read_i64(reader).await.map(Bson::I64),
            Some(TimeStamp) => read_i64(reader).await.map(Bson::TimeStamp),
            Some(UtcDatetime) => {
                // The int64 is UTC milliseconds since the Unix epoch.
                let time = read_i64(reader).await?;

                let sec = time / 1000;
                let tmp_msec = time % 1000;
                let msec = if tmp_msec < 0 {
                    1000 - tmp_msec
                } else {
                    tmp_msec
                };

                match Utc.timestamp_opt(sec, (msec as u32) * 1_000_000) {
                    LocalResult::None => Err(DecoderError::InvalidTimestamp(time)),
                    LocalResult::Ambiguous(..) => Err(DecoderError::AmbiguousTimestamp(time)),
                    LocalResult::Single(t) => Ok(Bson::UtcDatetime(t)),
                }
            }
            Some(Symbol) => read_string(reader, utf8_lossy).await.map(Bson::Symbol),
            Some(Undefined) | Some(DbPointer) | Some(MaxKey) | Some(MinKey) | None => {
                Err(DecoderError::UnrecognizedElementType(tag))
            }
        }
    })
}
