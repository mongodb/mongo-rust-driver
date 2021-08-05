use std::{
    convert::{TryFrom, TryInto},
    io::{Read, Write},
    time::Duration,
};

use bson::spec::ElementType;
use serde::{de::Error as SerdeDeError, ser, Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    bson::{doc, Bson, Document},
    error::{Error, ErrorKind, Result},
    runtime::{SyncLittleEndianRead, SyncLittleEndianWrite},
};

/// Coerce numeric types into an `i64` if it would be lossless to do so. If this Bson is not numeric
/// or the conversion would be lossy (e.g. 1.5 -> 1), this returns `None`.
pub(crate) fn get_int(val: &Bson) -> Option<i64> {
    match *val {
        Bson::Int32(i) => Some(i64::from(i)),
        Bson::Int64(i) => Some(i),
        Bson::Double(f) if (f - (f as i64 as f64)).abs() <= f64::EPSILON => Some(f as i64),
        _ => None,
    }
}

/// Coerce numeric types into an `u64` if it would be lossless to do so. If this Bson is not numeric
/// or the conversion would be lossy (e.g. 1.5 -> 1), this returns `None`.
pub(crate) fn get_u64(val: &Bson) -> Option<u64> {
    match *val {
        Bson::Int32(i) => u64::try_from(i).ok(),
        Bson::Int64(i) => u64::try_from(i).ok(),
        Bson::Double(f) if (f - (f as u64 as f64)).abs() <= f64::EPSILON => Some(f as u64),
        _ => None,
    }
}

pub(crate) fn to_bson_array(docs: &[Document]) -> Bson {
    Bson::Array(docs.iter().map(|doc| Bson::Document(doc.clone())).collect())
}

#[cfg(test)]
pub(crate) fn sort_document(document: &mut Document) {
    let temp = std::mem::take(document);

    let mut elements: Vec<_> = temp.into_iter().collect();
    elements.sort_by(|e1, e2| e1.0.cmp(&e2.0));

    document.extend(elements);
}

pub(crate) fn first_key(document: &Document) -> Option<&str> {
    document.keys().next().map(String::as_str)
}

pub(crate) fn replacement_document_check(replacement: &Document) -> Result<()> {
    match first_key(replacement) {
        Some(s) if !s.starts_with('$') => Ok(()),
        _ => Err(ErrorKind::InvalidArgument {
            message: "replace document must have first key not starting with '$".to_string(),
        }
        .into()),
    }
}

pub(crate) fn update_document_check(update: &Document) -> Result<()> {
    match first_key(update) {
        Some(s) if s.starts_with('$') => Ok(()),
        _ => Err(ErrorKind::InvalidArgument {
            message: "update document must have first key starting with '$".to_string(),
        }
        .into()),
    }
}

pub(crate) fn serialize_duration_option_as_int_millis<S: Serializer>(
    val: &Option<Duration>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    match val {
        Some(duration) if duration.as_millis() > i32::MAX as u128 => {
            serializer.serialize_i64(duration.as_millis() as i64)
        }
        Some(duration) => serializer.serialize_i32(duration.as_millis() as i32),
        None => serializer.serialize_none(),
    }
}

pub(crate) fn serialize_duration_option_as_int_secs<S: Serializer>(
    val: &Option<Duration>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    match val {
        Some(duration) if duration.as_secs() > i32::MAX as u64 => {
            serializer.serialize_i64(duration.as_secs() as i64)
        }
        Some(duration) => serializer.serialize_i32(duration.as_secs() as i32),
        None => serializer.serialize_none(),
    }
}

pub(crate) fn deserialize_duration_option_from_u64_millis<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let millis = Option::<u64>::deserialize(deserializer)?;
    Ok(millis.map(Duration::from_millis))
}

pub(crate) fn deserialize_duration_option_from_u64_seconds<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let millis = Option::<u64>::deserialize(deserializer)?;
    Ok(millis.map(Duration::from_secs))
}

#[allow(clippy::trivially_copy_pass_by_ref)]
pub(crate) fn serialize_u32_option_as_i32<S: Serializer>(
    val: &Option<u32>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    match val {
        Some(ref val) => bson::serde_helpers::serialize_u32_as_i32(val, serializer),
        None => serializer.serialize_none(),
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)]
pub(crate) fn serialize_u32_option_as_batch_size<S: Serializer>(
    val: &Option<u32>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    match val {
        Some(val) if *val <= std::i32::MAX as u32 => (doc! {
            "batchSize": (*val as i32)
        })
        .serialize(serializer),
        None => Document::new().serialize(serializer),
        _ => Err(ser::Error::custom(
            "batch size must be able to fit into a signed 32-bit integer",
        )),
    }
}

pub(crate) fn serialize_u64_option_as_i64<S: Serializer>(
    val: &Option<u64>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    match val {
        Some(ref v) => bson::serde_helpers::serialize_u64_as_i64(v, serializer),
        None => serializer.serialize_none(),
    }
}

/// Deserialize an u64 from any BSON number type if it could be done losslessly.
pub(crate) fn deserialize_u64_from_bson_number<'de, D>(
    deserializer: D,
) -> std::result::Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let bson = Bson::deserialize(deserializer)?;
    get_u64(&bson)
        .ok_or_else(|| D::Error::custom(format!("could not deserialize u64 from {:?}", bson)))
}

/// The size in bytes of the provided document's entry in a BSON array at the given index.
pub(crate) fn array_entry_size_bytes(index: usize, doc_len: usize) -> u64 {
    // 
    //   * type (1 byte)
    //   * number of decimal digits in key
    //   * null terminator for the key (1 byte)
    //   * size of value

    1 + num_decimal_digits(index) + 1 + doc_len as u64
}

/// The number of digits in `n` in base 10.
/// Useful for calculating the size of an array entry in BSON.
fn num_decimal_digits(mut n: usize) -> u64 {
    let mut digits = 0;

    loop {
        n /= 10;
        digits += 1;

        if n == 0 {
            return digits;
        }
    }
}

/// Read a document's raw BSON bytes from the provided reader.
pub(crate) fn read_document_bytes<R: Read>(mut reader: R) -> Result<Vec<u8>> {
    let length = reader.read_i32()?;

    let mut bytes = Vec::with_capacity(length as usize);
    bytes.write_i32(length)?;

    reader.take(length as u64 - 4).read_to_end(&mut bytes)?;

    Ok(bytes)
}

/// Get the value for the provided key from a buffer containing a BSON document.
/// If the key is not present, None will be returned.
/// If the BSON is not properly formatted, an internal error would be returned.
///
/// TODO: RUST-924 replace this with raw document API usage.
pub(crate) fn raw_get(doc: &[u8], key: &str) -> Result<Option<Bson>> {
    fn read_i32(reader: &mut std::io::Cursor<&[u8]>) -> Result<i32> {
        reader.read_i32().map_err(deserialize_error)
    }

    fn read_u8(reader: &mut std::io::Cursor<&[u8]>) -> Result<u8> {
        reader.read_u8().map_err(deserialize_error)
    }

    fn deserialize_error<T: std::error::Error>(_e: T) -> Error {
        deserialize_error_no_arg()
    }

    fn deserialize_error_no_arg() -> Error {
        Error::from(ErrorKind::Internal {
            message: "failed to read from serialized document".to_string(),
        })
    }

    let mut reader = std::io::Cursor::new(doc);
    let len: u64 = read_i32(&mut reader)?
        .try_into()
        .map_err(deserialize_error)?;

    while reader.position() < len {
        let element_start: usize = reader.position().try_into().map_err(deserialize_error)?;

        // read the element type
        let tag = read_u8(&mut reader)?;

        // check if we reached the end of the document
        if tag == 0 && reader.position() == len {
            return Ok(None);
        }

        let element_type = ElementType::from(tag).ok_or_else(deserialize_error_no_arg)?;

        // walk through the document until a null byte is encountered
        while read_u8(&mut reader)? != 0 {
            if reader.position() >= len {
                return Err(deserialize_error_no_arg());
            }
        }

        // parse the key
        let string_end: usize = reader
            .position()
            .checked_sub(1) // back from null byte
            .and_then(|u| usize::try_from(u).ok())
            .ok_or_else(deserialize_error_no_arg)?;
        let slice = &reader.get_ref()[(element_start + 1)..string_end];
        let k = std::str::from_utf8(slice).map_err(deserialize_error)?;

        // move to the end of the element
        let skip_len = match element_type {
            ElementType::Array
            | ElementType::EmbeddedDocument
            | ElementType::JavaScriptCodeWithScope => {
                let l = read_i32(&mut reader)?;
                // length includes the 4 bytes for the length, so subtrack them out
                l.checked_sub(4).ok_or_else(deserialize_error_no_arg)?
            }
            ElementType::Binary => read_i32(&mut reader)?
                .checked_add(1) // add one for subtype
                .ok_or_else(deserialize_error_no_arg)?,
            ElementType::Int32 => 4,
            ElementType::Int64 => 8,
            ElementType::String | ElementType::Symbol | ElementType::JavaScriptCode => {
                read_i32(&mut reader)?
            }
            ElementType::Boolean => 1,
            ElementType::Double => 8,
            ElementType::Timestamp => 8,
            ElementType::Decimal128 => 16,
            ElementType::MinKey
            | ElementType::MaxKey
            | ElementType::Null
            | ElementType::Undefined => 0,
            ElementType::DateTime => 8,
            ElementType::ObjectId => 12,
            ElementType::DbPointer => read_i32(&mut reader)?
                .checked_add(12) // add 12 for objectid
                .ok_or_else(deserialize_error_no_arg)?,
            ElementType::RegularExpression => {
                // read two cstr's
                for _i in 0..2 {
                    while read_u8(&mut reader)? != 0 {
                        if reader.position() >= len {
                            return Err(deserialize_error_no_arg());
                        }
                    }
                }

                0 // don't need to skip anymore since we already read the whole value
            }
        };
        let skip_len: u64 = skip_len.try_into().map_err(deserialize_error)?;
        reader.set_position(
            reader
                .position()
                .checked_add(skip_len)
                .ok_or_else(deserialize_error_no_arg)?,
        );

        if k == key {
            // if this is the element we're looking for, extract it.
            let element_end: usize = reader.position().try_into().map_err(deserialize_error)?;
            let element_slice = &reader.get_ref()[element_start..element_end];
            let element_length: i32 = element_slice.len().try_into().map_err(deserialize_error)?;

            // create a new temporary document which just has the element we want and grab the value
            let mut temp_doc = Vec::new();

            // write the document length
            let temp_len: i32 = element_length
                .checked_add(4 + 1)
                .ok_or_else(deserialize_error_no_arg)?;
            temp_doc
                .write_all(&temp_len.to_le_bytes())
                .map_err(deserialize_error)?;

            // add in the element
            temp_doc.extend(element_slice);

            // write the null byte
            temp_doc.push(0);

            let d = Document::from_reader(temp_doc.as_slice()).map_err(deserialize_error)?;
            return Ok(Some(
                d.get("_id").ok_or_else(deserialize_error_no_arg)?.clone(),
            ));
        }
    }

    // read all bytes but didn't reach null byte
    Err(deserialize_error_no_arg())
}

#[cfg(test)]
mod test {
    use crate::bson_util::num_decimal_digits;

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn num_digits() {
        assert_eq!(num_decimal_digits(0), 1);
        assert_eq!(num_decimal_digits(1), 1);
        assert_eq!(num_decimal_digits(10), 2);
        assert_eq!(num_decimal_digits(15), 2);
        assert_eq!(num_decimal_digits(100), 3);
        assert_eq!(num_decimal_digits(125), 3);
    }
}
