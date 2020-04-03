pub(crate) mod async_encoding;

use std::time::Duration;

use bson::{doc, oid::ObjectId, Bson, Document};
use serde::{ser, Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{ErrorKind, Result};

/// Coerce numeric types into an `i64` if it would be lossless to do so. If this Bson is not numeric
/// or the conversion would be lossy (e.g. 1.5 -> 1), this returns `None`.
pub(crate) fn get_int(val: &Bson) -> Option<i64> {
    match *val {
        Bson::I32(i) => Some(i64::from(i)),
        Bson::I64(i) => Some(i),
        Bson::FloatingPoint(f) if f == f as i64 as f64 => Some(f as i64),
        _ => None,
    }
}

pub(crate) fn add_id(doc: &mut Document) {
    doc.entry("_id".to_string())
        .or_insert_with(|| Bson::ObjectId(ObjectId::new().unwrap()));
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
        _ => Err(ErrorKind::ArgumentError {
            message: "replace document must have first key not starting with '$".to_string(),
        }
        .into()),
    }
}

pub(crate) fn update_document_check(update: &Document) -> Result<()> {
    match first_key(update) {
        Some(s) if s.starts_with('$') => Ok(()),
        _ => Err(ErrorKind::ArgumentError {
            message: "update document must have first key starting with '$".to_string(),
        }
        .into()),
    }
}

pub(crate) fn serialize_duration_as_i64_millis<S: Serializer>(
    val: &Option<Duration>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    match val {
        Some(duration) => serializer.serialize_i64(duration.as_millis() as i64),
        None => serializer.serialize_none(),
    }
}

pub(crate) fn deserialize_duration_from_u64_millis<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let millis = Option::<u64>::deserialize(deserializer)?;
    Ok(millis.map(Duration::from_millis))
}

#[allow(clippy::trivially_copy_pass_by_ref)]
pub(crate) fn serialize_u32_as_i32<S: Serializer>(
    val: &Option<u32>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    match val {
        Some(val) if { *val <= std::i32::MAX as u32 } => serializer.serialize_i32(*val as i32),
        None => serializer.serialize_none(),
        _ => Err(ser::Error::custom("u32 specified does not fit into an i32")),
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)]
pub(crate) fn serialize_batch_size<S: Serializer>(
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

pub fn doc_size_bytes(doc: &Document) -> usize {
    // 
    // * i32 length prefix (4 bytes)
    // * for each element:
    //   * type (1 byte)
    //   * number of UTF-8 bytes in key
    //   * null terminator for the key (1 byte)
    //   * size of the value
    // * null terminator (1 byte)
    4 + doc
        .into_iter()
        .map(|(key, val)| 1 + key.len() + 1 + size_bytes(val))
        .sum::<usize>()
        + 1
}

pub fn size_bytes(val: &Bson) -> usize {
    match val {
        Bson::FloatingPoint(_) => 8,
        // 
        // * length prefix (4 bytes)
        // * number of UTF-8 bytes
        // * null terminator (1 byte)
        Bson::String(s) => 4 + s.len() + 1,
        // An array is serialized as a document with the keys "0", "1", "2", etc., so the size of
        // an array is:
        //
        // * length prefix (4 bytes)
        // * for each element:
        //   * type (1 byte)
        //   * number of decimal digits in key
        //   * null terminator for the key (1 byte)
        //   * size of value
        // * null terminator (1 byte)
        Bson::Array(arr) => {
            4 + arr
                .iter()
                .enumerate()
                .map(|(i, val)| 1 + num_decimal_digits(i) + 1 + size_bytes(val))
                .sum::<usize>()
                + 1
        }
        Bson::Document(doc) => doc_size_bytes(doc),
        Bson::Boolean(_) => 1,
        Bson::Null => 0,
        // for $pattern and $opts:
        //   * number of UTF-8 bytes
        //   * null terminator (1 byte)
        Bson::RegExp(pattern, opts) => pattern.len() + 1 + opts.len() + 1,
        // 
        // * length prefix (4 bytes)
        // * number of UTF-8 bytes
        // * null terminator (1 byte)
        Bson::JavaScriptCode(code) => 4 + code.len() + 1,
        // 
        // * i32 length prefix (4 bytes)
        // * i32 length prefix for code (4 bytes)
        // * number of UTF-8 bytes in code
        // * null terminator for code (1 byte)
        // * length of document
        Bson::JavaScriptCodeWithScope(code, scope) => {
            4 + 4 + code.len() + 1 + doc_size_bytes(scope)
        }
        Bson::I32(_) => 4,
        Bson::I64(_) => 8,
        Bson::TimeStamp(_) => 8,
        // 
        // * i32 length prefix (4 bytes)
        // * subtype (1 byte)
        // * number of bytes
        Bson::Binary(_, bytes) => 4 + 1 + bytes.len(),
        Bson::ObjectId(_) => 12,
        Bson::UtcDatetime(_) => 8,
        // 
        // * i32 length prefix (4 bytes)
        // * subtype (1 byte)
        // * number of UTF-8 bytes
        Bson::Symbol(s) => 4 + 1 + s.len(),
    }
}

fn num_decimal_digits(n: usize) -> usize {
    let mut digits = 1;
    let mut curr = 10;

    while curr < n {
        curr = match curr.checked_mul(10) {
            Some(val) => val,
            None => break,
        };

        digits += 1;
    }

    digits
}

#[cfg(test)]
mod test {
    use bson::{doc, oid::ObjectId, spec::BinarySubtype, Bson};
    use chrono::{DateTime, NaiveDateTime, Utc};

    use super::doc_size_bytes;

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn doc_size_bytes_eq_serialized_size_bytes() {
        let doc = doc! {
            "double": -12.3,
            "string": "foo",
            "array": ["foobar", -7, Bson::Null, Bson::TimeStamp(1278), false],
            "document": {
                "x": 1,
                "yyz": "Rush is one of the greatest bands of all time",
            },
            "bool": true,
            "null": Bson::Null,
            "regex": Bson::RegExp("foobar".into(), "i".into()),
            "code": Bson::JavaScriptCode("foo(x) { return x + 1; }".into()),
            "code with scope": Bson::JavaScriptCodeWithScope(
                "foo(x) { return x + y; }".into(),
                doc! { "y": -17 },
            ),
            "i32": 12i32,
            "i64": -126i64,
            "timestamp": Bson::TimeStamp(1223334444),
            "binary": Bson::Binary(BinarySubtype::Generic, vec![3, 222, 11]),
            "objectid": ObjectId::with_bytes([1; 12]),
            "datetime": DateTime::from_utc(
                NaiveDateTime::from_timestamp(4444333221, 0),
                Utc,
            ),
            "symbol": Bson::Symbol("foobar".into()),
        };

        let size_bytes = doc_size_bytes(&doc);

        let mut serialized_bytes = Vec::new();
        bson::encode_document(&mut serialized_bytes, &doc).unwrap();

        assert_eq!(size_bytes, serialized_bytes.len());
    }
}
