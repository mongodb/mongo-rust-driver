use std::time::Duration;

use bson::{bson, doc, oid::ObjectId, Bson, Document};
use serde::{ser, Serialize, Serializer};

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

#[allow(dead_code)]
pub(crate) fn sort_document(document: &mut Document) {
    let temp = std::mem::replace(document, Default::default());

    let mut elements: Vec<_> = temp.into_iter().collect();
    elements.sort_by(|e1, e2| e1.0.cmp(&e2.0));

    document.extend(elements);
}

pub(crate) fn replacement_document_check(replacement: &Document) -> Result<()> {
    match replacement.iter().next() {
        Some((s, _)) if !s.starts_with('$') => Ok(()),
        _ => Err(ErrorKind::ArgumentError {
            message: "replace document must have first key not starting with '$".to_string(),
        }
        .into()),
    }
}

pub(crate) fn update_document_check(update: &Document) -> Result<()> {
    match update.iter().next() {
        Some((s, _)) if s.starts_with('$') => Ok(()),
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
