use bson::{oid::ObjectId, Bson, Document};

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

pub fn replacement_document_check(replacement: &Document) -> Result<()> {
    match replacement.iter().next() {
        Some((s, _)) if !s.starts_with('$') => Ok(()),
        _ => Err(ErrorKind::ArgumentError {
            message: "replace document must have first key not starting with '$".to_string(),
        }
        .into()),
    }
}

pub fn update_document_check(update: &Document) -> Result<()> {
    match update.iter().next() {
        Some((s, _)) if s.starts_with('$') => Ok(()),
        _ => Err(ErrorKind::ArgumentError {
            message: "update document must have first key starting with '$".to_string(),
        }
        .into()),
    }
}
