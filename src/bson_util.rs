use bson::{oid::ObjectId, Bson, Document};

use crate::error::{ErrorKind, Result};

pub fn get_int(val: &Bson) -> Option<i64> {
    match *val {
        Bson::I32(i) => Some(i64::from(i)),
        Bson::I64(i) => Some(i),
        Bson::FloatingPoint(f) if f == f as i64 as f64 => Some(f as i64),
        _ => None,
    }
}

pub fn add_id(doc: &mut Document) -> Bson {
    doc.entry("_id".to_string())
        .or_insert_with(|| Bson::ObjectId(ObjectId::new().unwrap()))
        .clone()
}

pub fn replacement_document_check(replacement: &Document) -> Result<()> {
    match replacement.iter().next() {
        Some((s, _)) if !s.starts_with('$') => Ok(()),
        _ => bail!(ErrorKind::ArgumentError(
            "replace document must have first key not starting with '$".to_string(),
        )),
    }
}

pub fn update_document_check(update: &Document) -> Result<()> {
    match update.iter().next() {
        Some((s, _)) if s.starts_with('$') => Ok(()),
        _ => bail!(ErrorKind::ArgumentError(
            "update document must have first key starting with '$".to_string(),
        )),
    }
}
