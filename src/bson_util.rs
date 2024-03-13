use std::{
    convert::TryFrom,
    io::{Read, Write},
};

use crate::{
    bson::{Bson, Document, RawArrayBuf, RawBson, RawBsonRef, RawDocumentBuf},
    checked::Checked,
    error::{ErrorKind, Result},
    runtime::SyncLittleEndianRead,
};

/// Coerce numeric types into an `i64` if it would be lossless to do so. If this Bson is not numeric
/// or the conversion would be lossy (e.g. 1.5 -> 1), this returns `None`.
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn get_int(val: &Bson) -> Option<i64> {
    match *val {
        Bson::Int32(i) => Some(i64::from(i)),
        Bson::Int64(i) => Some(i),
        Bson::Double(f) if (f - (f as i64 as f64)).abs() <= f64::EPSILON => Some(f as i64),
        _ => None,
    }
}

/// Coerce numeric types into an `i64` if it would be lossless to do so. If this Bson is not numeric
/// or the conversion would be lossy (e.g. 1.5 -> 1), this returns `None`.
pub(crate) fn get_int_raw(val: RawBsonRef<'_>) -> Option<i64> {
    match val {
        RawBsonRef::Int32(i) => get_int(&Bson::Int32(i)),
        RawBsonRef::Int64(i) => get_int(&Bson::Int64(i)),
        RawBsonRef::Double(i) => get_int(&Bson::Double(i)),
        _ => None,
    }
}

/// Coerce numeric types into an `u64` if it would be lossless to do so. If this Bson is not numeric
/// or the conversion would be lossy (e.g. 1.5 -> 1), this returns `None`.
#[allow(clippy::cast_possible_truncation)]
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

pub(crate) fn to_raw_bson_array(docs: &[Document]) -> Result<RawBson> {
    let mut array = RawArrayBuf::new();
    for doc in docs {
        array.push(RawDocumentBuf::from_document(doc)?);
    }
    Ok(RawBson::Array(array))
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

pub(crate) fn replacement_raw_document_check(replacement: &RawDocumentBuf) -> Result<()> {
    match replacement.iter().next().transpose()? {
        Some((key, _)) if !key.starts_with('$') => Ok(()),
        _ => Err(ErrorKind::InvalidArgument {
            message: "replace document must have first key not starting with '$'".to_string(),
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

/// The size in bytes of the provided document's entry in a BSON array at the given index.
pub(crate) fn array_entry_size_bytes(index: usize, doc_len: usize) -> Result<usize> {
    //   * type (1 byte)
    //   * number of decimal digits in key
    //   * null terminator for the key (1 byte)
    //   * size of value

    (Checked::new(1) + num_decimal_digits(index) + 1 + doc_len).get()
}

/// The number of digits in `n` in base 10.
/// Useful for calculating the size of an array entry in BSON.
fn num_decimal_digits(mut n: usize) -> usize {
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
    let length = reader.read_i32_sync()?;

    let mut bytes = Vec::with_capacity(length as usize);
    bytes.write_all(&length.to_le_bytes())?;

    reader.take(length as u64 - 4).read_to_end(&mut bytes)?;

    Ok(bytes)
}

pub(crate) fn extend_raw_document_buf(
    this: &mut RawDocumentBuf,
    other: RawDocumentBuf,
) -> Result<()> {
    for result in other.iter() {
        let (k, v) = result?;
        this.append(k, v.to_raw_bson());
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::bson_util::num_decimal_digits;

    #[test]
    fn num_digits() {
        assert_eq!(num_decimal_digits(0), 1);
        assert_eq!(num_decimal_digits(1), 1);
        assert_eq!(num_decimal_digits(10), 2);
        assert_eq!(num_decimal_digits(15), 2);
        assert_eq!(num_decimal_digits(100), 3);
        assert_eq!(num_decimal_digits(125), 3);
    }
}
