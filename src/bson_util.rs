use std::{
    collections::HashSet,
    convert::TryFrom,
    io::{Read, Write},
};

use serde::Serialize;

#[cfg(feature = "bson-3")]
use crate::bson_compat::RawBsonRefExt as _;
use crate::{
    bson::{
        oid::ObjectId,
        rawdoc,
        Bson,
        Document,
        RawArrayBuf,
        RawBson,
        RawBsonRef,
        RawDocumentBuf,
    },
    bson_compat::RawDocumentBufExt as _,
    checked::Checked,
    error::{Error, ErrorKind, Result},
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

/// Coerce numeric types into an `f64` if it would be lossless to do so. If this Bson is not numeric
/// or the conversion would be lossy (e.g. 1.5 -> 1), this returns `None`.
#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn get_double(val: &Bson) -> Option<f64> {
    match *val {
        Bson::Int32(i) => Some(f64::from(i)),
        Bson::Int64(i) if i == i as f64 as i64 => Some(i as f64),
        Bson::Double(f) => Some(f),
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
        array.push(RawDocumentBuf::try_from(doc)?);
    }
    Ok(RawBson::Array(array))
}
pub(crate) fn to_raw_bson_array_ser<T: Serialize>(values: &[T]) -> Result<RawBson> {
    let mut array = RawArrayBuf::new();
    for value in values {
        array.push(crate::bson_compat::serialize_to_raw_document_buf(value)?);
    }
    Ok(RawBson::Array(array))
}

pub(crate) fn first_key(document: &Document) -> Option<&str> {
    document.keys().next().map(String::as_str)
}

pub(crate) fn update_document_check(update: &Document) -> Result<()> {
    match first_key(update) {
        Some(key) => {
            if !key.starts_with('$') {
                Err(ErrorKind::InvalidArgument {
                    message: "update document must only contain update modifiers".to_string(),
                }
                .into())
            } else {
                Ok(())
            }
        }
        None => Err(ErrorKind::InvalidArgument {
            message: "update document must not be empty".to_string(),
        }
        .into()),
    }
}

pub(crate) fn replacement_document_check(replacement: &Document) -> Result<()> {
    if let Some(key) = first_key(replacement) {
        if key.starts_with('$') {
            return Err(ErrorKind::InvalidArgument {
                message: "replacement document must not contain update modifiers".to_string(),
            }
            .into());
        }
    }
    Ok(())
}

pub(crate) fn replacement_raw_document_check(replacement: &RawDocumentBuf) -> Result<()> {
    if let Some((key, _)) = replacement.iter().next().transpose()? {
        if crate::bson_compat::cstr_to_str(key).starts_with('$') {
            return Err(ErrorKind::InvalidArgument {
                message: "replacement document must not contain update modifiers".to_string(),
            }
            .into());
        };
    }
    Ok(())
}

/// The size in bytes of the provided document's entry in a BSON array at the given index.
pub(crate) fn array_entry_size_bytes(index: usize, doc_len: usize) -> Result<usize> {
    //   * type (1 byte)
    //   * number of decimal digits in key
    //   * null terminator for the key (1 byte)
    //   * size of value

    (Checked::new(1) + num_decimal_digits(index) + 1 + doc_len).get()
}

pub(crate) fn vec_to_raw_array_buf(docs: Vec<RawDocumentBuf>) -> RawArrayBuf {
    let mut array = RawArrayBuf::new();
    for doc in docs {
        array.push(doc);
    }
    array
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
    let length = Checked::new(reader.read_i32_sync()?);

    let mut bytes = Vec::with_capacity(length.try_into()?);
    bytes.write_all(&length.try_into::<u32>()?.to_le_bytes())?;

    reader
        .take((length - 4).try_into()?)
        .read_to_end(&mut bytes)?;

    Ok(bytes)
}

pub(crate) fn extend_raw_document_buf(
    this: &mut RawDocumentBuf,
    other: RawDocumentBuf,
) -> Result<()> {
    let mut keys: HashSet<crate::bson_compat::CString> = HashSet::new();
    for elem in this.iter_elements() {
        keys.insert(elem?.key().to_owned());
    }
    for result in other.iter() {
        let (k, v) = result?;
        if keys.contains(k) {
            return Err(Error::internal(format!(
                "duplicate raw document key {:?}",
                k
            )));
        }
        this.append(k, v.to_raw_bson());
    }
    Ok(())
}

pub(crate) fn append_ser(
    this: &mut RawDocumentBuf,
    key: impl AsRef<crate::bson_compat::CStr>,
    value: impl Serialize,
) -> Result<()> {
    #[derive(Serialize)]
    struct Helper<T> {
        value: T,
    }
    let raw_doc = crate::bson_compat::serialize_to_raw_document_buf(&Helper { value })?;
    this.append_ref_compat(
        key,
        raw_doc
            .get("value")?
            .ok_or_else(|| Error::internal("no value"))?,
    );
    Ok(())
}

/// Returns the _id field of this document, prepending the field to the document if one is not
/// already present.
pub(crate) fn get_or_prepend_id_field(doc: &mut RawDocumentBuf) -> Result<Bson> {
    match doc.get("_id")? {
        Some(id) => Ok(id.try_into()?),
        None => {
            let id = ObjectId::new();
            let mut new_bytes = rawdoc! { "_id": id }.into_bytes();

            // Remove the trailing null byte (which will be replaced by the null byte in the given
            // document) and append the document's elements
            new_bytes.pop();
            new_bytes.extend(&doc.as_bytes()[4..]);

            let new_length: i32 = Checked::new(new_bytes.len()).try_into()?;
            new_bytes[0..4].copy_from_slice(&new_length.to_le_bytes());

            *doc = RawDocumentBuf::from_bytes(new_bytes)?;

            Ok(id.into())
        }
    }
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
