use std::time::Duration;

use bson::SerializerOptions;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    bson::{doc, Bson, Document, RawDocumentBuf},
    bson_util::get_u64,
    error::{Error, Result},
    options::WriteConcern,
};

pub(crate) mod duration_option_as_int_seconds {
    use super::*;

    pub(crate) fn serialize<S: Serializer>(
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

    pub(crate) fn deserialize<'de, D>(
        deserializer: D,
    ) -> std::result::Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = Option::<u64>::deserialize(deserializer)?;
        Ok(millis.map(Duration::from_secs))
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

pub(crate) fn deserialize_duration_option_from_u64_millis<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let millis = Option::<u64>::deserialize(deserializer)?;
    Ok(millis.map(Duration::from_millis))
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
        _ => Err(serde::ser::Error::custom(
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

pub(crate) fn deserialize_u64_from_bson_number<'de, D>(
    deserializer: D,
) -> std::result::Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let bson = Bson::deserialize(deserializer)?;
    get_u64(&bson).ok_or_else(|| {
        serde::de::Error::custom(format!("could not deserialize u64 from {:?}", bson))
    })
}

pub(crate) fn serialize_error_as_string<S: Serializer>(
    val: &Error,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(&val.to_string())
}

pub(crate) fn serialize_result_error_as_string<S: Serializer, T: Serialize>(
    val: &Result<T>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    val.as_ref()
        .map_err(|e| e.to_string())
        .serialize(serializer)
}

#[cfg(feature = "aws-auth")]
pub(crate) fn deserialize_datetime_option_from_double_or_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<bson::DateTime>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum AwsDateTime {
        Double(f64),
        String(String),
    }

    let date_time = match AwsDateTime::deserialize(deserializer)? {
        AwsDateTime::Double(seconds) => {
            let millis = seconds * 1000.0;
            bson::DateTime::from_millis(millis as i64)
        }
        AwsDateTime::String(string) => bson::DateTime::parse_rfc3339_str(string)
            .map_err(|e| serde::de::Error::custom(format!("invalid RFC 3339 string: {}", e)))?,
    };

    Ok(Some(date_time))
}

pub(crate) fn to_raw_document_buf_with_options<T: Serialize>(
    doc: &T,
    human_readable_serialization: bool,
) -> Result<RawDocumentBuf> {
    let raw_doc = if human_readable_serialization {
        let doc = bson::to_document_with_options(
            doc,
            SerializerOptions::builder().human_readable(true).build(),
        )?;
        RawDocumentBuf::from_document(&doc)?
    } else {
        bson::to_raw_document_buf(doc)?
    };
    Ok(raw_doc)
}

pub(crate) fn write_concern_is_empty(write_concern: &Option<WriteConcern>) -> bool {
    write_concern
        .as_ref()
        .map_or(true, |write_concern| write_concern.is_empty())
}

#[cfg(test)]
pub(crate) fn deserialize_nonempty_vec<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Option<Vec<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    let vec: Vec<T> = Vec::deserialize(deserializer)?;
    if vec.is_empty() {
        return Err(serde::de::Error::custom(format!(
            "list provided for {} cannot be empty",
            std::any::type_name::<T>()
        )));
    }
    Ok(Some(vec))
}
