use serde::de::{Deserialize, DeserializeOwned, Deserializer};

pub(crate) fn deserialize_nonempty_vec<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Option<Vec<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
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
