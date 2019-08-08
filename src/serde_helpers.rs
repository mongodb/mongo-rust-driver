fn deserialize_duration_from_f64_millis<'de, D>(
    deserializer: D,
) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
{
    let millis = Option::<u64>::deserialize(deserializer)?;
    Ok(millis.map(Duration::from_millis))
}