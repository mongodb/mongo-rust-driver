use bson::{Bson, Document};
use serde::{Deserialize, Deserializer};

fn flatten_extjson_value(mut doc: Document) -> Bson {
    if let Some(Bson::String(s)) = doc.remove("$numberInt") {
        return Bson::I32(s.parse().unwrap());
    }

    if let Some(Bson::String(s)) = doc.remove("$numberLong") {
        return Bson::I64(s.parse().unwrap());
    }

    Bson::Document(doc)
}

pub fn flatten_extjson_document(doc: Document) -> impl Iterator<Item = (String, Bson)> {
    doc.into_iter().map(|(key, val)| {
        let val = match val {
            Bson::Document(d) => flatten_extjson_value(d),
            other => other,
        };

        (key, val)
    })
}

pub fn deserialize_extjson_doc<'de, D>(deserializer: D) -> Result<Option<Document>, D::Error>
where
    D: Deserializer<'de>,
{
    let doc = Option::<Document>::deserialize(deserializer)?;
    Ok(doc.map(|d| flatten_extjson_document(d).collect()))
}
