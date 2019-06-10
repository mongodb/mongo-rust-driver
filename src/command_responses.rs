use bson::{oid::ObjectId, Bson, Document, UtcDateTime};

use crate::{read_preference::TagSet, topology::OpTime};

#[derive(Debug, Deserialize)]
pub struct DistinctCommandResponse {
    pub values: Vec<Bson>,
}

#[derive(Debug, Deserialize)]
pub struct FindCommandResponse {
    pub cursor: FindCommandResponseInner,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindCommandResponseInner {
    pub first_batch: Vec<Document>,
    pub id: i64,
}

#[derive(Debug, Deserialize)]
pub struct GetMoreCommandResponse {
    pub cursor: GetMoreCommandResponseInner,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetMoreCommandResponseInner {
    pub next_batch: Vec<Document>,
    pub id: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateCommandResponse {
    pub n: i64,
    pub n_modified: i64,
    pub upserted: Option<Vec<UpsertedResponse>>,
}

#[derive(Debug, Deserialize)]
pub struct UpsertedResponse {
    pub index: i64,
    #[serde(rename = "_id")]
    pub id: Bson,
}

#[derive(Debug, Deserialize)]
pub struct DeleteCommandResponse {
    pub n: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindAndModifyCommandResponse {
    pub value: Option<Document>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateIndexesResponse {
    pub ok: Bson,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListDatabasesResponse {
    pub databases: Vec<Document>,
}

#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IsMasterCommandResponse {
    #[serde(rename = "ismaster")]
    pub is_master: Option<bool>,
    pub ok: Option<f32>,
    pub hosts: Option<Vec<String>>,
    pub passives: Option<Vec<String>>,
    pub arbiters: Option<Vec<String>>,
    pub msg: Option<String>,
    pub me: Option<String>,
    pub set_version: Option<i32>,
    pub set_name: Option<String>,
    pub hidden: Option<bool>,
    pub secondary: Option<bool>,
    pub arbiter_only: Option<bool>,
    #[serde(rename = "isreplicaset")]
    pub is_replica_set: Option<bool>,
    pub logical_session_timeout_minutes: Option<i64>,
    pub last_write: Option<LastWrite>,
    pub min_wire_version: Option<i32>,
    pub max_wire_version: Option<i32>,
    pub tags: Option<TagSet>,
    pub election_id: Option<ObjectId>,
    pub primary: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LastWrite {
    pub op_time: OpTime,
    pub last_write_date: UtcDateTime,
}

#[cfg(test)]
mod test {
    use bson::Bson;

    use super::IsMasterCommandResponse;

    #[test]
    fn is_master_command_response() {
        let client = crate::Client::with_uri_str(
            option_env!("MONGODB_URI").unwrap_or("mongodb://localhost:27017"),
        )
        .unwrap();
        let db = client.database("test");

        let response = db.run_command(doc! { "isMaster": 1 }, None).unwrap();

        let is_master_response: IsMasterCommandResponse =
            bson::from_bson(Bson::Document(response)).unwrap();

        assert_eq!(is_master_response.ok, Some(1.0));
    }
}
