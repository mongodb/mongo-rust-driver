use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Operator {
    name: String,
    link: String,
    #[serde(rename = "type")]
    type_: Vec<OperatorType>,
    encode: EncodeType,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum OperatorType {
    SearchOperator,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum EncodeType {
    Object,
}

fn main() {
    println!("Hello, world!");
}
