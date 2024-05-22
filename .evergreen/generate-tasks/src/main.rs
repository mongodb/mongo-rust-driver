use serde_json::json;

static VERSIONS: &[&str] = &[
    "3.6", "4.0", "4.2", "4.4", "5.0", "6.0", "7.0", "rapid", "latest",
];

static TOPOLOGIES: &[(&str, &str)] = &[
    ("standalone", "server"),
    ("replicaset", "replica_set"),
    ("sharded", "sharded_cluster"),
];

fn main() {
    let mut tasks = vec![];
    for &version in VERSIONS.iter() {
        for &(top_name, topology) in TOPOLOGIES.iter() {
            tasks.push(json!({
                "name": format!("test-{}-{}", version, top_name),
                "tags": [version, top_name],
                "commands": [{
                    "func": "bootstrap mongo-orchestration",
                    "vars": {
                        "MONGODB_VERSION": version,
                        "TOPOLOGY": topology,
                    }
                },
                {
                    "func": "run driver test suite",
                }],
            }));
        }
    }

    let file = json!({"tasks": tasks});
    println!("{}", serde_json::to_string_pretty(&file).unwrap());
}
