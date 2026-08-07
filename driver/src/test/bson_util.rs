#[cfg(feature = "tracing-unstable")]
#[test]
fn rawdoc_to_json_str_sanity() {
    use crate::{bson::rawdoc, bson_util::rawdoc_to_json_str};

    assert_eq!(
        r#"{ "hello": "world" }"#,
        rawdoc_to_json_str(&(rawdoc! {"hello": "world"}), 1000).unwrap()
    );
    assert_eq!(
        r#"{ "hello": 1, "world": 2 }"#,
        rawdoc_to_json_str(&(rawdoc! {"hello": 1, "world": 2}), 1000).unwrap()
    )
}

#[cfg(feature = "tracing-unstable")]
#[test]
fn rawdoc_to_json_str_nesting() {
    use crate::{bson::rawdoc, bson_util::rawdoc_to_json_str};

    assert_eq!(
        r#"{ "hello": { "world": 1 } }"#,
        rawdoc_to_json_str(&(rawdoc! {"hello": { "world": 1 }}), 1000).unwrap()
    );
    assert_eq!(
        r#"{ "hello": 1, "world": [ 2 ] }"#,
        rawdoc_to_json_str(&(rawdoc! {"hello": 1, "world": [ 2 ]}), 1000).unwrap()
    );
    assert_eq!(
        r#"{ "hello": {}, "world": 2 }"#,
        rawdoc_to_json_str(&(rawdoc! {"hello": { }, "world": 2 }), 1000).unwrap()
    );
    assert_eq!(
        r#"{ "hello": [], "world": 2 }"#,
        rawdoc_to_json_str(&(rawdoc! {"hello": [], "world": 2 }), 1000).unwrap()
    );
    assert_eq!("{}", rawdoc_to_json_str(&rawdoc! {}, 1000).unwrap());
}

#[cfg(feature = "tracing-unstable")]
#[test]
fn rawdoc_to_json_str_truncation() {
    use crate::{bson::rawdoc, bson_util::rawdoc_to_json_str};

    assert_eq!(
        r#"{ "he..."#,
        rawdoc_to_json_str(&(rawdoc! {"hello": "world" }), 5).unwrap()
    );
    assert_eq!(
        r#"{ "hello": ..."#,
        rawdoc_to_json_str(&(rawdoc! {"hello": "world" }), 11).unwrap()
    );
    assert_eq!(
        r#"{ "hello": "wor..."#,
        rawdoc_to_json_str(&(rawdoc! {"hello": "world" }), 15).unwrap()
    );
}
