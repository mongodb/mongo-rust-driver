use crate::error::{
    Error,
    Redact,
    NOTWRITABLEPRIMARY_CODES,
    RECOVERING_CODES,
    SHUTTING_DOWN_CODES,
};

#[test]
fn custom_display() {
    let error = Error::custom("my custom error".to_string());
    let display = error.to_string();
    let kind = display.split(",").next().unwrap();
    assert_eq!(kind, "Kind: Custom user error: my custom error");

    let error = Error::custom(1);
    let display = error.to_string();
    let kind = display.split(",").next().unwrap();
    assert_eq!(kind, "Kind: Custom user error");
}

fn is_subset(subset: &[i32], superset: &[i32]) -> bool {
    subset.iter().all(|c| superset.contains(c))
}

#[test]
fn shutting_down_codes_subset_of_recovering_codes() {
    assert!(
        is_subset(&SHUTTING_DOWN_CODES, &RECOVERING_CODES),
        "SHUTTING_DOWN_CODES must be a subset of RECOVERING_CODES; update both arrays together \
         when adding new codes"
    );
}

#[test]
fn not_writeable_primary_codes_disjoint_from_recovering_codes() {
    assert!(
        NOTWRITABLEPRIMARY_CODES
            .iter()
            .all(|c| !RECOVERING_CODES.contains(c)),
        "NOTWRITABLEPRIMARY_CODES must be disjoint from RECOVERING_CODES",
    )
}

#[test]
fn redacted_display() {
    let value = "test";
    let actual = format!("{}", Redact(value));
    if cfg!(feature = "redact-errors") {
        assert!(
            !actual.contains(value),
            "value: {value:?} actual: {actual:?}"
        );
    } else {
        assert!(
            actual.contains(value),
            "value: {value:?} actual: {actual:?}"
        );
    }
}

#[test]
fn redacted_debug() {
    let value = "test";
    let actual = format!("{:?}", Redact(value));
    if cfg!(feature = "redact-errors") {
        assert!(
            !actual.contains(value),
            "value: {value:?} actual: {actual:?}"
        );
    } else {
        assert!(
            actual.contains(value),
            "value: {value:?} actual: {actual:?}"
        );
    }
}
