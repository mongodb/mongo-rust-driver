use crate::error::{
    Error,
    RECOVERING_CODES,
    RETRYABLE_READ_CODES,
    RETRYABLE_WRITE_CODES,
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
fn retryable_write_codes_subset_of_retryable_read_codes() {
    assert!(
        is_subset(&RETRYABLE_WRITE_CODES, &RETRYABLE_READ_CODES),
        "RETRYABLE_WRITE_CODES must be a subset of RETRYABLE_READ_CODES"
    );
}

#[test]
fn retryable_read_codes_differ_from_write_codes_by_exactly_134() {
    let read_only: Vec<i32> = RETRYABLE_READ_CODES
        .iter()
        .copied()
        .filter(|c| !RETRYABLE_WRITE_CODES.contains(c))
        .collect();
    assert_eq!(
        read_only,
        vec![134],
        "RETRYABLE_READ_CODES should differ from RETRYABLE_WRITE_CODES only by code 134 \
         (ReadConcernMajorityNotAvailableYet)"
    );
}
