use crate::error::Error;

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
