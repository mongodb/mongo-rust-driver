use crate::trace::truncate_on_char_boundary;

#[cfg(feature="tracing-unstable")]
#[test]
fn tracing_truncation() {
    let single_emoji = String::from("🤔");
    let two_emoji = String::from("🤔🤔");

    let mut s = two_emoji.clone();
    assert_eq!(s.len(), 8);

    // start of string is a boundary, so we should truncate there
    truncate_on_char_boundary(&mut s, 0);
    assert_eq!(s, String::from(""));

    // we should "round up" to the end of the first emoji
    s = two_emoji.clone();
    truncate_on_char_boundary(&mut s, 1);
    assert_eq!(s, single_emoji);

    // 4 is a boundary, so we should truncate there
    s = two_emoji.clone();
    truncate_on_char_boundary(&mut s, 4);
    assert_eq!(s, single_emoji);

    // we should round up to the full string
    s = two_emoji.clone();
    truncate_on_char_boundary(&mut s, 5);
    assert_eq!(s, two_emoji);

    // end of string is a boundary, so we should truncate there
    s = two_emoji.clone();
    truncate_on_char_boundary(&mut s, 8);
    assert_eq!(s, two_emoji);

    // we should get the full string back if the new length is longer than the original
    s = two_emoji.clone();
    truncate_on_char_boundary(&mut s, 10);
    assert_eq!(s, two_emoji);
}
