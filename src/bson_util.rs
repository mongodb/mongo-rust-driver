use bson::Bson;

/// Coerce numeric types into an `i64` if it would be lossless to do so. If this Bson is not numeric
/// or the conversion would be lossy (e.g. 1.5 -> 1), this returns `None`.
pub fn get_int(val: &Bson) -> Option<i64> {
    match *val {
        Bson::I32(i) => Some(i64::from(i)),
        Bson::I64(i) => Some(i),
        Bson::FloatingPoint(f) if f == f as i64 as f64 => Some(f as i64),
        _ => None,
    }
}
