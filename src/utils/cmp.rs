/// Represents a key value extracted from an index node payload.
/// This enum allows for comparing different types of keys.
#[derive(Debug, Clone, PartialEq)]
pub enum KeyValue {
    Null,
    Integer(i64),
    Float(f64),
    String(String),
    Blob(Vec<u8>),
}

/// Implements partial ordering for the `KeyValue` enum, following SQLite's type affinity rules:
/// 
/// - `Null` is considered less than all other types.
/// - `Integer`, `Float`, `String`, and `Blob` are compared within their own types using their natural ordering.
/// - For cross-type comparisons, the following precedence is used: `Null` < `Integer` < `Float` < `String` < `Blob`.
/// - `Blob` values are compared lexicographically by their byte content.
/// 
/// Returns `Some(Ordering)` for all possible pairs, as all variants are comparable under these rules.
impl PartialOrd for KeyValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            // Null is less than everything else
            (KeyValue::Null, KeyValue::Null) => Some(std::cmp::Ordering::Equal),
            (KeyValue::Null, _) => Some(std::cmp::Ordering::Less),
            (_, KeyValue::Null) => Some(std::cmp::Ordering::Greater),

            // Integer comparison
            (KeyValue::Integer(a), KeyValue::Integer(b)) => a.partial_cmp(b),

            // Float comparison
            (KeyValue::Float(a), KeyValue::Float(b)) => a.partial_cmp(b),

            // String comparison
            (KeyValue::String(a), KeyValue::String(b)) => a.partial_cmp(b),

            // Blob comparison (lexicographical byte comparison)
            (KeyValue::Blob(a), KeyValue::Blob(b)) => a.partial_cmp(b),

            // Cross-type comparisons follow SQLite rules:
            // NULL < INTEGER < FLOAT < STRING < BLOB
            (KeyValue::Integer(_), KeyValue::Float(_)) => Some(std::cmp::Ordering::Less),
            (KeyValue::Integer(_), KeyValue::String(_)) => Some(std::cmp::Ordering::Less),
            (KeyValue::Integer(_), KeyValue::Blob(_)) => Some(std::cmp::Ordering::Less),

            (KeyValue::Float(_), KeyValue::Integer(_)) => Some(std::cmp::Ordering::Greater),
            (KeyValue::Float(_), KeyValue::String(_)) => Some(std::cmp::Ordering::Less),
            (KeyValue::Float(_), KeyValue::Blob(_)) => Some(std::cmp::Ordering::Less),

            (KeyValue::String(_), KeyValue::Integer(_)) => Some(std::cmp::Ordering::Greater),
            (KeyValue::String(_), KeyValue::Float(_)) => Some(std::cmp::Ordering::Greater),
            (KeyValue::String(_), KeyValue::Blob(_)) => Some(std::cmp::Ordering::Less),

            (KeyValue::Blob(_), KeyValue::Integer(_)) => Some(std::cmp::Ordering::Greater),
            (KeyValue::Blob(_), KeyValue::Float(_)) => Some(std::cmp::Ordering::Greater),
            (KeyValue::Blob(_), KeyValue::String(_)) => Some(std::cmp::Ordering::Greater),
        }
    }
}

impl std::hash::Hash for KeyValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            KeyValue::Null => state.write_u8(0),
            KeyValue::Integer(i) => {
                state.write_u8(1);
                state.write_i64(*i);
            }
            KeyValue::Float(f) => {
                state.write_u8(2);
                state.write_u64(f.to_bits());
            }
            KeyValue::String(s) => {
                state.write_u8(3);
                state.write(s.as_bytes());
            }
            KeyValue::Blob(b) => {
                state.write_u8(4);
                state.write(b);
            }
        }
    }
}
