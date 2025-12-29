/// Macro to implement partialeq for different datatypes
#[macro_export]
macro_rules! impl_datatype_partial_eq {
    ($($type_name:tt)+) => {
        impl PartialEq for $($type_name)+ {
            fn eq(&self, other: &Self) -> bool {
                match (self, other) {
                    // Null
                    (Self::Null, Self::Null) => true,
                    (Self::Null, _) | (_, Self::Null) => false,

                    // Bool and Blob require exact match
                    (Self::Bool(a), Self::Bool(b)) => a == b,
                    (Self::Blob(a), Self::Blob(b)) => a == b,

                    // Numeric types are promoted to f64 to compare
                    (a, b) if a.is_numeric() && b.is_numeric() => {
                        match (a.to_f64(), b.to_f64()) {
                            (Some(x), Some(y)) => x == y,
                            _ => false,
                        }
                    }

                    // Different type categories are not equal
                    _ => false,
                }
            }
        }
    };
}
/// Macro to implement partialord for different
#[macro_export]
macro_rules! impl_datatype_partial_ord {
    ($($type_name:tt)+) => {
        impl PartialOrd for $($type_name)+ {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                match (self, other) {
                    // Null comparisons return None (SQL three-valued logic)
                    (Self::Null, _) | (_, Self::Null) => None,

                    // Bool
                    (Self::Bool(a), Self::Bool(b)) => a.partial_cmp(b),

                    // Blob uses lexicographic ordering
                    (Self::Blob(a), Self::Blob(b)) => a.partial_cmp(b),

                    // Numeric types are promoted to f64 and compared
                    (a, b) if a.is_numeric() && b.is_numeric() => {
                        match (a.to_f64(), b.to_f64()) {
                            (Some(x), Some(y)) => x.partial_cmp(&y),
                            _ => None,
                        }
                    }

                    // Different type categories cannot be compared
                    _ => None,
                }
            }
        }
    };
}

#[macro_export]
macro_rules! impl_datatype_ord_traits {
    ($($type_name:tt)+) => {
        $crate::impl_datatype_partial_eq!($($type_name)+);
        $crate::impl_datatype_partial_ord!($($type_name)+);

    };
}

#[macro_export]
macro_rules! impl_datatype_hash {
    ($($type_name:tt)+) => {
        impl Hash for $($type_name)+ {
            fn hash<H: Hasher>(&self, state: &mut H) {
                match self {
                    Self::Null => {
                        0u8.hash(state);
                    }
                    Self::Bool(b) => {
                        1u8.hash(state);
                        (b.value() as u8).hash(state);
                    }
                    // Numerics are converted to bits using the same discriminant
                    Self::Int(v) => {
                        2u8.hash(state);
                        (v.0 as f64).to_bits().hash(state);
                    }
                    Self::BigInt(v) => {
                        2u8.hash(state);
                        (v.0 as f64).to_bits().hash(state);
                    }
                    Self::UInt(v) => {
                        2u8.hash(state);
                        (v.0 as f64).to_bits().hash(state);
                    }
                    Self::BigUInt(v) => {
                        2u8.hash(state);
                        (v.0 as f64).to_bits().hash(state);
                    }
                    Self::Float(v) => {
                        2u8.hash(state);
                        (v.0 as f64).to_bits().hash(state);
                    }
                    Self::Double(v) => {
                        2u8.hash(state);
                        v.0.to_bits().hash(state);
                    }
                    Self::Blob(b) => {
                        3u8.hash(state);
                        b.as_ref().hash(state);
                    }
                }
            }
        }
    };
}
