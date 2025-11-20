/// Macro to generate ID types with automatic incrementing values
#[macro_export]
macro_rules! id_type {
    ($name:ident, $counter_name:ident, $display_name:literal) => {
        // Global counter for this ID type
        static $counter_name: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

        #[derive(Debug, Clone, Hash, PartialEq, Eq, Copy, Default)]
        pub struct $name(u64);

        impl $name {
            // Generate a new ID with atomic counter
            fn __gen_new() -> Self {
                Self($counter_name.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
            }

            pub const fn to_be_bytes(self) -> [u8; 8] {
                self.0.to_be_bytes()
            }

            pub const fn from_be_bytes(bytes: [u8; 8]) -> Self {
                Self(u64::from_be_bytes(bytes))
            }

            pub const fn as_u64(&self) -> u64 {
                self.0
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self::__gen_new()
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self(value)
            }
        }

        impl From<$crate::types::UInt64> for $name {
            fn from(value: $crate::types::UInt64) -> Self {
                Self(value.0)
            }
        }

        impl From<$name> for $crate::types::UInt64 {
            fn from(value: $name) -> $crate::types::UInt64 {
                $crate::types::UInt64(value.0)
            }
        }

        impl From<$name> for u64 {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}: ({})", $display_name, self.0)
            }
        }

        impl PartialOrd for $name {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for $name {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.0.cmp(&other.0)
            }
        }

        impl TryFrom<&[u8]> for $name {
            type Error = std::io::Error;

            fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
                use std::convert::TryInto;
                use std::io::{Error, ErrorKind};

                // Verify length
                if value.len() < 8 {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "not enough bytes"));
                }

                // Copies to a fixed size array
                let arr: [u8; 8] = value[0..8]
                    .try_into()
                    .map_err(|_| Error::new(ErrorKind::InvalidData, "failed to copy bytes"))?;

                // Converts from an inner type.
                Ok(Self(u64::from_ne_bytes(arr)))
            }
        }

        impl AsRef<[u8]> for $name {
            fn as_ref(&self) -> &[u8] {
                unsafe {
                    std::slice::from_raw_parts(
                        &self.0 as *const u64 as *const u8,
                        std::mem::size_of::<u64>(),
                    )
                }
            }
        }
        $crate::arith!($name, u64);
    };
}
