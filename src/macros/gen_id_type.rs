/// Macro to generate ID types with automatic incrementing values
#[macro_export]
macro_rules! define_id_type {
    ($name:ident, $counter_name:ident, $display_name:literal) => {
        // Global counter for this ID type
        static $counter_name: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

        #[derive(Debug, Clone, Hash, PartialEq, Eq, Copy, Default)]
        pub struct $name(u32);

        impl $name {
            // Generate a new ID with atomic counter
            fn __gen_new() -> Self {
                Self($counter_name.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
            }
        }

        impl $crate::types::Key for $name {
            fn new_key() -> Self {
                Self::__gen_new()
            }
        }

        impl $crate::DataType for $name {
            fn size_of(&self) -> u16 {
                4
            }

            fn _type_of(&self) -> $crate::types::DataTypeMarker {
                $crate::types::DataTypeMarker::Key
            }
        }

        impl From<u32> for $name {
            fn from(value: u32) -> Self {
                Self(value)
            }
        }

        impl From<$name> for u32 {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}: ({})", $display_name, self.0)
            }
        }

        impl $crate::serialization::Serializable for $name {
            fn read_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
            where
                Self: Sized,
            {
                let mut buffer = [0u8; 4];
                reader.read_exact(&mut buffer)?;
                Ok(Self(u32::from_be_bytes(buffer)))
            }

            fn write_to<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<()> {
                writer.write_all(&self.0.to_be_bytes())?;
                Ok(())
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

        // Apply arithmetic operations macro
        $crate::impl_arithmetic_ops!($name);
    };
}
