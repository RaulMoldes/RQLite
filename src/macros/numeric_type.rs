#[macro_export]
macro_rules! numeric_type {
    (
        $(
            $wrapper:ident($inner:ty) => {
                type_marker: $marker:expr,
                size: $size:expr,
                display_name: $display_name:expr
            }
        ),+ $(,)?
    ) => {
        $(
            #[derive(Debug, Clone, Copy, Default)]
            #[repr(transparent)]
            pub struct $wrapper(pub $inner);

            $crate::impl_arithmetic_ops!($wrapper);

            impl $wrapper {
                /// Create a new instance
                pub const fn new(value: $inner) -> Self {
                    Self(value)
                }

                /// Get the inner value
                pub const fn value(self) -> $inner {
                    self.0
                }

                /// Get a reference to the inner value
                pub const fn as_ref(&self) -> &$inner {
                    &self.0
                }

                /// Get a mutable reference to the inner value
                pub fn as_mut(&mut self) -> &mut $inner {
                    &mut self.0
                }

                /// Convert to inner type
                pub const fn into_inner(self) -> $inner {
                    self.0
                }
            }

            // Implement DataType
            impl DataType for $wrapper {
                fn _type_of(&self) -> DataTypeMarker {
                    $marker
                }

                fn size_of(&self) -> u16 {
                    $size
                }
            }

            // Implement Display
            impl std::fmt::Display for $wrapper {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}: {}", $display_name, self.0)
                }
            }

            // Implement Serializable
            impl Serializable for $wrapper {
                fn read_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
                where
                    Self: Sized,
                {
                    let mut bytes = [0u8; $size as usize];
                    reader.read_exact(&mut bytes)?;
                    let value = <$inner>::from_be_bytes(bytes);
                    Ok($wrapper(value))
                }

                fn write_to<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<()> {
                    let bytes = self.0.to_be_bytes();
                    writer.write_all(&bytes)?;
                    Ok(())
                }
            }

            // Implement comparison traits
            impl PartialEq for $wrapper {
                fn eq(&self, other: &Self) -> bool {
                    self.0 == other.0
                }
            }

            impl Eq for $wrapper {}

            impl PartialOrd for $wrapper {
                fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                    Some(self.cmp(other))
                }
            }

            impl Ord for $wrapper {
                fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                    // Handle NaN cases for floating point
                    numeric_type!(@cmp self.0, other.0)
                }
            }

            // Implement From/Into for seamless conversion
            impl From<$inner> for $wrapper {
                fn from(value: $inner) -> Self {
                    Self(value)
                }
            }

            impl From<$wrapper> for $inner {
                fn from(wrapper: $wrapper) -> Self {
                    wrapper.0
                }
            }

            // Implement AsRef and AsMut
            impl AsRef<$inner> for $wrapper {
                fn as_ref(&self) -> &$inner {
                    &self.0
                }
            }

            impl AsMut<$inner> for $wrapper {
                fn as_mut(&mut self) -> &mut $inner {
                    &mut self.0
                }
            }

            // Deref for ergonomic access to inner type methods
            impl std::ops::Deref for $wrapper {
                type Target = $inner;

                fn deref(&self) -> &Self::Target {
                    &self.0
                }
            }

            impl std::ops::DerefMut for $wrapper {
                fn deref_mut(&mut self) -> &mut Self::Target {
                    &mut self.0
                }
            }


        )+


    };



    // Helper for handling comparison with NaN
    (@cmp $a:expr, $b:expr) => {{
        #[allow(clippy::float_cmp)]
        match $a.partial_cmp(&$b) {
            Some(ord) => ord,
            None => {
                // NaN handling: NaN is considered equal to itself and greater than any number
                if $a != $a && $b != $b {
                    std::cmp::Ordering::Equal
                } else if $a != $a {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }
        }
    }};


}
