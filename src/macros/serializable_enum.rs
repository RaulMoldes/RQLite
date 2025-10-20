/// Macro to generate serializable enums with automatic TryFrom implementation
/// Supports different representation types (u8, u16, u32, u64)
#[macro_export]
macro_rules! serializable_enum {
    // Version with custom repr type
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident : $repr:ty {
            $(
                $(#[$variant_meta:meta])*
                $variant:ident = $value:literal
                $(=> $display:literal)?
            ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[repr($repr)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        $vis enum $name {
            $(
                $(#[$variant_meta])*
                $variant = $value,
            )+
        }

        impl TryFrom<$repr> for $name {
            type Error = std::io::Error;

            fn try_from(value: $repr) -> Result<Self, Self::Error> {
                match value {
                    $(
                        $value => Ok($name::$variant),
                    )+
                    _ => Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Invalid {} value: {}", stringify!($name), value),
                    )),
                }
            }
        }

        impl From<$name> for $repr {
            fn from(value: $name) -> Self {
                value as $repr
            }
        }

        impl $name {
            /// Convert to the underlying representation type for serialization
            pub fn to_repr(self) -> $repr {
                self as $repr
            }

            /// Try to convert from the underlying representation type
            pub fn from_repr(value: $repr) -> std::io::Result<Self> {
                Self::try_from(value)
            }

            /// Get all possible variants
            pub const fn variants() -> &'static [$name] {
                &[
                    $(
                        $name::$variant,
                    )+
                ]
            }

            /// Get the name of the variant as a string
            pub fn name(&self) -> &'static str {
                match self {
                    $(
                        $name::$variant => stringify!($variant),
                    )+
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

                write!(f, "{}", self.name())

            }
        }
    };


}
