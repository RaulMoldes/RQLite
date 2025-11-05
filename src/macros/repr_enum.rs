#[macro_export]
macro_rules! enum_cast {
    ($name:ident : $repr:ty, { $($variant:ident = $value:expr),+ }) => {
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

        impl $name {
            /// Try to convert from the underlying representation type
            pub fn from_repr(value: $repr) -> std::io::Result<Self> {
                Self::try_from(value)
            }
        }
    };
}

#[macro_export]
macro_rules! named {
    ($name:ident, { $($variant:ident $(=> ($long:expr, $short:expr))?),+ }) => {
        impl $name {
            /// Get the name of the variant as a string
            pub const fn name(self) -> &'static str {
                match self {
                    $(
                        Self::$variant => {
                            // Use provided long name or default to variant name
                            impl_enum_names!(@name $variant $(, $long)?)
                        },
                    )+
                }
            }

            $($(
                // Only implement short_name if any variant has a short name
                impl_enum_names!(@impl_short_if_exists $name, { $($variant $(=> $short)?),+ });
            )?)?
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.name())
            }
        }
    };

    // Helper to get the name
    (@name $variant:ident) => { stringify!($variant) };
    (@name $variant:ident, $long:expr) => { $long };

    // Conditionally implement short_name if any variant has it
    (@impl_short_if_exists $name:ident, { $($variant:ident => $short:expr),+ }) => {
        impl $name {
            pub const fn short_name(self) -> &'static str {
                match self {
                    $(
                        Self::$variant => $short,
                    )+
                }
            }
        }
    };
    (@impl_short_if_exists $name:ident, { $($variant:ident),+ }) => {};
}

/// Unified enum macro that can handle both named_enum and byte_enum patterns
#[macro_export]
macro_rules! repr_enum {
    // Full version with names and display strings
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident : $repr:ty {
            $(
                $(#[$variant_meta:meta])*
                $variant:ident = $value:expr
                $(=> ($long:expr, $short:expr))?
            ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[repr($repr)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        $vis enum $name {
            $(
                $(#[$variant_meta])*
                $variant = $value,
            )+
        }

        impl From<$name> for $repr {
            fn from(value: $name) -> Self {
                value as $repr
            }
        }




        impl $name {
            /// Convert to the underlying representation type
            pub const fn as_repr(self) -> $repr {
                self as $repr
            }

            /// Get the numeric value
            pub const fn value(self) -> $repr {
                self as $repr
            }
        }

        // Implement TryFrom
        $crate::enum_cast!($name : $repr, { $($variant = $value),+ });

        // Implement name methods
        impl $name {
            /// Get all possible variants
            pub const fn variants() -> &'static [$name] {
                &[
                    $(
                        $name::$variant,
                    )+
                ]
            }

            /// Get the name of the variant
            pub const fn name(self) -> &'static str {
                match self {
                    $(
                        Self::$variant => $crate::repr_enum!(@get_long $variant $(, $long)?),
                    )+
                }
            }

            $crate::repr_enum!(@impl_short_name $name, { $($variant $(=> $short)?),+ });
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.name())
            }
        }
    };


    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $(
                $(#[$variant_meta:meta])*
                $variant:ident = $value:expr
                $(=> ($long:expr, $short:expr))?
            ),+ $(,)?
        }
    ) => {
        repr_enum! {
            $(#[$meta])*
            $vis enum $name : u8 {
                $(
                    $(#[$variant_meta])*
                    $variant = $value
                    $(=> ($long, $short))?
                ),+
            }
        }
    };

    // Helper to get long name
    (@get_long $variant:ident) => { stringify!($variant) };
    (@get_long $variant:ident, $long:expr) => { $long };

    // Conditionally implement short_name if any variant has it
    (@impl_short_name $name:ident, { $($variant:ident => $short:expr),+ }) => {
        /// Get the short name of the variant
        pub const fn short_name(self) -> &'static str {
            match self {
                $(
                    Self::$variant => $short,
                )+
            }
        }
    };
    (@impl_short_name $name:ident, { $($variant:ident),+ }) => {};
}
