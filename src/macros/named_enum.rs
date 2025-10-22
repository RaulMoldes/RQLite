#[macro_export]
macro_rules! named_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $(
                $variant:ident = $value:expr => ($long:expr, $short:expr)
            ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr(u8)]
        pub enum $name {
            $(
                $variant = $value,
            )*
        }

        impl $name {
            pub const fn from_u8(value: u8) -> Option<Self> {
                match value {
                    $(
                        $value => Some(Self::$variant),
                    )*
                    _ => None,
                }
            }

            pub const fn as_u8(self) -> u8 {
                self as u8
            }

            pub const fn name(self) -> &'static str {
                match self {
                    $(
                        Self::$variant => $long,
                    )*
                }
            }

            pub const fn short_name(self) -> &'static str {
                match self {
                    $(
                        Self::$variant => $short,
                    )*
                }
            }
        }
    };
}
