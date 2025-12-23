/// Macro to automate the conversion from the variants of an enum into its type.
#[macro_export]
macro_rules! impl_enum_from {
    (
        enum $enum:ident {
            $(
                $variant:ident => $inner:ty
            ),+ $(,)?
        }
    ) => {
        $(
            impl From<$inner> for $enum {
                fn from(value: $inner) -> Self {
                    Self::$variant(value)
                }
            }

        )+
    };
}
