#[macro_export]
macro_rules! numeric {
    (
        wrapper   $name:ident,
        inner     $inner:ty,
        primitive $primitive:ty,
        marker    $marker:ident $(,)?
    ) => {
        #[repr(transparent)]
        #[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default)]
        pub struct $name(pub $inner);

        /// Will trigger the autogeneration of the ByteMuck ref type.
        unsafe impl bytemuck::Pod for $name {}
        unsafe impl bytemuck::Zeroable for $name {}

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl $name {
            pub const fn new(value: $inner) -> Self {
                Self(value)
            }

            pub const fn value(self) -> $inner {
                self.0
            }
        }

        impl From<$inner> for $name {
            fn from(value: $inner) -> Self {
                Self(value)
            }
        }

        impl From<$name> for $inner {
            fn from(value: $name) -> Self {
                value.0
            }
        }


        impl $crate::types::core::BytemuckDeserializable for $name {

        }
            impl AsRef<[u8]> for $name {
            fn as_ref(&self) -> &[u8] {
                bytemuck::bytes_of(self)
            }
        }


        /// Numeric types always have a fixed size.
        impl $crate::types::core::FixedSizeType for $name {}


        /// Implement [to_primitive] type casting
        impl $crate::types::core::NumericType for $name {
            type Primitive = $primitive;

            fn to_primitive(self) -> $primitive {
                self.0 as $primitive
            }

            fn from_primitive(value: $primitive) -> Self {
                Self(value as $inner)
            }
        }

        impl $marker for $name {}

        // All numeric types are self-promotable to their target primitive type.
        $crate::promote_pair!($name, $name => $primitive);
    };
}

/// Creates a type promotion table for a pair of numeric types.
#[macro_export]
macro_rules! promote_pair {
    ($lhs:ty, $rhs:ty => $output:ty) => {
        impl $crate::types::core::Promote<$rhs> for $lhs {
            type Output = $output;

            fn promote_lhs(self) -> $output {
                self.0 as $output
            }

            fn promote_rhs(rhs: $rhs) -> $output {
                rhs.0 as $output
            }
        }
    };
}

/// Creates a symmetric promotion relationship between two types.
#[macro_export]
macro_rules! promote_symmetric {
    ($a:ty, $b:ty => $output:ty) => {
        $crate::promote_pair!($a, $b => $output);
        $crate::promote_pair!($b, $a => $output);
    };
}
