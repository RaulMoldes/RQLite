#[macro_export]
macro_rules! scalar {
    (
        $(#[$meta:meta])*
        pub struct $name:ident($inner:ty);
    ) => {
        $(#[$meta])*
        #[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        #[repr(transparent)]
        pub struct $name(pub $inner);


        impl $name {
            /// Fixed size of the type in bytes
            pub const SIZE: usize = std::mem::size_of::<$inner>();


            pub fn saturating_sub(&self, value: $inner) -> Self {
                Self(self.0.saturating_sub(value))
            }

            pub fn clamped_add(&self, value: $inner, max_val: $inner) -> Self {
                Self(self.0.saturating_add(value).max(max_val))
            }
        }

        impl TryFrom<&[u8]> for $name {
            type Error = std::io::Error;

            fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
                use std::convert::TryInto;
                use std::io::{Error, ErrorKind};

                // Verify length
                if value.len() < Self::SIZE {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "not enough bytes"));
                }

                // Copies to a fixed size array
                let arr: [u8; Self::SIZE] = value[0..Self::SIZE]
                    .try_into()
                    .map_err(|_| Error::new(ErrorKind::InvalidData, "failed to copy bytes"))?;

                // Converts from an inner type.
                Ok(Self(<$inner>::from_be_bytes(arr)))
            }
        }



        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }


        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{} index: {}", stringify!($name), self.0)
            }
        }

        impl From<$inner> for $name {
            fn from(v: $inner) -> Self {
                Self(v)
            }
        }

        impl From<$name> for $inner {
            fn from(v: $name) -> $inner {
                v.0
            }
        }


        impl From<usize> for $name {
            fn from(v: usize) -> Self {
                Self(v as $inner)
            }
        }

        impl From<$name> for usize {
            fn from(v: $name) -> usize {
                v.0 as usize
            }
        }

        impl std::ops::Add<$inner> for $name {
            type Output = $name;
            fn add(self, rhs: $inner) -> Self::Output {
                $name(self.0 + rhs)
            }
        }

        impl std::ops::Sub<$inner> for $name {
            type Output = $name;
            fn sub(self, rhs: $inner) -> Self::Output {
                $name(self.0 - rhs)
            }
        }


        impl std::ops::Add<usize> for $name {
            type Output = $name;
            fn add(self, rhs: usize) -> Self::Output {
                $name(self.0 + rhs as $inner)
            }
        }

        impl std::ops::Sub<usize> for $name {
            type Output = $name;
            fn sub(self, rhs: usize) -> Self::Output {
                $name(self.0 - rhs as $inner)
            }
        }

        impl std::ops::Mul<$inner> for $name {
            type Output = $name;
            fn mul(self, rhs: $inner) -> Self::Output {
                $name(self.0 * rhs)
            }
        }

        impl std::ops::AddAssign<$inner> for $name {
            fn add_assign(&mut self, rhs: $inner) {
                self.0 += rhs;
            }
        }

        impl std::ops::SubAssign<$inner> for $name {
            fn sub_assign(&mut self, rhs: $inner) {
                self.0 -= rhs;
            }
        }

        impl std::ops::MulAssign<$inner> for $name {
            fn mul_assign(&mut self, rhs: $inner) {
                self.0 *= rhs;
            }
        }


        impl PartialEq<$inner> for $name {
            fn eq(&self, other: &$inner) -> bool {
                self.0 == *other
            }
        }

        impl PartialEq<$name> for $inner {
            fn eq(&self, other: &$name) -> bool {
                *self == other.0
            }
        }

        $crate::impl_arithmetic_ops!($name);
        $crate::impl_bitwise_ops!($name);
    };
}

#[macro_export]
macro_rules! float_scalar {
    (
        $(#[$meta:meta])*
        pub struct $name:ident($inner:ty);
    ) => {
        $(#[$meta])*
        #[derive(Copy, Clone, PartialEq,  PartialOrd,  Default)]
        #[repr(transparent)]
        pub struct $name(pub $inner);

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{} index: {}", stringify!($name), self.0)
            }
        }

        impl From<$inner> for $name {
            fn from(v: $inner) -> Self {
                Self(v)
            }
        }

        impl From<$name> for $inner {
            fn from(v: $name) -> $inner {
                v.0
            }
        }


        impl From<usize> for $name {
            fn from(v: usize) -> Self {
                Self(v as $inner)
            }
        }

        impl From<$name> for usize {
            fn from(v: $name) -> usize {
                v.0 as usize
            }
        }

        impl std::ops::Add<$inner> for $name {
            type Output = $name;
            fn add(self, rhs: $inner) -> Self::Output {
                $name(self.0 + rhs)
            }
        }

        impl std::ops::Sub<$inner> for $name {
            type Output = $name;
            fn sub(self, rhs: $inner) -> Self::Output {
                $name(self.0 - rhs)
            }
        }

        impl std::ops::Mul<$inner> for $name {
            type Output = $name;
            fn mul(self, rhs: $inner) -> Self::Output {
                $name(self.0 * rhs)
            }
        }

        impl std::ops::AddAssign<$inner> for $name {
            fn add_assign(&mut self, rhs: $inner) {
                self.0 += rhs;
            }
        }

        impl std::ops::SubAssign<$inner> for $name {
            fn sub_assign(&mut self, rhs: $inner) {
                self.0 -= rhs;
            }
        }

        impl std::ops::MulAssign<$inner> for $name {
            fn mul_assign(&mut self, rhs: $inner) {
                self.0 *= rhs;
            }
        }


        impl PartialEq<$inner> for $name {
            fn eq(&self, other: &$inner) -> bool {
                self.0 == *other
            }
        }

        impl PartialEq<$name> for $inner {
            fn eq(&self, other: &$name) -> bool {
                *self == other.0
            }
        }

        $crate::impl_arithmetic_ops!($name);

    };
}
