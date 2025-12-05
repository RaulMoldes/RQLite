#[macro_export]
macro_rules! arith {
    ($($wrapper:ident, $inner:ty),+ $(,)?) => {
        $(

            impl std::ops::Add for $wrapper {
                type Output = Self;
                fn add(self, rhs: Self) -> Self::Output {
                    Self(self.0 + rhs.0)
                }
            }

            impl std::ops::Sub for $wrapper {
                type Output = Self;
                fn sub(self, rhs: Self) -> Self::Output {
                    Self(self.0 - rhs.0)
                }
            }

            impl std::ops::Mul for $wrapper {
                type Output = Self;
                fn mul(self, rhs: Self) -> Self::Output {
                    Self(self.0 * rhs.0)
                }
            }

            impl std::ops::Div for $wrapper {
                type Output = Self;
                fn div(self, rhs: Self) -> Self::Output {
                    Self(self.0 / rhs.0)
                }
            }

            impl std::ops::Rem for $wrapper {
                type Output = Self;
                fn rem(self, rhs: Self) -> Self::Output {
                    Self(self.0 % rhs.0)
                }
            }

            impl<'a, 'b> std::ops::Add<&'b $wrapper> for &'a $wrapper {
                type Output = $wrapper;
                fn add(self, rhs: &'b $wrapper) -> Self::Output {
                    $wrapper(self.0 + rhs.0)
                }
            }

            impl<'a, 'b> std::ops::Sub<&'b $wrapper> for &'a $wrapper {
                type Output = $wrapper;
                fn sub(self, rhs: &'b $wrapper) -> Self::Output {
                    $wrapper(self.0 - rhs.0)
                }
            }

            impl<'a, 'b> std::ops::Mul<&'b $wrapper> for &'a $wrapper {
                type Output = $wrapper;
                fn mul(self, rhs: &'b $wrapper) -> Self::Output {
                    $wrapper(self.0 * rhs.0)
                }
            }

            impl<'a, 'b> std::ops::Div<&'b $wrapper> for &'a $wrapper {
                type Output = $wrapper;
                fn div(self, rhs: &'b $wrapper) -> Self::Output {
                    $wrapper(self.0 / rhs.0)
                }
            }

            impl<'a, 'b> std::ops::Rem<&'b $wrapper> for &'a $wrapper {
                type Output = $wrapper;
                fn rem(self, rhs: &'b $wrapper) -> Self::Output {
                    $wrapper(self.0 % rhs.0)
                }
            }


            impl<'a> std::ops::Add<&'a $wrapper> for $wrapper {
                type Output = $wrapper;
                fn add(self, rhs: &'a $wrapper) -> Self::Output {
                    $wrapper(self.0 + rhs.0)
                }
            }

            impl<'a> std::ops::Sub<&'a $wrapper> for $wrapper {
                type Output = $wrapper;
                fn sub(self, rhs: &'a $wrapper) -> Self::Output {
                    $wrapper(self.0 - rhs.0)
                }
            }

            impl<'a> std::ops::Mul<&'a $wrapper> for $wrapper {
                type Output = $wrapper;
                fn mul(self, rhs: &'a $wrapper) -> Self::Output {
                    $wrapper(self.0 * rhs.0)
                }
            }

            impl<'a> std::ops::Div<&'a $wrapper> for $wrapper {
                type Output = $wrapper;
                fn div(self, rhs: &'a $wrapper) -> Self::Output {
                    $wrapper(self.0 / rhs.0)
                }
            }

            impl<'a> std::ops::Rem<&'a $wrapper> for $wrapper {
                type Output = $wrapper;
                fn rem(self, rhs: &'a $wrapper) -> Self::Output {
                    $wrapper(self.0 % rhs.0)
                }
            }


            impl<'a> std::ops::Add<$wrapper> for &'a $wrapper {
                type Output = $wrapper;
                fn add(self, rhs: $wrapper) -> Self::Output {
                    $wrapper(self.0 + rhs.0)
                }
            }

            impl<'a> std::ops::Sub<$wrapper> for &'a $wrapper {
                type Output = $wrapper;
                fn sub(self, rhs: $wrapper) -> Self::Output {
                    $wrapper(self.0 - rhs.0)
                }
            }

            impl<'a> std::ops::Mul<$wrapper> for &'a $wrapper {
                type Output = $wrapper;
                fn mul(self, rhs: $wrapper) -> Self::Output {
                    $wrapper(self.0 * rhs.0)
                }
            }

            impl<'a> std::ops::Div<$wrapper> for &'a $wrapper {
                type Output = $wrapper;
                fn div(self, rhs: $wrapper) -> Self::Output {
                    $wrapper(self.0 / rhs.0)
                }
            }

            impl<'a> std::ops::Rem<$wrapper> for &'a $wrapper {
                type Output = $wrapper;
                fn rem(self, rhs: $wrapper) -> Self::Output {
                    $wrapper(self.0 % rhs.0)
                }
            }


            impl std::ops::Add<$inner> for $wrapper {
                type Output = $wrapper;
                fn add(self, rhs: $inner) -> Self::Output {
                    $wrapper(self.0 + rhs)
                }
            }

            impl std::ops::Sub<$inner> for $wrapper {
                type Output = $wrapper;
                fn sub(self, rhs: $inner) -> Self::Output {
                    $wrapper(self.0 - rhs)
                }
            }

            impl std::ops::Mul<$inner> for $wrapper {
                type Output = $wrapper;
                fn mul(self, rhs: $inner) -> Self::Output {
                    $wrapper(self.0 * rhs)
                }
            }

            impl std::ops::Div<$inner> for $wrapper {
                type Output = $wrapper;
                fn div(self, rhs: $inner) -> Self::Output {
                    $wrapper(self.0 / rhs)
                }
            }

            impl std::ops::Rem<$inner> for $wrapper {
                type Output = $wrapper;
                fn rem(self, rhs: $inner) -> Self::Output {
                    $wrapper(self.0 % rhs)
                }
            }

            impl<'a> std::ops::Add<$inner> for &'a $wrapper {
                type Output = $wrapper;
                fn add(self, rhs: $inner) -> Self::Output {
                    $wrapper(self.0 + rhs)
                }
            }

            impl<'a> std::ops::Sub<$inner> for &'a $wrapper {
                type Output = $wrapper;
                fn sub(self, rhs: $inner) -> Self::Output {
                    $wrapper(self.0 - rhs)
                }
            }

            impl<'a> std::ops::Mul<$inner> for &'a $wrapper {
                type Output = $wrapper;
                fn mul(self, rhs: $inner) -> Self::Output {
                    $wrapper(self.0 * rhs)
                }
            }

            impl<'a> std::ops::Div<$inner> for &'a $wrapper {
                type Output = $wrapper;
                fn div(self, rhs: $inner) -> Self::Output {
                    $wrapper(self.0 / rhs)
                }
            }

            impl<'a> std::ops::Rem<$inner> for &'a $wrapper {
                type Output = $wrapper;
                fn rem(self, rhs: $inner) -> Self::Output {
                    $wrapper(self.0 % rhs)
                }
            }

            impl<'a> std::ops::Add<&'a $inner> for $wrapper {
                type Output = $wrapper;
                fn add(self, rhs: &'a $inner) -> Self::Output {
                    $wrapper(self.0 + *rhs)
                }
            }

            impl<'a> std::ops::Sub<&'a $inner> for $wrapper {
                type Output = $wrapper;
                fn sub(self, rhs: &'a $inner) -> Self::Output {
                    $wrapper(self.0 - *rhs)
                }
            }

            impl<'a> std::ops::Mul<&'a $inner> for $wrapper {
                type Output = $wrapper;
                fn mul(self, rhs: &'a $inner) -> Self::Output {
                    $wrapper(self.0 * *rhs)
                }
            }

            impl<'a> std::ops::Div<&'a $inner> for $wrapper {
                type Output = $wrapper;
                fn div(self, rhs: &'a $inner) -> Self::Output {
                    $wrapper(self.0 / *rhs)
                }
            }

            impl<'a> std::ops::Rem<&'a $inner> for $wrapper {
                type Output = $wrapper;
                fn rem(self, rhs: &'a $inner) -> Self::Output {
                    $wrapper(self.0 % *rhs)
                }
            }


            impl<'a, 'b> std::ops::Add<&'b $inner> for &'a $wrapper {
                type Output = $wrapper;
                fn add(self, rhs: &'b $inner) -> Self::Output {
                    $wrapper(self.0 + *rhs)
                }
            }

            impl<'a, 'b> std::ops::Sub<&'b $inner> for &'a $wrapper {
                type Output = $wrapper;
                fn sub(self, rhs: &'b $inner) -> Self::Output {
                    $wrapper(self.0 - *rhs)
                }
            }

            impl<'a, 'b> std::ops::Mul<&'b $inner> for &'a $wrapper {
                type Output = $wrapper;
                fn mul(self, rhs: &'b $inner) -> Self::Output {
                    $wrapper(self.0 * *rhs)
                }
            }

            impl<'a, 'b> std::ops::Div<&'b $inner> for &'a $wrapper {
                type Output = $wrapper;
                fn div(self, rhs: &'b $inner) -> Self::Output {
                    $wrapper(self.0 / *rhs)
                }
            }

            impl<'a, 'b> std::ops::Rem<&'b $inner> for &'a $wrapper {
                type Output = $wrapper;
                fn rem(self, rhs: &'b $inner) -> Self::Output {
                    $wrapper(self.0 % *rhs)
                }
            }

            impl std::ops::Add<usize> for $wrapper {
                type Output = $wrapper;
                fn add(self, rhs: usize) -> Self::Output {
                    $wrapper(self.0 + rhs as $inner)
                }
            }

            impl std::ops::Sub<usize> for $wrapper {
                type Output = $wrapper;
                fn sub(self, rhs: usize) -> Self::Output {
                    $wrapper(self.0 - rhs as $inner)
                }
            }

            impl std::ops::Mul<usize> for $wrapper {
                type Output = $wrapper;
                fn mul(self, rhs: usize) -> Self::Output {
                    $wrapper(self.0 * rhs as $inner)
                }
            }

            impl std::ops::Div<usize> for $wrapper {
                type Output = $wrapper;
                fn div(self, rhs: usize) -> Self::Output {
                    $wrapper(self.0 / rhs as $inner)
                }
            }

            impl std::ops::Rem<usize> for $wrapper {
                type Output = $wrapper;
                fn rem(self, rhs: usize) -> Self::Output {
                    $wrapper(self.0 % rhs as $inner)
                }
            }

            impl<'a> std::ops::Add<&'a usize> for $wrapper {
                type Output = $wrapper;
                fn add(self, rhs: &'a usize) -> Self::Output {
                    $wrapper(self.0 + *rhs as $inner)
                }
            }

            impl<'a> std::ops::Sub<&'a usize> for $wrapper {
                type Output = $wrapper;
                fn sub(self, rhs: &'a usize) -> Self::Output {
                    $wrapper(self.0 - *rhs as $inner)
                }
            }

            impl<'a> std::ops::Mul<&'a usize> for $wrapper {
                type Output = $wrapper;
                fn mul(self, rhs: &'a usize) -> Self::Output {
                    $wrapper(self.0 * *rhs as $inner)
                }
            }

            impl<'a> std::ops::Div<&'a usize> for $wrapper {
                type Output = $wrapper;
                fn div(self, rhs: &'a usize) -> Self::Output {
                    $wrapper(self.0 / *rhs as $inner)
                }
            }

            impl<'a> std::ops::Rem<&'a usize> for $wrapper {
                type Output = $wrapper;
                fn rem(self, rhs: &'a usize) -> Self::Output {
                    $wrapper(self.0 % *rhs as $inner)
                }
            }


            impl<'a> std::ops::Add<usize> for &'a $wrapper {
                type Output = $wrapper;
                fn add(self, rhs: usize) -> Self::Output {
                    $wrapper(self.0 + rhs as $inner)
                }
            }

            impl<'a> std::ops::Sub<usize> for &'a $wrapper {
                type Output = $wrapper;
                fn sub(self, rhs: usize) -> Self::Output {
                    $wrapper(self.0 - rhs as $inner)
                }
            }

            impl<'a> std::ops::Mul<usize> for &'a $wrapper {
                type Output = $wrapper;
                fn mul(self, rhs: usize) -> Self::Output {
                    $wrapper(self.0 * rhs as $inner)
                }
            }

            impl<'a> std::ops::Div<usize> for &'a $wrapper {
                type Output = $wrapper;
                fn div(self, rhs: usize) -> Self::Output {
                    $wrapper(self.0 / rhs as $inner)
                }
            }

            impl<'a> std::ops::Rem<usize> for &'a $wrapper {
                type Output = $wrapper;
                fn rem(self, rhs: usize) -> Self::Output {
                    $wrapper(self.0 % rhs as $inner)
                }
            }


            impl<'a, 'b> std::ops::Add<&'b usize> for &'a $wrapper {
                type Output = $wrapper;
                fn add(self, rhs: &'b usize) -> Self::Output {
                    $wrapper(self.0 + *rhs as $inner)
                }
            }

            impl<'a, 'b> std::ops::Sub<&'b usize> for &'a $wrapper {
                type Output = $wrapper;
                fn sub(self, rhs: &'b usize) -> Self::Output {
                    $wrapper(self.0 - *rhs as $inner)
                }
            }

            impl<'a, 'b> std::ops::Mul<&'b usize> for &'a $wrapper {
                type Output = $wrapper;
                fn mul(self, rhs: &'b usize) -> Self::Output {
                    $wrapper(self.0 * *rhs as $inner)
                }
            }

            impl<'a, 'b> std::ops::Div<&'b usize> for &'a $wrapper {
                type Output = $wrapper;
                fn div(self, rhs: &'b usize) -> Self::Output {
                    $wrapper(self.0 / *rhs as $inner)
                }
            }

            impl<'a, 'b> std::ops::Rem<&'b usize> for &'a $wrapper {
                type Output = $wrapper;
                fn rem(self, rhs: &'b usize) -> Self::Output {
                    $wrapper(self.0 % *rhs as $inner)
                }
            }


            impl std::ops::AddAssign for $wrapper {
                fn add_assign(&mut self, rhs: Self) {
                    self.0 += rhs.0;
                }
            }

            impl std::ops::SubAssign for $wrapper {
                fn sub_assign(&mut self, rhs: Self) {
                    self.0 -= rhs.0;
                }
            }

            impl std::ops::MulAssign for $wrapper {
                fn mul_assign(&mut self, rhs: Self) {
                    self.0 *= rhs.0;
                }
            }

            impl std::ops::DivAssign for $wrapper {
                fn div_assign(&mut self, rhs: Self) {
                    self.0 /= rhs.0;
                }
            }

            impl std::ops::RemAssign for $wrapper {
                fn rem_assign(&mut self, rhs: Self) {
                    self.0 %= rhs.0;
                }
            }


            impl std::ops::AddAssign<$inner> for $wrapper {
                fn add_assign(&mut self, rhs: $inner) {
                    self.0 += rhs;
                }
            }

            impl std::ops::SubAssign<$inner> for $wrapper {
                fn sub_assign(&mut self, rhs: $inner) {
                    self.0 -= rhs;
                }
            }

            impl std::ops::MulAssign<$inner> for $wrapper {
                fn mul_assign(&mut self, rhs: $inner) {
                    self.0 *= rhs;
                }
            }

            impl std::ops::DivAssign<$inner> for $wrapper {
                fn div_assign(&mut self, rhs: $inner) {
                    self.0 /= rhs;
                }
            }

            impl std::ops::RemAssign<$inner> for $wrapper {
                fn rem_assign(&mut self, rhs: $inner) {
                    self.0 %= rhs;
                }
            }


            impl<'a> std::ops::AddAssign<&'a $inner> for $wrapper {
                fn add_assign(&mut self, rhs: &'a $inner) {
                    self.0 += *rhs;
                }
            }

            impl<'a> std::ops::SubAssign<&'a $inner> for $wrapper {
                fn sub_assign(&mut self, rhs: &'a $inner) {
                    self.0 -= *rhs;
                }
            }

            impl<'a> std::ops::MulAssign<&'a $inner> for $wrapper {
                fn mul_assign(&mut self, rhs: &'a $inner) {
                    self.0 *= *rhs;
                }
            }

            impl<'a> std::ops::DivAssign<&'a $inner> for $wrapper {
                fn div_assign(&mut self, rhs: &'a $inner) {
                    self.0 /= *rhs;
                }
            }

            impl<'a> std::ops::RemAssign<&'a $inner> for $wrapper {
                fn rem_assign(&mut self, rhs: &'a $inner) {
                    self.0 %= *rhs;
                }
            }


            impl std::ops::AddAssign<usize> for $wrapper {
                fn add_assign(&mut self, rhs: usize) {
                    self.0 += rhs as $inner;
                }
            }

            impl std::ops::SubAssign<usize> for $wrapper {
                fn sub_assign(&mut self, rhs: usize) {
                    self.0 -= rhs as $inner;
                }
            }

            impl std::ops::MulAssign<usize> for $wrapper {
                fn mul_assign(&mut self, rhs: usize) {
                    self.0 *= rhs as $inner;
                }
            }

            impl std::ops::DivAssign<usize> for $wrapper {
                fn div_assign(&mut self, rhs: usize) {
                    self.0 /= rhs as $inner;
                }
            }

            impl std::ops::RemAssign<usize> for $wrapper {
                fn rem_assign(&mut self, rhs: usize) {
                    self.0 %= rhs as $inner;
                }
            }


            impl<'a> std::ops::AddAssign<&'a usize> for $wrapper {
                fn add_assign(&mut self, rhs: &'a usize) {
                    self.0 += *rhs as $inner;
                }
            }

            impl<'a> std::ops::SubAssign<&'a usize> for $wrapper {
                fn sub_assign(&mut self, rhs: &'a usize) {
                    self.0 -= *rhs as $inner;
                }
            }

            impl<'a> std::ops::MulAssign<&'a usize> for $wrapper {
                fn mul_assign(&mut self, rhs: &'a usize) {
                    self.0 *= *rhs as $inner;
                }
            }

            impl<'a> std::ops::DivAssign<&'a usize> for $wrapper {
                fn div_assign(&mut self, rhs: &'a usize) {
                    self.0 /= *rhs as $inner;
                }
            }

            impl<'a> std::ops::RemAssign<&'a usize> for $wrapper {
                fn rem_assign(&mut self, rhs: &'a usize) {
                    self.0 %= *rhs as $inner;
                }
            }


        )+
    };
}

/// Implements AxmosCastable using direct cast (`as`)
#[macro_export]
macro_rules! direct_axmos_cast {
    ($from:ty [$from_inner:ty] => $($to:ty [$to_inner:ty]),+ $(,)?) => {
        $(
            impl $crate::types::core::AxmosCastable<$to> for $from {
                fn try_cast(&self) -> Option<$to> {
                    Some(<$to>::from(self.0 as $to_inner))
                }
            }
        )+
    };
}

#[macro_export]
macro_rules! to_blob {
    ($($type:ty),*) => {
        $(
            impl AxmosCastable<Blob> for $type {
                fn can_cast(&self) -> bool {
                    true
                }

                fn try_cast(&self) -> Option<Blob> {
                    let bytes: &[u8] = self.as_ref();
                    Some(Blob::from(bytes))
                }
            }
        )*
    };
}

#[macro_export]
macro_rules! from_blob {
    ($($type:ty),*) => {
        $(
            impl AxmosCastable<$type> for Blob {
                fn can_cast(&self) -> bool {
                    <$type>::try_from(self.content()).is_ok()
                }

                fn try_cast(&self) -> Option<$type> {
                    <$type>::try_from(self.content()).ok()
                }
            }
        )*
    };
}

#[macro_export]
macro_rules! non_castable {
    ($from:ty, $to:ty) => {
        impl AxmosCastable<$to> for $from {
            fn can_cast(&self) -> bool {
                false
            }

            fn try_cast(&self) -> Option<$to> {
                None
            }
        }
    };
}

#[macro_export]
macro_rules! checked_conversion {
    ($from:ty => $to:ty) => {
        paste::paste! {
            #[inline]
            fn [<$from _to_ $to _checked>](value: $from) -> Option<$to> {
                let casted = value as $to;
                if casted as $from == value {
                    Some(casted)
                } else {
                    None
                }
            }
        }
    };
}

#[macro_export]
macro_rules! numeric {
    ($wrapper:ident, $inner:ty) => {
        impl $wrapper {
            pub const SIZE: usize = std::mem::size_of::<$inner>();
        }

        impl std::fmt::Debug for $wrapper {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}({})", stringify!($wrapper), self.0)
            }
        }

        impl std::fmt::Display for $wrapper {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}: {}", stringify!($wrapper), self.0)
            }
        }

        impl From<$inner> for $wrapper {
            fn from(v: $inner) -> Self {
                Self(v)
            }
        }

        impl From<$wrapper> for $inner {
            fn from(v: $wrapper) -> $inner {
                v.0
            }
        }

        impl From<usize> for $wrapper {
            fn from(v: usize) -> Self {
                Self(v as $inner)
            }
        }

        impl From<$wrapper> for usize {
            fn from(v: $wrapper) -> usize {
                v.0 as usize
            }
        }

        impl PartialEq<$inner> for $wrapper {
            fn eq(&self, other: &$inner) -> bool {
                self.0 == *other
            }
        }

        impl PartialEq<$wrapper> for $inner {
            fn eq(&self, other: &$wrapper) -> bool {
                *self == other.0
            }
        }
    };
}

#[macro_export]
macro_rules! impl_ref {
    ($wrapper:ident, $inner:ty) => {
        paste::paste! {
            // Fixed-size byte wrapper
            #[repr(transparent)]
            #[derive(Copy, Clone, Default)]
            pub struct [<$wrapper Bytes>]([u8; std::mem::size_of::<$inner>()]);

            // SAFE: [u8; N] is Pod and Zeroable
            unsafe impl bytemuck::Zeroable for [<$wrapper Bytes>] {}
            unsafe impl bytemuck::Pod for [<$wrapper Bytes>] {}

            /// Immutable reference
            #[repr(transparent)]
            pub struct [<$wrapper Ref>]<'a> {
                bytes: &'a [<$wrapper Bytes>],
            }

            impl<'a> [<$wrapper Ref>]<'a> {
                pub const SIZE: usize = std::mem::size_of::<$inner>();

                pub fn from_bytes(bytes: &'a [u8]) -> std::io::Result<Self> {
                    use std::io::{Error, ErrorKind};
                    if bytes.len() < Self::SIZE {
                        return Err(Error::new(ErrorKind::UnexpectedEof, "not enough bytes"));
                    }

                    let prefix = &bytes[..Self::SIZE];
                    let bytes_elem: &[<$wrapper Bytes>] =
                        bytemuck::try_from_bytes(prefix)
                            .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid bytes for Pod"))?;

                    Ok(Self { bytes: bytes_elem })
                }

                #[inline]
                pub fn get(&self) -> $inner {
                    <$inner>::from_le_bytes(self.bytes.0)
                }

                #[inline]
                pub fn to_owned(&self) -> $wrapper {
                    $wrapper(self.get())
                }

                #[inline]
                pub fn as_raw_bytes(&self) -> &[u8] {
                    bytemuck::bytes_of(self.bytes)
                }
            }

            impl<'a> std::fmt::Debug for [<$wrapper Ref>]<'a> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}Ref({})", stringify!($wrapper), self.get())
                }
            }

            impl<'a> std::fmt::Display for [<$wrapper Ref>]<'a> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}Ref({})", stringify!($wrapper), self.get())
                }
            }

            impl<'a> PartialEq<$inner> for [<$wrapper Ref>]<'a> {
                fn eq(&self, other: &$inner) -> bool { self.get() == *other }
            }

            impl<'a> PartialEq for [<$wrapper Ref>]<'a> {
                fn eq(&self, other: &Self) -> bool { self.get() == other.get() }
            }

            impl<'a> PartialEq<[<$wrapper Ref>]<'a>> for $inner {
                fn eq(&self, other: &[<$wrapper Ref>]<'a>) -> bool { *self == other.get() }
            }


            impl<'a> From<&'a [u8]> for [<$wrapper Ref>]<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        Self::from_bytes(bytes).expect("invalid bytes")
    }
}



            impl<'a> PartialOrd<$inner> for [<$wrapper Ref>]<'a> {
    fn partial_cmp(&self, other: &$inner) -> Option<std::cmp::Ordering> {
        self.get().partial_cmp(other)
    }
}

impl<'a> PartialOrd for [<$wrapper Ref>]<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.get().partial_cmp(&other.get())
    }
}

impl<'a> PartialOrd<[<$wrapper Ref>]<'a>> for $inner {
    fn partial_cmp(&self, other: &[<$wrapper Ref>]<'a>) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other.get())
    }
}

            impl<'a> $crate::types::core::AxmosValueTypeRef<'a> for [<$wrapper Ref>]<'a> {
                type Owned = $wrapper;
                fn to_owned(&self) -> Self::Owned { self.to_owned() }
                fn as_bytes(&self) -> &[u8] { self.as_raw_bytes() }
            }

            /// Mutable reference
            #[repr(transparent)]
            pub struct [<$wrapper RefMut>]<'a> {
                bytes: &'a mut [<$wrapper Bytes>],
            }

            impl<'a> [<$wrapper RefMut>]<'a> {
                pub const SIZE: usize = std::mem::size_of::<$inner>();

                pub fn from_bytes(bytes: &'a mut [u8]) -> std::io::Result<Self> {
                    use std::io::{Error, ErrorKind};
                    if bytes.len() < Self::SIZE {
                        return Err(Error::new(ErrorKind::UnexpectedEof, "not enough bytes"));
                    }

                    let prefix = &mut bytes[..Self::SIZE];
                    let bytes_elem: &mut [<$wrapper Bytes>] =
                        bytemuck::try_from_bytes_mut(prefix)
                            .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid bytes for Pod"))?;

                    Ok(Self { bytes: bytes_elem })
                }

                #[inline]
                pub fn get(&self) -> $inner { <$inner>::from_le_bytes(self.bytes.0) }

                #[inline]
                pub fn set(&mut self, value: $inner) {
                    self.bytes.0.copy_from_slice(&value.to_le_bytes());
                }

                #[inline]
                pub fn update<F>(&mut self, f: F)
                where F: FnOnce($inner) -> $inner {
                    let new = f(self.get());
                    self.set(new);
                }

                #[inline]
                pub fn to_owned(&self) -> $wrapper { $wrapper(self.get()) }

                #[inline]
                pub fn as_raw_bytes(&self) -> &[u8] { bytemuck::bytes_of(self.bytes) }

                #[inline]
                pub fn as_raw_bytes_mut(&mut self) -> &mut [u8] { bytemuck::bytes_of_mut(self.bytes) }
            }

            impl<'a> std::fmt::Debug for [<$wrapper RefMut>]<'a> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}RefMut({})", stringify!($wrapper), self.get())
                }
            }

            impl<'a> PartialEq<$inner> for [<$wrapper RefMut>]<'a> {
                fn eq(&self, other: &$inner) -> bool { self.get() == *other }
            }

            impl<'a> PartialEq for [<$wrapper RefMut>]<'a> {
                fn eq(&self, other: &Self) -> bool { self.get() == other.get() }
            }

            impl<'a> PartialEq<[<$wrapper RefMut>]<'a>> for $inner {
                fn eq(&self, other: &[<$wrapper RefMut>]<'a>) -> bool { *self == other.get() }
            }


                impl<'a> From<&'a mut [u8]> for [<$wrapper RefMut>]<'a> {
    fn from(bytes: &'a mut [u8]) -> Self {
        Self::from_bytes(bytes).expect("invalid bytes")
    }
}


            impl<'a> PartialOrd<$inner> for [<$wrapper RefMut>]<'a> {
    fn partial_cmp(&self, other: &$inner) -> Option<std::cmp::Ordering> {
        self.get().partial_cmp(other)
    }
}

impl<'a> PartialOrd for [<$wrapper RefMut>]<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.get().partial_cmp(&other.get())
    }
}

impl<'a> PartialOrd<[<$wrapper RefMut>]<'a>> for $inner {
    fn partial_cmp(&self, other: &[<$wrapper RefMut>]<'a>) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other.get())
    }
}


            impl<'a> $crate::types::core::AxmosValueTypeRefMut<'a> for [<$wrapper RefMut>]<'a> {
                type Owned = $wrapper;
                fn to_owned(&self) -> Self::Owned { self.to_owned() }
                fn as_bytes(&self) -> &[u8] { self.as_raw_bytes() }
                fn as_bytes_mut(&mut self) -> &mut [u8] { self.as_raw_bytes_mut() }
            }

            /// Conversion helpers
            impl [<$wrapper Bytes>] {
                pub fn from_value(value: $inner) -> Self { Self(value.to_le_bytes()) }
                pub fn to_value(&self) -> $inner { <$inner>::from_le_bytes(self.0) }
                pub fn as_bytes(&self) -> &[u8] { &self.0 }
                pub fn as_mut_bytes(&mut self) -> &mut [u8] { &mut self.0 }
            }

            impl From<$inner> for [<$wrapper Bytes>] { fn from(value: $inner) -> Self { Self::from_value(value) } }
            impl From<[<$wrapper Bytes>]> for $inner { fn from(b: [<$wrapper Bytes>]) -> Self { b.to_value() } }
            impl AsRef<[u8]> for [<$wrapper Bytes>] { fn as_ref(&self) -> &[u8] { &self.0 } }
            impl AsMut<[u8]> for [<$wrapper Bytes>] { fn as_mut(&mut self) -> &mut [u8] { &mut self.0 } }
        }
    };
}

/// Implements AxmosValueType for a fixed-size numeric wrapper type
#[macro_export]
macro_rules! impl_axmos_value_type_fixed {
    ($wrapper:ident, $inner:ty, numeric = $is_numeric:expr) => {
        paste::paste! {
            impl $crate::types::core::AxmosValueType for $wrapper {
                type Ref<'a> = [<$wrapper Ref>]<'a>;
                type RefMut<'a> = [<$wrapper RefMut>]<'a>;

                const FIXED_SIZE: Option<usize> = Some(std::mem::size_of::<$inner>());
                const IS_NUMERIC: bool = $is_numeric;
                const IS_SIGNED: bool  = <Self as $crate::types::core::NumericType>::IS_SIGNED;
                fn reinterpret(buffer: &[u8]) -> std::io::Result<(Self::Ref<'_>, usize)> {
                    let r = [<$wrapper Ref>]::from_bytes(buffer)?;
                    Ok((r, Self::SIZE))
                }

                fn reinterpret_mut(buffer: &mut [u8]) -> std::io::Result<(Self::RefMut<'_>, usize)> {
                    let r = [<$wrapper RefMut>]::from_bytes(buffer)?;
                    Ok((r, Self::SIZE))
                }

                fn value_size(&self) -> usize {
                    Self::SIZE
                }
            }

            impl $crate::types::core::FixedSizeType for $wrapper {
                const SIZE: usize = std::mem::size_of::<$inner>();
            }
        }
    };
}

#[macro_export]
macro_rules! recast {
    ($wrapper:ident, $inner:ty) => {
        impl TryFrom<&[u8]> for $wrapper {
            type Error = std::io::Error;

            fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
                use std::convert::TryInto;
                use std::io::{Error, ErrorKind};

                if value.len() < Self::SIZE {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "not enough bytes"));
                }

                let arr: [u8; Self::SIZE] = value[0..Self::SIZE]
                    .try_into()
                    .map_err(|_| Error::new(ErrorKind::InvalidData, "failed to copy bytes"))?;

                Ok(Self(<$inner>::from_le_bytes(arr)))
            }
        }

        impl TryFrom<&mut [u8]> for $wrapper {
            type Error = std::io::Error;

            fn try_from(value: &mut [u8]) -> Result<Self, Self::Error> {
                use std::convert::TryInto;
                use std::io::{Error, ErrorKind};

                if value.len() < Self::SIZE {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "not enough bytes"));
                }

                let arr: [u8; Self::SIZE] = value[0..Self::SIZE]
                    .try_into()
                    .map_err(|_| Error::new(ErrorKind::InvalidData, "failed to copy bytes"))?;

                Ok(Self(<$inner>::from_le_bytes(arr)))
            }
        }

        impl AsMut<[u8]> for $wrapper {
            fn as_mut(&mut self) -> &mut [u8] {
                unsafe {
                    std::slice::from_raw_parts_mut(
                        &mut self.0 as *mut $inner as *mut u8,
                        std::mem::size_of::<$inner>(),
                    )
                }
            }
        }

        impl AsRef<[u8]> for $wrapper {
            fn as_ref(&self) -> &[u8] {
                unsafe {
                    std::slice::from_raw_parts(
                        &self.0 as *const $inner as *const u8,
                        std::mem::size_of::<$inner>(),
                    )
                }
            }
        }
    };
}

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

        $crate::numeric!($name, $inner);
        $crate::arith!($name, $inner);
        $crate::as_slice!($name);
    };
}

/// Defines a float wrapper type with full trait implementations.
/// Generates: owned type, Ref type, RefMut type, and AxmosValueType impl.
#[macro_export]
macro_rules! float {
    (
        $(#[$meta:meta])*
        pub struct $name:ident($inner:ty);
    ) => {
        $(#[$meta])*
        #[derive(Copy, Clone, PartialEq, PartialOrd, Default)]
        #[repr(transparent)]
        pub struct $name(pub $inner);

        $crate::numeric!($name, $inner);
        $crate::recast!($name, $inner);
        $crate::arith!($name, $inner);
        $crate::impl_ref!($name, $inner);
        $crate::impl_axmos_value_type_fixed!($name, $inner, numeric = true);

        // Implement NumericType
        impl $crate::types::core::NumericType for $name {
            const IS_SIGNED: bool = true;
        }

    };
}

#[macro_export]
macro_rules! signed_integer {
    (
        $(#[$meta:meta])*
        pub struct $name:ident($inner:ty);
    ) => {
        $(#[$meta])*
        #[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        #[repr(transparent)]
        pub struct $name(pub $inner);

        $crate::numeric!($name, $inner);
        $crate::recast!($name, $inner);
        $crate::arith!($name, $inner);
        $crate::impl_ref!($name, $inner);
        $crate::impl_axmos_value_type_fixed!($name, $inner, numeric = true);

        // Implement NumericType
        impl $crate::types::core::NumericType for $name {
            const IS_SIGNED: bool = true;
        }

    };
}

#[macro_export]
macro_rules! unsigned_integer {
    (
        $(#[$meta:meta])*
        pub struct $name:ident($inner:ty);
    ) => {
        $(#[$meta])*
        #[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        #[repr(transparent)]
        pub struct $name(pub $inner);

        $crate::numeric!($name, $inner);
        $crate::recast!($name, $inner);
        $crate::arith!($name, $inner);
        $crate::impl_ref!($name, $inner);
        $crate::impl_axmos_value_type_fixed!($name, $inner, numeric = true);

        // Implement NumericType
        impl $crate::types::core::NumericType for $name {
            const IS_SIGNED: bool = false;
        }

    };
}
