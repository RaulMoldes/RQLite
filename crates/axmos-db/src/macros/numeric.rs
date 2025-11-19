/// Macro to implement arithmetic operations for numeric types
/// Can implement operations between the wrapper and itself, or between wrapper and another type
#[macro_export]
macro_rules! arith {
    // Entry point for implementing ops between wrapper and itself
    ($($wrapper:ident, $inner:ty),+ $(,)?) => {
        $(
            // Binary operations between wrapper instances
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

            // Assignment operations between wrapper instances
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
        )+
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

            // Immutable reference type
            #[repr(transparent)]
            pub struct [<$wrapper Ref>]<'a> {
                pub bytes: &'a [u8; std::mem::size_of::<$inner>()],
            }

            impl<'a> [<$wrapper Ref>]<'a> {
                pub const SIZE: usize = std::mem::size_of::<$inner>();


                pub fn from_bytes(bytes: &'a [u8]) -> std::io::Result<Self> {
                    use std::io::{Error, ErrorKind};

                    if bytes.len() < Self::SIZE {
                        return Err(Error::new(ErrorKind::UnexpectedEof, "not enough bytes"));
                    }

                    let ptr = bytes.as_ptr();
                    let bytes = unsafe {
                        &*(std::ptr::slice_from_raw_parts(ptr, Self::SIZE) as *const [u8; std::mem::size_of::<$inner>()])
                    };

                    Ok(Self { bytes })
                }

                #[inline]
                pub fn get(&self) -> $inner {
                    <$inner>::from_le_bytes(*self.bytes)
                }

                #[inline]
                pub fn to_owned(&self) -> $wrapper {
                    $wrapper(self.get())
                }
            }


            impl<'a> TryFrom<&'a [u8]> for [<$wrapper Ref>]<'a> {
                type Error = std::io::Error;
                fn try_from(value: &'a [u8]) -> std::io::Result<Self> {
                    Self::from_bytes(value)
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

            impl<'a> std::ops::Deref for [<$wrapper Ref>]<'a> {
                type Target = $inner;
                fn deref(&self) -> &Self::Target {
                    unsafe { &*(self.bytes.as_ptr() as *const $inner) }
                }
            }

            impl<'a> AsRef<[u8]> for [<$wrapper Ref>]<'a> {
                fn as_ref(&self) -> &[u8] {
                    self.bytes
                }
            }

            impl<'a> AsRef<[<$wrapper Ref>]<'a>> for $wrapper {
                fn as_ref(&self) -> &[<$wrapper Ref>]<'a> {
                    unsafe { &*(self.as_ref() as *const [u8] as *const [<$wrapper Ref>]<'_>) }
                }
            }

            impl<'a> PartialEq<$inner> for [<$wrapper Ref>]<'a> {
                fn eq(&self, other: &$inner) -> bool {
                    self.get() == *other
                }
            }

            impl<'a> PartialEq<[<$wrapper Ref>]<'a>> for $inner {
                fn eq(&self, other: &[<$wrapper Ref>]<'a>) -> bool {
                    *self == other.get()
                }
            }

            impl<'a> PartialEq for [<$wrapper Ref>]<'a> {
                fn eq(&self, other: &Self) -> bool {
                    self.get() == other.get()
                }
            }

            // Mutable reference type
            #[repr(transparent)]
            pub struct [<$wrapper RefMut>]<'a> {
                bytes: &'a mut [u8; std::mem::size_of::<$inner>()],
            }

            impl<'a> [<$wrapper RefMut>]<'a> {
                pub const SIZE: usize = std::mem::size_of::<$inner>();

                pub fn from_bytes(bytes: &'a mut [u8]) -> std::io::Result<Self> {
                    use std::io::{Error, ErrorKind};

                    if bytes.len() < Self::SIZE {
                        return Err(Error::new(ErrorKind::UnexpectedEof, "not enough bytes"));
                    }

                    let ptr = bytes.as_mut_ptr();
                    let bytes = unsafe {
                        &mut *(std::ptr::slice_from_raw_parts_mut(ptr, Self::SIZE) as *mut [u8; std::mem::size_of::<$inner>()])
                    };

                    Ok(Self { bytes })
                }

                #[inline]
                pub fn get(&self) -> $inner {
                    <$inner>::from_le_bytes(*self.bytes)
                }

                #[inline]
                pub fn to_owned(&self) -> $wrapper {
                    $wrapper(self.get())
                }


                #[inline]
                pub fn set(&mut self, value: $inner) {
                    self.bytes.copy_from_slice(&value.to_le_bytes());
                }

                #[inline]
                pub fn set_from(&mut self, wrapper: $wrapper) {
                    self.set(wrapper.0);
                }

                #[inline]
                pub fn update<F>(&mut self, f: F)
                where
                    F: FnOnce($inner) -> $inner,
                {
                    let current = self.get();
                    let new_value = f(current);
                    self.set(new_value);
                }
            }


            impl<'a> std::ops::AddAssign<$inner> for [<$wrapper RefMut>]<'a> {
                fn add_assign(&mut self, rhs: $inner) {
                    self.update(|v| v + rhs);
                }
            }

            impl<'a> AsRef<[u8]> for [<$wrapper RefMut>]<'a> {
                fn as_ref(&self) -> &[u8] {
                    self.bytes
                }
            }


            impl<'a> AsMut<[u8]> for [<$wrapper RefMut>]<'a> {
                fn as_mut(&mut self) -> &mut [u8] {
                    self.bytes
                }
            }

            impl<'a> std::ops::SubAssign<$inner> for [<$wrapper RefMut>]<'a> {
                fn sub_assign(&mut self, rhs: $inner) {
                    self.update(|v| v - rhs);
                }
            }

            impl<'a> std::ops::MulAssign<$inner> for [<$wrapper RefMut>]<'a> {
                fn mul_assign(&mut self, rhs: $inner) {
                    self.update(|v| v * rhs);
                }
            }

            impl<'a> std::ops::DivAssign<$inner> for [<$wrapper RefMut>]<'a> {
                fn div_assign(&mut self, rhs: $inner) {
                    self.update(|v| v / rhs);
                }
            }

            impl<'a> std::ops::RemAssign<$inner> for [<$wrapper RefMut>]<'a> {
                fn rem_assign(&mut self, rhs: $inner) {
                    self.update(|v| v % rhs);
                }
            }

            // Assignment from wrapper type
            impl<'a> std::ops::AddAssign<$wrapper> for [<$wrapper RefMut>]<'a> {
                fn add_assign(&mut self, rhs: $wrapper) {
                    self.update(|v| v + rhs.0);
                }
            }

            impl<'a> std::ops::SubAssign<$wrapper> for [<$wrapper RefMut>]<'a> {
                fn sub_assign(&mut self, rhs: $wrapper) {
                    self.update(|v| v - rhs.0);
                }
            }

            impl<'a> std::ops::MulAssign<$wrapper> for [<$wrapper RefMut>]<'a> {
                fn mul_assign(&mut self, rhs: $wrapper) {
                    self.update(|v| v * rhs.0);
                }
            }

            impl<'a> std::ops::DivAssign<$wrapper> for [<$wrapper RefMut>]<'a> {
                fn div_assign(&mut self, rhs: $wrapper) {
                    self.update(|v| v / rhs.0);
                }
            }

            impl<'a> std::ops::RemAssign<$wrapper> for [<$wrapper RefMut>]<'a> {
                fn rem_assign(&mut self, rhs: $wrapper) {
                    self.update(|v| v % rhs.0);
                }
            }


            impl<'a> TryFrom<&'a mut [u8]> for [<$wrapper RefMut>]<'a> {
                type Error = std::io::Error;
                fn try_from(value: &'a mut [u8]) -> std::io::Result<Self> {
                    Self::from_bytes(value)
                }
            }

            impl<'a> std::fmt::Debug for [<$wrapper RefMut>]<'a> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}RefMut({})", stringify!($wrapper), self.get())
                }
            }

            impl<'a> std::fmt::Display for [<$wrapper RefMut>]<'a> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}Ref({})", stringify!($wrapper), self.get())
                }
            }

            impl<'a> std::ops::Deref for [<$wrapper RefMut>]<'a> {
                type Target = $inner;
                fn deref(&self) -> &Self::Target {
                    unsafe { &*(self.bytes.as_ptr() as *const $inner) }
                }
            }

            impl<'a> std::ops::DerefMut for [<$wrapper RefMut>]<'a> {
                fn deref_mut(&mut self) -> &mut Self::Target {
                    unsafe { &mut *(self.bytes.as_mut_ptr() as *mut $inner) }
                }
            }

            impl<'a> PartialEq<$inner> for [<$wrapper RefMut>]<'a> {
                fn eq(&self, other: &$inner) -> bool {
                    self.get() == *other
                }
            }

            impl<'a> PartialEq<[<$wrapper RefMut>]<'a>> for $inner {
                fn eq(&self, other: &[<$wrapper RefMut>]<'a>) -> bool {
                    *self == other.get()
                }
            }

            impl<'a> PartialEq for [<$wrapper RefMut>]<'a> {
                fn eq(&self, other: &Self) -> bool {
                    self.get() == other.get()
                }
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

    };
}

#[macro_export]
macro_rules! integer {
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


    };
}

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


    };
}
