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

        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }


        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}: {}", stringify!($name), self.0)
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


        impl std::ops::Mul<usize> for $name {
            type Output = $name;
            fn mul(self, rhs: usize) -> Self::Output {
                $name(self.0 * rhs as $inner)
            }
        }

        impl std::ops::AddAssign<usize> for $name {
            fn add_assign(&mut self, rhs: usize) {
                self.0 += rhs as $inner;
            }
        }

        impl std::ops::SubAssign<usize> for $name {
            fn sub_assign(&mut self, rhs: usize) {
                self.0 -= rhs as $inner;
            }
        }

        impl std::ops::MulAssign<usize> for $name {
            fn mul_assign(&mut self, rhs: usize) {
                self.0 *= rhs as $inner;
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
macro_rules! integer {
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

        }


        // TryFrom for owned value requires a copy
        impl TryFrom<&[u8]> for $name {
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


        impl TryFrom<&mut [u8]> for $name {
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


        impl AsRef<[u8]> for $name {
        fn as_ref(&self) -> &[u8] {
            unsafe {
                std::slice::from_raw_parts(
                    &self.0 as *const $inner as *const u8,
                    std::mem::size_of::<$inner>(),
                )
                 }
            }
        }


        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }


        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}: {}", stringify!($name), self.0)
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


        impl std::ops::Mul<usize> for $name {
            type Output = $name;
            fn mul(self, rhs: usize) -> Self::Output {
                $name(self.0 * rhs as $inner)
            }
        }

        impl std::ops::AddAssign<usize> for $name {
            fn add_assign(&mut self, rhs: usize) {
                self.0 += rhs as $inner;
            }
        }

        impl std::ops::SubAssign<usize> for $name {
            fn sub_assign(&mut self, rhs: usize) {
                self.0 -= rhs as $inner;
            }
        }

        impl std::ops::MulAssign<usize> for $name {
            fn mul_assign(&mut self, rhs: usize) {
                self.0 *= rhs as $inner;
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

        paste::paste! {
            #[repr(transparent)]
            pub struct [<$name Ref>]<'a> {
                bytes: &'a [u8; std::mem::size_of::<$inner>()],
            }

            impl<'a> [<$name Ref>]<'a> {
                pub const SIZE: usize = std::mem::size_of::<$inner>();

                /// Safe recasting from bytes.
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
                pub fn to_owned(&self) -> $name {
                    $name(self.get())
                }

            }

            impl<'a> TryFrom<&'a [u8]> for [<$name Ref>]<'a> {
                type Error = std::io::Error;
                fn try_from(value: &'a [u8]) -> std::io::Result<Self> {
                    Self::from_bytes(value)
                }
            }

            impl<'a> std::fmt::Debug for [<$name Ref>]<'a> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}Ref({})", stringify!($name), self.get())
                }
            }

            impl<'a> std::fmt::Display for [<$name Ref>]<'a> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}Ref({})", stringify!($name), self.get())
                }
            }


            impl<'a> std::ops::Deref for [<$name Ref>]<'a> {
                type Target = $inner;
                fn deref(&self) -> &Self::Target {
                    unsafe { &*(self.bytes.as_ptr() as *const $inner) }
                }
            }

            impl<'a> AsRef<[u8]> for [<$name Ref>]<'a> {
                fn as_ref(&self) -> &[u8] { self.bytes }
            }

            impl<'a> AsRef<[<$name Ref>]<'a>> for $name {
                fn as_ref(&self) -> &[<$name Ref>]<'a> {
                    unsafe { &*(self.as_ref() as *const [u8] as *const [<$name Ref>]<'_>) }
                }
            }


             impl<'a> PartialEq<$inner> for [<$name Ref>]<'a> {
                fn eq(&self, other: &$inner) -> bool {
                    self.get() == *other
                }
            }

            impl<'a> PartialEq<[<$name Ref>]<'a>> for $inner {
                fn eq(&self, other: &[<$name Ref>]<'a>) -> bool {
                    *self == other.get()
                }
            }

               impl<'a> PartialEq for [<$name Ref>]<'a> {
                fn eq(&self, other: &Self) -> bool {
                        self.get() == other.get()
                }
            }




        }



        paste::paste! {
            #[repr(transparent)]
            pub struct [<$name RefMut>]<'a> {
                bytes: &'a mut [u8; std::mem::size_of::<$inner>()],
            }

            impl<'a> [<$name RefMut>]<'a> {

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
                pub fn to_owned(&self) -> $name {
                    $name(self.get())
                }


                #[inline]
                pub fn get(&self) -> $inner {
                    <$inner>::from_le_bytes(*self.bytes)
                }

            }
            impl <'a> std::ops::Deref for [<$name RefMut>]<'a> {
                type Target = $inner;

                fn deref(&self) -> &Self::Target {
                    unsafe { &*(self.bytes.as_ptr() as *const $inner) }
                }
            }

            impl<'a> std::ops::DerefMut for [<$name RefMut>]<'a> {

            fn deref_mut(&mut self) -> &mut Self::Target {
                unsafe { &mut *(self.bytes.as_mut_ptr() as *mut $inner) }
            }
        }


            impl<'a> TryFrom<&'a mut [u8]> for [<$name RefMut>]<'a> {
                type Error = std::io::Error;
                fn try_from(value: &'a mut [u8]) -> std::io::Result<Self> {
                    Self::from_bytes(value)
                }
            }

            impl<'a> std::fmt::Debug for [<$name RefMut>]<'a> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}RefMut({})", stringify!($name), self.get())
                }
            }

            impl<'a> std::fmt::Display for [<$name RefMut>]<'a> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}Ref({})", stringify!($name), self.get())
                }
            }

            impl<'a> PartialEq<$inner> for [<$name RefMut>]<'a> {
                fn eq(&self, other: &$inner) -> bool {
                    self.get() == *other
                }
            }

            impl<'a> PartialEq<[<$name RefMut>]<'a>> for $inner {
                fn eq(&self, other: &[<$name RefMut>]<'a>) -> bool {
                    *self == other.get()
                }
            }

            impl<'a> PartialEq for [<$name RefMut>]<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}







        }


    };
}

#[macro_export]
macro_rules! float {
    (
        $(#[$meta:meta])*
        pub struct $name:ident($inner:ty);
    ) => {

        $(#[$meta])*
        #[derive(Copy, Clone, PartialEq,  PartialOrd,  Default)]
        #[repr(transparent)]
        pub struct $name(pub $inner);



        impl $name {
            /// Fixed size of the type in bytes
            pub const SIZE: usize = std::mem::size_of::<$inner>();
        }



        // TryFrom for owned value requires a copy
        impl TryFrom<&[u8]> for $name {
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



        impl TryFrom<&mut [u8]> for $name {
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


        impl AsRef<[u8]> for $name {
        fn as_ref(&self) -> &[u8] {
            unsafe {
                std::slice::from_raw_parts(
                    &self.0 as *const $inner as *const u8,
                    std::mem::size_of::<$inner>(),
                )
                 }
            }
        }





        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }


        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}: {}", stringify!($name), self.0)
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

        paste::paste! {
            #[repr(transparent)]
            pub struct [<$name Ref>]<'a> {
                bytes: &'a [u8; std::mem::size_of::<$inner>()],
            }

            impl<'a> [<$name Ref>]<'a> {
                pub const SIZE: usize = std::mem::size_of::<$inner>();
                /// Safe recasting from bytes.
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
                pub fn to_owned(&self) -> $name {
                    $name(self.get())
                }

            }

            impl<'a> TryFrom<&'a [u8]> for [<$name Ref>]<'a> {
                type Error = std::io::Error;
                fn try_from(value: &'a [u8]) -> std::io::Result<Self> {
                    Self::from_bytes(value)
                }
            }

            impl<'a> std::fmt::Debug for [<$name Ref>]<'a> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}Ref({})", stringify!($name), self.get())
                }
            }

            impl<'a> std::fmt::Display for [<$name Ref>]<'a> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}Ref({})", stringify!($name), self.get())
                }
            }

            impl<'a> PartialEq<$inner> for [<$name Ref>]<'a> {
                fn eq(&self, other: &$inner) -> bool {
                    self.get() == *other
                }
            }

            impl<'a> PartialEq<[<$name Ref>]<'a>> for $inner {
                fn eq(&self, other: &[<$name Ref>]<'a>) -> bool {
                    *self == other.get()
                }
            }

            impl<'a> PartialEq for [<$name Ref>]<'a> {
                fn eq(&self, other: &Self) -> bool {
                        self.get() == other.get()
            }
            }



             impl<'a> std::ops::Deref for [<$name Ref>]<'a> {
                type Target = $inner;
                fn deref(&self) -> &Self::Target {
                    unsafe { &*(self.bytes.as_ptr() as *const $inner) }
                }
            }

            impl<'a> AsRef<[u8]> for [<$name Ref>]<'a> {
                fn as_ref(&self) -> &[u8] { self.bytes }
            }

            impl<'a> AsRef<[<$name Ref>]<'a>> for $name {
                fn as_ref(&self) -> &[<$name Ref>]<'a> {
                    unsafe { &*(self.as_ref() as *const [u8] as *const [<$name Ref>]<'_>) }
                }
            }
        }

     paste::paste! {
            #[repr(transparent)]
            pub struct [<$name RefMut>]<'a> {
                bytes: &'a mut [u8; std::mem::size_of::<$inner>()],
            }

            impl<'a> [<$name RefMut>]<'a> {

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
                pub fn to_owned(&self) -> $name {
                    $name(self.get())
                }

            }

            impl<'a> TryFrom<&'a mut [u8]> for [<$name RefMut>]<'a> {
                type Error = std::io::Error;
                fn try_from(value: &'a mut [u8]) -> std::io::Result<Self> {
                    Self::from_bytes(value)
                }
            }

            impl<'a> std::fmt::Debug for [<$name RefMut>]<'a> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}RefMut({})", stringify!($name), self.get())
                }
            }

            impl<'a> std::fmt::Display for [<$name RefMut>]<'a> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}Ref({})", stringify!($name), self.get())
                }
            }

            impl<'a> PartialEq<$inner> for [<$name RefMut>]<'a> {
                fn eq(&self, other: &$inner) -> bool {
                    self.get() == *other
                }
            }

            impl<'a> PartialEq<[<$name RefMut>]<'a>> for $inner {
                fn eq(&self, other: &[<$name RefMut>]<'a>) -> bool {
                    *self == other.get()
                }
            }


               impl<'a> PartialEq for [<$name RefMut>]<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}


            impl <'a> std::ops::Deref for [<$name RefMut>]<'a> {
                type Target = $inner;

                fn deref(&self) -> &Self::Target {
                    unsafe { &*(self.bytes.as_ptr() as *const $inner) }
                }
            }

            impl<'a> std::ops::DerefMut for [<$name RefMut>]<'a> {

                fn deref_mut(&mut self) -> &mut Self::Target {
                    unsafe { &mut *(self.bytes.as_mut_ptr() as *mut $inner) }
                }
            }
        }


    };
}
