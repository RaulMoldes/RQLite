//! Core trait for Axmos value types.
//!
//! This trait defines the interface that all inner types must implement
//! to participate in the DataType system. The derive macro uses trait
//! dispatch exclusively

use crate::impl_number_op;
use murmur3::murmur3_x64_128;
use std::io::{Cursor, Result};
/// Core trait for all Axmos value types.
///
/// The derive macro dispatches all operations through this trait,
/// enabling fully generic handling of any conforming type.
pub trait AxmosValueType: Sized {
    /// The immutable reference type for this value
    type Ref<'a>: AxmosValueTypeRef<'a, Owned = Self>
    where
        Self: 'a;

    /// The mutable reference type for this value
    type RefMut<'a>: AxmosValueTypeRefMut<'a, Owned = Self>
    where
        Self: 'a;

    /// Fixed size in bytes, if this is a fixed-size type.
    /// Returns `None` for dynamic-size types (e.g., Blob, Text).
    const FIXED_SIZE: Option<usize>;

    /// Whether this type is considered numeric for coercion purposes.
    const IS_NUMERIC: bool = false;

    /// Wether this type is signed or not
    const NUM_CLASS: NumClass = NumClass::Unknown;

    /// Reinterpret a byte buffer as this type's reference.
    /// Returns the reference and the number of bytes consumed.
    fn reinterpret(buffer: &[u8]) -> Result<(Self::Ref<'_>, usize)>;

    /// Reinterpret a mutable byte buffer as this type's mutable reference.
    /// Returns the mutable reference and the number of bytes consumed.
    fn reinterpret_mut(buffer: &mut [u8]) -> Result<(Self::RefMut<'_>, usize)>;

    /// Get the size of a specific value instance.
    /// For fixed types, this equals FIXED_SIZE.
    /// For dynamic types, this returns the actual encoded length.
    fn value_size(&self) -> usize;
}

/// Trait for immutable reference types.
pub trait AxmosValueTypeRef<'a>: Sized {
    /// The owned type this reference corresponds to
    type Owned: AxmosValueType;

    /// Convert to owned value
    fn to_owned(&self) -> Self::Owned;

    /// Get the underlying bytes
    fn as_bytes(&self) -> &[u8];

    /// Get the size in bytes
    fn size(&self) -> usize {
        self.as_bytes().len()
    }
}

/// Trait for mutable reference types.
pub trait AxmosValueTypeRefMut<'a>: Sized {
    /// The owned type this reference corresponds to
    type Owned: AxmosValueType;

    /// Convert to owned value
    fn to_owned(&self) -> Self::Owned;

    /// Get the underlying bytes immutably
    fn as_bytes(&self) -> &[u8];

    /// Get the underlying bytes mutably
    fn as_bytes_mut(&mut self) -> &mut [u8];

    /// Get the size in bytes
    fn size(&self) -> usize {
        self.as_bytes().len()
    }
}

/// Marker trait for fixed-size types.
/// This is automatically implemented when FIXED_SIZE is Some.
pub trait FixedSizeType: AxmosValueType {
    /// The fixed size in bytes
    const SIZE: usize;
}

/// Marker trait for dynamic-size types.
pub trait DynamicSizeType: AxmosValueType {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NumClass {
    Signed,
    Unsigned,
    Float,
    Unknown,
}

impl NumClass {
    /// Promote two classes to a common class
    pub(crate) fn promote(self, other: Self) -> Self {
        match (self, other) {
            (NumClass::Float, _) | (_, NumClass::Float) => NumClass::Float,
            (NumClass::Signed, NumClass::Unsigned) | (NumClass::Unsigned, NumClass::Signed) => {
                NumClass::Signed
            }
            (NumClass::Signed, NumClass::Signed) => NumClass::Signed,
            (NumClass::Unsigned, NumClass::Unsigned) => NumClass::Unsigned,
            (_, _) => panic!("Unknown numeric class. Cannot promote"),
        }
    }
}

pub trait AxmosOps: AxmosValueType {
    fn abs(&self) -> Self;
    fn ceil(&self) -> Self;
    fn floor(&self) -> Self;
    fn sqrt(&self) -> Self;
    fn round(&self) -> Self;
    fn pow(&self, other: Self) -> Self;
}

/// Trait for numeric types.
pub trait NumericType: AxmosValueType + Copy {
    /// Classification for type promotion
    const NUM_CLASS: NumClass;

    /// Extract as i64 (for signed types)
    fn as_i64(&self) -> Option<i64>;

    /// Extract as u64 (for unsigned types)
    fn as_u64(&self) -> Option<u64>;

    /// Extract as f64 (for all numeric types)
    fn as_f64(&self) -> f64;

    /// Create from i64
    fn from_i64(v: i64) -> Self
    where
        Self: Sized;

    /// Create from u64
    fn from_u64(v: u64) -> Self
    where
        Self: Sized;

    /// Create from f64
    fn from_f64(v: f64) -> Self
    where
        Self: Sized;
}

/// Trait for type casting between Axmos value types.
///
/// This trait defines how one type can be converted to another.
/// The derive macro uses this for `try_cast` implementation.
///
/// Implementations should be provided externally (not in the macro)
/// to allow flexible casting rules.
pub trait AxmosCastable<Target>: Sized {
    /// Try to cast this value to the target type.
    /// Returns `None` if the cast is not possible or would lose precision.
    fn try_cast(&self) -> Option<Target>;

    /// Check if this value can be cast to the target type.
    fn can_cast(&self) -> bool {
        self.try_cast().is_some()
    }
}

/// Blanket implementation: any type can cast to itself.
impl<T: Clone> AxmosCastable<T> for T {
    #[inline]
    fn try_cast(&self) -> Option<T> {
        Some(self.clone())
    }

    #[inline]
    fn can_cast(&self) -> bool {
        true
    }
}

/// Trait for types that support comparison operations.
/// This is used by the derive macro to determine if a variant supports ordering.
pub trait AxmosComparable: PartialOrd {}

// Blanket implementation for all PartialOrd types
impl<T: PartialOrd> AxmosComparable for T {}

pub trait AxmosHashable {
    fn hash128(&self) -> u128;
}

/// MurmurHash3 (128-bit x64 variant).
pub(crate) fn murmur_hash(bytes: &[u8]) -> u128 {
    let mut cursor = Cursor::new(bytes);
    let buf = [0u8; 16];
    let hash128 = murmur3_x64_128(&mut cursor, 0).unwrap();
    hash128
}

pub(crate) enum Number {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
}

impl_number_op!(number_add, +);
impl_number_op!(number_sub, -);
impl_number_op!(number_mul, *);
impl_number_op!(number_div, /);
impl_number_op!(number_mod, %);

use std::ops::{Add, Div, Mul, Rem, Sub};

impl Add for Number {
    type Output = Number;
    fn add(self, rhs: Number) -> Number {
        number_add(self, rhs)
    }
}

impl Sub for Number {
    type Output = Number;
    fn sub(self, rhs: Number) -> Number {
        number_sub(self, rhs)
    }
}

impl Mul for Number {
    type Output = Number;
    fn mul(self, rhs: Number) -> Number {
        number_mul(self, rhs)
    }
}

impl Div for Number {
    type Output = Number;
    fn div(self, rhs: Number) -> Number {
        number_div(self, rhs)
    }
}

impl Rem for Number {
    type Output = Number;
    fn rem(self, rhs: Number) -> Number {
        number_mod(self, rhs)
    }
}
