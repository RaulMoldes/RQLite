//! Core trait for Axmos value types.
//!
//! This trait defines the interface that all inner types must implement
//! to participate in the DataType system. The derive macro uses trait
//! dispatch exclusively

use std::io::Result;

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

/// Marker trait for numeric types that support coercion.
pub trait NumericType: AxmosValueType {}
