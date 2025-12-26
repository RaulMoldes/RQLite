use bytemuck::{Pod, Zeroable};
use std::{
    cmp::Ordering,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    mem,
    ops::{Add, Deref, Div, Mul, Rem, Sub},
};

use crate::{SerializationError, types::SerializationResult};

use murmur3::murmur3_x64_128;
use std::io::Cursor;

/// Trait to define the [TypeClass] of all types in the type system.
/// For a type to be used in the type system it must implement this trait, which forces to also implement [PartialOrd], [PartialEq], [Debug], and [Display].
///
/// Type Classes are [NullTypeClass], [VarlenTypeClass], and [NumericTypeClass].
///
/// The only special one is the numeric type, which has set of extra trait bounds in order to support arithmetic operations.
pub trait TypeClass: Sized + Debug + Display + PartialEq + PartialOrd {
    const SIZE: Option<usize> = None; // Size is unknown by default.
    const ALIGN: usize = 1;
}

/// Varlen Type's size stays unknown.
pub trait VarlenType: TypeClass {}

/// TypeClass representing all fixed size types.
pub trait FixedSizeType: TypeClass + Copy {
    #[inline]
    fn mem_size() -> usize {
        Self::SIZE.unwrap_or(0)
    }

    #[inline]
    fn aligned_offset(offset: usize) -> usize {
        (offset + Self::ALIGN - 1) & !(Self::ALIGN - 1)
    }
}

/// All fixed sizes are known at compile time.
impl<T: FixedSizeType> TypeClass for T {
    const SIZE: Option<usize> = Some(mem::size_of::<T>());
    const ALIGN: usize = mem::align_of::<T>();
}

/// Trait for Boolean types
pub trait BooleanType: FixedSizeType {}

/// Numeric types do have a fixed size, and also some extra constraints.
pub trait NumericType: FixedSizeType {
    /// Every numeric type has an associated [Primitive] type to which it can promote to.
    type Primitive: Copy
        + Add<Output = Self::Primitive>
        + Sub<Output = Self::Primitive>
        + Mul<Output = Self::Primitive>
        + Div<Output = Self::Primitive>
        + Rem<Output = Self::Primitive>;

    /// Convert from the target type to the primitive type.
    fn to_primitive(self) -> Self::Primitive;

    /// Convert back from the primitive type to the target type.
    fn from_primitive(value: Self::Primitive) -> Self;
}

/// Numeric types specializations.
pub trait SignedIntType: NumericType<Primitive = i64> {}
pub trait UnsignedIntType: NumericType<Primitive = u64> {}
pub trait FloatType: NumericType<Primitive = f64> {}

/// Trait to generate promotion tables for numeric types.
pub trait Promote<Rhs>: NumericType {
    type Output: Add<Output = Self::Output>
        + Sub<Output = Self::Output>
        + Mul<Output = Self::Output>
        + Div<Output = Self::Output>
        + Rem<Output = Self::Output>
        + Copy;

    fn promote_lhs(self) -> Self::Output;
    fn promote_rhs(rhs: Rhs) -> Self::Output;
}

/// Arithmetic operations over the promoted types.
pub trait PromotedAdd<Rhs = Self> {
    type Output;
    fn promoted_add(self, rhs: Rhs) -> Self::Output;
}

pub trait PromotedSub<Rhs = Self> {
    type Output;
    fn promoted_sub(self, rhs: Rhs) -> Self::Output;
}

pub trait PromotedMul<Rhs = Self> {
    type Output;
    fn promoted_mul(self, rhs: Rhs) -> Self::Output;
}

pub trait PromotedDiv<Rhs = Self> {
    type Output;
    fn promoted_div(self, rhs: Rhs) -> Self::Output;
}

pub trait PromotedRem<Rhs = Self> {
    type Output;
    fn promoted_rem(self, rhs: Rhs) -> Self::Output;
}

// Blanket impls for promoted operations.
impl<L, R> PromotedAdd<R> for L
where
    L: Promote<R>,
{
    type Output = L::Output;
    fn promoted_add(self, rhs: R) -> Self::Output {
        L::promote_lhs(self) + L::promote_rhs(rhs)
    }
}

impl<L, R> PromotedSub<R> for L
where
    L: Promote<R>,
{
    type Output = L::Output;
    fn promoted_sub(self, rhs: R) -> Self::Output {
        L::promote_lhs(self) - L::promote_rhs(rhs)
    }
}

impl<L, R> PromotedMul<R> for L
where
    L: Promote<R>,
{
    type Output = L::Output;
    fn promoted_mul(self, rhs: R) -> Self::Output {
        L::promote_lhs(self) * L::promote_rhs(rhs)
    }
}

impl<L, R> PromotedDiv<R> for L
where
    L: Promote<R>,
{
    type Output = L::Output;
    fn promoted_div(self, rhs: R) -> Self::Output {
        L::promote_lhs(self) / L::promote_rhs(rhs)
    }
}

impl<L, R> PromotedRem<R> for L
where
    L: Promote<R>,
{
    type Output = L::Output;
    fn promoted_rem(self, rhs: R) -> Self::Output {
        L::promote_lhs(self) % L::promote_rhs(rhs)
    }
}

pub trait BytemuckDeserializable: Pod + Zeroable + FixedSizeType {}

/// Automatically generates a [RefType] for all types that implement [bytemuck's] [Pod] and [Zeroable] traits.
///
/// The list of primitive types that implement this are found here: https://docs.rs/bytemuck/latest/bytemuck/trait.Pod.html
///
/// Bool and char do not implement [Pod] because its implementation smay be illegal, which is why this implementation is separated from the [FixedSizeType].
///
/// The target [T] type must be both [Pod] and [Zeroable]
#[repr(transparent)]
#[derive(Copy, Clone)]
pub struct BytemuckRef<'a, T: BytemuckDeserializable>(&'a T);

impl<'a, T: BytemuckDeserializable> BytemuckRef<'a, T> {
    #[inline]
    pub fn get(&self) -> &T {
        self.0
    }

    #[inline]
    pub fn to_owned(&self) -> T {
        *self.0
    }
}

impl<'a, T: BytemuckDeserializable> Deref for BytemuckRef<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, T: BytemuckDeserializable> AsRef<[u8]> for BytemuckRef<'a, T> {
    fn as_ref(&self) -> &[u8] {
        bytemuck::bytes_of(self.0)
    }
}

impl<'a, T: BytemuckDeserializable> TryFrom<&'a [u8]> for BytemuckRef<'a, T> {
    type Error = SerializationError;
    // We use
    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        if value.len() < T::mem_size() {
            return Err(SerializationError::UnexpectedEof);
        }

        let bytes = bytemuck::try_from_bytes::<T>(value)?;
        Ok(Self(bytes))
    }
}

/// The types are equal if their byte patterns are the same.
impl<'a, T: BytemuckDeserializable> PartialEq for BytemuckRef<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<'a, T: BytemuckDeserializable> Eq for BytemuckRef<'a, T> {}

impl<'a, T: BytemuckDeserializable> PartialOrd for BytemuckRef<'a, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(other.0)
    }
}

impl<'a, T: BytemuckDeserializable + Ord> Ord for BytemuckRef<'a, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(other.0)
    }
}

impl<'a, T: BytemuckDeserializable> Debug for BytemuckRef<'a, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "Ref({:?})", self.0)
    }
}

impl<'a, T: BytemuckDeserializable> Display for BytemuckRef<'a, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Display::fmt(self.0, f)
    }
}

/// Trait to indicate which is the Ref::type of each inner type in the system
pub trait RefTrait: TypeClass {
    type Ref<'a>: TypeRef<'a, Owned = Self>
    where
        Self: 'a;
}
/// Trait for making types deserializable.
pub trait DeserializableType: RefTrait {
    /// Reinterprets a buffer slice into a  its [Ref] type.
    ///
    /// For fixed size numeric types we are leveraging [bytemuck] to do this, while [Blob]
    ///
    /// handles its deserialization separately.
    fn reinterpret_cast(buffer: &[u8]) -> SerializationResult<(Self::Ref<'_>, usize)>;

    /// Deserializes a type from a buffer, taking into account alignment requirements
    ///
    /// Instead of returning the bytes read, returns the new cursor position
    fn deserialize(buffer: &[u8], cursor: usize) -> SerializationResult<(Self::Ref<'_>, usize)>;
}

/// Trait for making types deserializable.
///
/// If the type implements AsRef this is automatically derived. If not (like for booleans), it has to be manually implemented.
pub trait SerializableType: TypeClass {
    /// Serializes the data type to a byte slice.
    fn write_to(&self, writer: &mut [u8], cursor: usize) -> SerializationResult<usize>;
}

impl<T: TypeClass + BytemuckDeserializable + AsRef<[u8]>> SerializableType for T {
    fn write_to(&self, writer: &mut [u8], cursor: usize) -> SerializationResult<usize> {
        let aligned = Self::aligned_offset(cursor);
        let data_bytes = self.as_ref();
        writer[aligned..aligned + Self::mem_size()].copy_from_slice(data_bytes);

        Ok(aligned + data_bytes.len())
    }
}

impl<T: BytemuckDeserializable> RefTrait for T {
    type Ref<'a>
        = BytemuckRef<'a, T>
    where
        T: 'a;
}

/// All bytemuck serializable types will implement [DeserialiableType]
impl<T: BytemuckDeserializable> DeserializableType for T {
    fn reinterpret_cast(buffer: &[u8]) -> SerializationResult<(Self::Ref<'_>, usize)> {
        Ok((
            BytemuckRef::try_from(&buffer[..Self::SIZE.unwrap_or(0)])?,
            Self::SIZE.unwrap_or(0),
        ))
    }

    fn deserialize(buffer: &[u8], cursor: usize) -> SerializationResult<(Self::Ref<'_>, usize)> {
        let cursor = Self::aligned_offset(cursor);
        let (data_ref, bytes_read) = Self::reinterpret_cast(&buffer[cursor..])?;
        Ok((data_ref, cursor + bytes_read))
    }
}

pub trait TypeRef<'a>: Sized + AsRef<[u8]> {
    type Owned: DeserializableType;
    fn to_owned(&self) -> Self::Owned;
    fn as_slice(&self) -> &[u8];
}

impl<'a, T: BytemuckDeserializable + DeserializableType> TypeRef<'a> for BytemuckRef<'a, T> {
    type Owned = T;
    fn to_owned(&self) -> Self::Owned {
        BytemuckRef::to_owned(self)
    }

    fn as_slice(&self) -> &[u8] {
        self.as_ref()
    }
}

pub trait RuntimeSized: TypeClass {
    fn runtime_size(&self) -> usize {
        Self::SIZE.unwrap_or(0)
    }
}

pub trait TypeCast<T: TypeClass> {
    fn try_cast(&self) -> Option<T>;
    fn can_cast(&self) -> bool {
        self.try_cast().is_some()
    }
}

impl<T: TypeClass + Copy> TypeCast<T> for T {
    fn try_cast(&self) -> Option<T> {
        Some(*self)
    }
}

pub trait Hashable {
    fn hash128(&self) -> u128;

    fn hash64(&self) -> u64 {
        self.hash128() as u64
    }
}

impl<T: AsRef<[u8]>> Hashable for T {
    fn hash128(&self) -> u128 {
        let mut cursor = Cursor::new(self.as_ref());
        murmur3_x64_128(&mut cursor, 0).unwrap()
    }
}

impl<T: FixedSizeType> RuntimeSized for T {
    fn runtime_size(&self) -> usize {
        T::SIZE.unwrap_or(0)
    }
}
