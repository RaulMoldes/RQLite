use crate::{
    SerializationResult,
    core::{DeserializableType, RefTrait, RuntimeSized, SerializableType},
    types::{
        SerializationError, TypeSystemResult, VarInt,
        core::{NumericType, TypeCast, TypeClass, TypeRef, VarlenType},
        varint::MAX_VARINT_LEN,
    },
};

use std::{
    cmp::{Ordering, PartialEq},
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    ops::{Deref, DerefMut},
};

// Blob is aligned to 1 byte.
const BLOB_ALIGNMENT: usize = 1;

/// Blob is a self-contained type.
///
/// Includes a VarInt prefix which encodes the length of the actual content in the blob, represented as bytes.
#[derive(Debug, Clone)]
pub struct Blob(Box<[u8]>);

/// Blob is the only Varlen TypeClass in the type system.
impl TypeClass for Blob {
    const ALIGN: usize = BLOB_ALIGNMENT;
}
impl VarlenType for Blob {}

impl Blob {
    /// Creates a [Blob] from an unencoded byte slice.
    ///
    /// Note that the From<&[u8]> implementation assumes that the alice you are passing to the from conversion already has a VarInt prefix. This constructor is intended to create Blobs from unencoded data.
    pub fn from_unencoded_slice(data: &[u8]) -> Self {
        let mut buffer = [0u8; MAX_VARINT_LEN];
        let vlen = VarInt::encode(data.len() as i64, &mut buffer);
        let mut blob_buffer = vlen.to_vec();
        blob_buffer.extend_from_slice(data);
        Blob(blob_buffer.into_boxed_slice())
    }

    /// Get the length of the full byte slice.
    pub fn total_length(&self) -> usize {
        self.0.len()
    }

    /// Returns 0 if the blob holds no data at all.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the offset where the data starts in the [Blob]
    fn data_start(&self) -> TypeSystemResult<usize> {
        let (_, offset) = VarInt::from_encoded_bytes(self.0.as_ref())?;
        Ok(offset)
    }

    /// Gets the length of the data part
    pub fn data_length(&self) -> TypeSystemResult<usize> {
        let (length, _) = VarInt::from_encoded_bytes(self.0.as_ref())?;
        Ok(length.into())
    }

    /// Get a reference to the data content in the [Blob]
    pub fn data(&self) -> TypeSystemResult<&[u8]> {
        let offset = self.data_start()?;
        Ok(&self.0[offset..])
    }

    /// Get a mutable reference to the data content in the [Blob]
    pub fn data_mut(&mut self) -> TypeSystemResult<&mut [u8]> {
        let offset = self.data_start()?;
        Ok(&mut self.0[offset..])
    }

    /// Cast the [Blob] to a [str]
    pub fn as_str(&self) -> TypeSystemResult<&str> {
        let str = str::from_utf8(self.data()?)?;
        Ok(str)
    }
    /// Cast the [Blob] to an owned [String]
    pub fn to_string_lossy(&self) -> TypeSystemResult<String> {
        Ok(String::from_utf8_lossy(self.data()?).into_owned())
    }

    /// Cast the [Blob] to a [str] (unchecked)
    pub fn as_str_unchecked(&self) -> &str {
        str::from_utf8(self.data().unwrap()).unwrap()
    }
    /// Cast the [Blob] to an owned [String] (unchecked)
    pub fn to_string_lossy_unchecked(&self) -> String {
        String::from_utf8_lossy(self.data().unwrap()).into_owned()
    }

    pub fn as_blob_ref(&self) -> BlobRef<'_> {
        BlobRef::from(self.as_ref())
    }
}

impl Display for Blob {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "Blob ({})", self.to_string_lossy_unchecked())
    }
}

/// [AsRef] returns the full Blob data, including the [VarInt] prefix
impl AsRef<[u8]> for Blob {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

/// [AsMut] returns the full Blob data, including the [VarInt] prefix
impl AsMut<[u8]> for Blob {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }
}

/// [Deref] and [DerefMut] implementations have the same behaviour as [AsRef] and [AsMut]
impl Deref for Blob {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl DerefMut for Blob {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

/// Equality and Ordering are implementing considering the full [Blob], not the data content.
///
/// If Blobs are always well formed there should be no differences.
impl PartialEq for Blob {
    fn eq(&self, other: &Self) -> bool {
        matches!(self.partial_cmp(other), Some(Ordering::Equal))
    }
}

impl Eq for Blob {}

impl PartialOrd for Blob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let comparator = BlobComparator;
        comparator.partial_cmp_blobs(&self.as_blob_ref(), &other.as_blob_ref())
    }
}

impl Ord for Blob {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Non mutable reference to variable length data.
#[derive(Debug, Clone, Copy)]
pub struct BlobRef<'a>(&'a [u8]);

impl<'a> TypeClass for BlobRef<'a> {}
impl<'a> VarlenType for BlobRef<'a> {}

impl<'a> BlobRef<'a> {
    /// Get the length of the full byte slice.
    pub fn total_length(&self) -> usize {
        self.0.len()
    }

    /// Returns 0 if the blob holds no data at all.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the offset where the data starts in the [BlobRef]
    fn data_start(&self) -> TypeSystemResult<usize> {
        let (_, offset) = VarInt::from_encoded_bytes(self.0.as_ref())?;
        Ok(offset)
    }

    /// Gets the length of the data part
    pub fn data_length(&self) -> TypeSystemResult<usize> {
        let (length, _) = VarInt::from_encoded_bytes(self.0.as_ref())?;
        Ok(length.into())
    }

    /// Get a reference to the data content in the [BlobRef]
    pub fn data(&self) -> TypeSystemResult<&[u8]> {
        let offset = self.data_start()?;
        Ok(&self.0[offset..])
    }

    /// Cast the [BlobRef] to a [str]
    pub fn as_str(&self) -> TypeSystemResult<&str> {
        let str = str::from_utf8(self.data()?)?;
        Ok(str)
    }
    /// Cast the [BlobRef] to an owned [String]
    pub fn to_string_lossy(&self) -> TypeSystemResult<String> {
        Ok(String::from_utf8_lossy(self.data()?).into_owned())
    }

    /// Cast the [BlobRef] to a [str] (unchecked)
    pub fn as_str_unchecked(&self) -> &str {
        str::from_utf8(self.data().unwrap()).unwrap()
    }
    /// Cast the [BlobRef] to an owned [String] (unchecked)
    pub fn to_string_lossy_unchecked(&self) -> String {
        String::from_utf8_lossy(self.data().unwrap()).into_owned()
    }

    /// Cast to an owned [Blob]
    pub fn to_blob(&self) -> Blob {
        Blob(self.0.to_vec().into_boxed_slice())
    }
}

impl<'a> Display for BlobRef<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "BlobRef ({})", self.to_string_lossy_unchecked())
    }
}

impl<'a> AsRef<[u8]> for BlobRef<'a> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

impl<'a> Deref for BlobRef<'a> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a> PartialEq for BlobRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        matches!(self.partial_cmp(other), Some(Ordering::Equal))
    }
}

impl<'a> Eq for BlobRef<'a> {}

/// See [VarlenComparator] for details
impl<'a> PartialOrd for BlobRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let comparator = BlobComparator;
        comparator.partial_cmp_blobs(self, other)
    }
}

impl<'a> Ord for BlobRef<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl<'a> PartialEq<BlobRef<'a>> for Blob {
    fn eq(&self, other: &BlobRef<'a>) -> bool {
        self.0.as_ref() == other.0
    }
}

/// Conversion from a raw byte slice to a [BlobRef<'a>]
///
/// To be consistent with [AsRef] implementations, the encoding is handled separately.
impl<'a> From<&'a [u8]> for BlobRef<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self(value)
    }
}

impl<'a> TypeRef<'a> for BlobRef<'a> {
    type Owned = Blob;

    fn to_owned(&self) -> Self::Owned {
        self.to_blob()
    }

    fn as_slice(&self) -> &[u8] {
        self.as_ref()
    }
}

impl RefTrait for Blob {
    type Ref<'a> = BlobRef<'a>;
}

impl DeserializableType for Blob {
    fn reinterpret_cast(buffer: &[u8]) -> SerializationResult<(Self::Ref<'_>, usize)> {
        let (len_varint, offset) = VarInt::from_encoded_bytes(buffer)?;
        let len_usize: usize = len_varint.into();
        let total_size = offset + len_usize;

        if buffer.len() < total_size {
            return Err(SerializationError::UnexpectedEof);
        }

        Ok((BlobRef::from(&buffer[..total_size]), total_size))
    }

    fn deserialize(buffer: &[u8], cursor: usize) -> SerializationResult<(Self::Ref<'_>, usize)> {
        let (data_ref, bytes_read) = Self::reinterpret_cast(&buffer[cursor..])?;
        Ok((data_ref, cursor + bytes_read))
    }
}

/// [Blob] size is only known at runtime.
impl RuntimeSized for Blob {
    fn runtime_size(&self) -> usize {
        self.total_length()
    }
}

/// [str] won't hold a length prefix by itself so we have to append it here.
impl From<&str> for Blob {
    fn from(value: &str) -> Self {
        Self::from_unencoded_slice(value.as_bytes())
    }
}

/// [String] won't hold a length prefix by itself so we have to append it here.
impl From<String> for Blob {
    fn from(value: String) -> Self {
        Self::from_unencoded_slice(value.as_bytes())
    }
}

/// For consistency with [AsRef] implementations, this conversion assumes that the slice is encoded before creating the [Blob]. Same for From<Vec<u8>> and From<Box<[u8]>> impls.
impl From<&[u8]> for Blob {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec().into_boxed_slice())
    }
}

impl From<Vec<u8>> for Blob {
    fn from(value: Vec<u8>) -> Self {
        Self(value.into_boxed_slice())
    }
}

impl From<Box<[u8]>> for Blob {
    fn from(value: Box<[u8]>) -> Self {
        Self(value)
    }
}

impl<T> TypeCast<Blob> for T
where
    T: NumericType + AsRef<[u8]>,
{
    /// Automatic casting from any [NumericType] to a [Blob]
    fn try_cast(&self) -> Option<Blob> {
        Some(Blob::from_unencoded_slice(self.as_ref()))
    }
}

impl SerializableType for Blob {
    fn write_to(&self, writer: &mut [u8], cursor: usize) -> SerializationResult<usize> {
        let data_bytes = self.as_ref();
        writer[cursor..cursor + self.runtime_size()].copy_from_slice(data_bytes);
        Ok(cursor + self.runtime_size())
    }
}

/// Comparator for variable-length data with VarInt length prefix.
///
/// Format: `[VarInt length][data bytes]`
/// Comparison is lexicographic on the data portion.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BlobComparator;

impl BlobComparator {
    fn partial_cmp_blobs(&self, lhs: &BlobRef<'_>, rhs: &BlobRef<'_>) -> Option<Ordering> {
        if lhs.as_ref().as_ptr() == rhs.as_ref().as_ptr() && lhs.len() == rhs.len() {
            return Some(Ordering::Equal);
        }

        let lhs_len: usize = lhs.data_length().ok()?;
        let left_data = lhs.data().ok()?;

        let rhs_len: usize = rhs.data_length().ok()?;
        let right_data = rhs.data().ok()?;

        let min_len = lhs_len.min(rhs_len);

        if min_len > 8 {
            // Compare in 8-byte chunks for efficiency
            let chunk_count = min_len / 8;
            for i in 0..chunk_count {
                let lhs_chunk = &left_data[i * 8..(i + 1) * 8];
                let rhs_chunk = &right_data[i * 8..(i + 1) * 8];

                // Use big-endian for lexicographic ordering
                let lhs_u64 = u64::from_be_bytes(lhs_chunk.try_into().unwrap());
                let rhs_u64 = u64::from_be_bytes(rhs_chunk.try_into().unwrap());

                match lhs_u64.cmp(&rhs_u64) {
                    Ordering::Equal => continue,
                    other => return Some(other),
                }
            }

            // Compare remaining bytes
            for i in (chunk_count * 8)..min_len {
                match left_data[i].cmp(&right_data[i]) {
                    Ordering::Equal => continue,
                    other => return Some(other),
                }
            }
        } else {
            // Compare small payloads byte by byte
            for i in 0..min_len {
                match left_data[i].cmp(&right_data[i]) {
                    Ordering::Equal => continue,
                    other => return Some(other),
                }
            }
        }

        // If all compared bytes are equal, decide by length
        Some(lhs_len.cmp(&rhs_len))
    }
}
