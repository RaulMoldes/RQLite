use std::{
    alloc::{AllocError, Allocator, Global, Layout},
    any,
    fmt::{self, Debug},
    mem::{self, ManuallyDrop},
    ptr::NonNull,
    slice,
};

use crate::{
    PAGE_ALIGNMENT,
    storage::core::traits::{
        Buffer, BufferOps, ComposedBuffer, ComposedMetadata, Identifiable, Writable,
    },
};

pub struct MemBlock<M> {
    /// Total size of the buffer (size of header + size of data).
    size: u32, // We need four bytes to store the max size of a page (2^32) which would not fit in 2 bytes.
    /// Pointer to the header located at the beginning of the buffer.
    pub(crate) metadata: NonNull<M>,
    /// Pointer to the data located right after the header.
    pub(crate) data: NonNull<[u8]>,
}

impl<M> MemBlock<M> {
    // This function must be used to allocate 'owned' Buffers.
    fn allocate_zeroed(size: usize) -> Result<NonNull<[u8]>, AllocError> {
        assert!(
            PAGE_ALIGNMENT as usize >= mem::align_of::<M>(),
            "Alignment {PAGE_ALIGNMENT} is too small for type {} which requires {} bytes",
            any::type_name::<M>(),
            mem::align_of::<M>(),
        );
        assert!(
            size >= mem::size_of::<M>(),
            "Attempted to allocate {} of insufficient size: size of {} is {} while allocation size is {}",
            any::type_name::<Self>(),
            any::type_name::<M>(),
            mem::size_of::<M>(),
            size,
        );

        Global.allocate_zeroed(Layout::from_size_align(size, PAGE_ALIGNMENT as usize).unwrap())
    }

    /// Allocate a zeroed buffer.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(capacity)
    }

    /// Returns how many bytes can still be written without reallocating.
    pub fn capacity(&self) -> usize {
        Self::usable_space(self.size as usize) as usize
    }

    pub fn metadata_size(&self) -> usize {
        mem::size_of::<M>()
    }

    /// Returns the actual size of the buffer
    pub fn size(&self) -> usize {
        self.size as usize
    }

    /// Returns a new buffer of the given size where all the bytes are 0.
    ///
    /// If the fields of the header need values other than 0 for initialization
    /// then they should be written manually after this function call.
    pub fn new(size: usize) -> Self {
        let ptr = Self::allocate_zeroed(size).expect("Allocation error. Unable to allocate buffer");
        unsafe { Self::from_non_null(ptr) }
    }

    /// Constructs a new buffer from the given [`NonNull`] pointer.
    ///
    /// SAFETY: This functions checks internally that the pointer is of enough len() and is aligned to the required alignment by the header struct, so as long as this checks are passed, it should be safe to use.
    pub unsafe fn from_non_null(pointer: NonNull<[u8]>) -> Self {
        assert!(
            pointer.len() >= mem::size_of::<M>(),
            "attempt to construct {} from invalid pointer of size {} when size of {} is {}",
            any::type_name::<Self>(),
            pointer.len(),
            any::type_name::<M>(),
            mem::size_of::<M>(),
        );

        assert!(
            pointer.is_aligned_to(PAGE_ALIGNMENT as usize),
            "attempt to create {} from unaligned pointer {:?}",
            any::type_name::<Self>(),
            pointer
        );

        let data = NonNull::slice_from_raw_parts(
            unsafe { pointer.byte_add(mem::size_of::<M>()).cast::<u8>() },
            Self::usable_space(pointer.len()) as usize,
        );

        Self {
            metadata: pointer.cast(),
            data,
            size: pointer.len() as u32,
        }
    }

    /// Transforms this BUFFER into another page with a different METADATA type.
    pub fn cast<T>(self) -> MemBlock<T> {
        let Self {
            metadata,
            data,
            size,
        } = self;

        assert!(
            size as usize > mem::size_of::<T>(),
            "cannot cast {} of total size {size} to {} where the size of {} is {}",
            any::type_name::<Self>(),
            any::type_name::<MemBlock<T>>(),
            any::type_name::<T>(),
            mem::size_of::<T>(),
        );

        // std::mem::forget does not actually run the destructor of [`BufferWIthMetadata`].
        // Instead, it will create (intentionally) a memory leak that will allow us to create a page of a new type.
        mem::forget(self);

        let metadata = metadata.cast();

        let data = unsafe {
            NonNull::slice_from_raw_parts(
                metadata.byte_add(mem::size_of::<T>()).cast::<u8>(),
                MemBlock::<T>::usable_space(size as usize) as usize,
            )
        };

        MemBlock {
            metadata,
            data,
            size,
        }
    }

    /// Number of bytes that can be used to store data.
    pub fn usable_space(size: usize) -> usize {
        size - mem::size_of::<M>()
    }

    /// Returns a read-only reference to the header.
    pub fn metadata(&self) -> &M {
        unsafe { self.metadata.as_ref() }
    }

    /// Returns a mutable reference to the header.
    pub fn metadata_mut(&mut self) -> &mut M {
        unsafe { self.metadata.as_mut() }
    }

    /// Returns a read-only reference to the data part of the page.
    pub fn data(&self) -> &[u8] {
        unsafe { self.data.as_ref() }
    }

    /// Returns a mutable reference to the data of the page.
    pub fn data_mut(&mut self) -> &mut [u8] {
        unsafe { self.data.as_mut() }
    }

    /// Returns a [`NonNull`] pointer to the entire in-memory buffer.
    fn as_non_null(&self) -> NonNull<[u8]> {
        NonNull::slice_from_raw_parts(self.metadata.cast::<u8>(), self.size as usize)
    }

    /// Returns a byte slice of the entire buffer including its header.
    fn as_slice(&self) -> &[u8] {
        unsafe { self.as_non_null().as_ref() }
    }

    /// Returns a mutable byte slice of the entire buffer including its header.
    fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { self.as_non_null().as_mut() }
    }

    /// Consumes [`self`] and returns a pointer to the underlying memory buffer.
    pub fn into_non_null(self) -> NonNull<[u8]> {
        ManuallyDrop::new(self).as_non_null()
    }
}

impl<M> AsRef<[u8]> for MemBlock<M> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<M> AsMut<[u8]> for MemBlock<M> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_slice_mut()
    }
}

impl<M> PartialEq for MemBlock<M> {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

/// Clone the block by allocating a new one with the same alignment and size and copying all the content into it.
impl<M> Clone for MemBlock<M> {
    fn clone(&self) -> Self {
        let mut cloned = Self::new(self.size as usize);
        cloned.size = self.size;

        cloned.as_slice_mut().copy_from_slice(self.as_slice());

        assert_eq!(cloned.data(), self.data());
        cloned
    }
}

impl<M: Debug> Debug for MemBlock<M> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Buffer")
            .field("size", &self.size)
            .field("metadata", self.metadata())
            .field("data content", &self.data())
            .finish()
    }
}

/// Destructor.
/// As noted above, this destructor is only intended to be run for fully owned buffers.
/// Buffers intended to be a portion of a bigger one should be deallocated using [std::mem::forget].
/// Otherwise, dropping the buffer portion would invalidate the owned pointer.
impl<M> Drop for MemBlock<M> {
    fn drop(&mut self) {
        unsafe {
            Global.deallocate(
                self.metadata.cast(),
                Layout::from_size_align(self.size as usize, PAGE_ALIGNMENT as usize).unwrap(),
            )
        }
    }
}

/// Copies the content of the given slice into a new memory buffer
impl<M> TryFrom<&[u8]> for MemBlock<M> {
    type Error = AllocError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let header_size = mem::size_of::<M>();

        if value.len() < header_size {
            return Err(AllocError);
        }

        let mut ptr = Self::allocate_zeroed(value.len())?;

        unsafe {
            ptr.as_mut().copy_from_slice(value);
            Ok(Self::from_non_null(ptr))
        }
    }
}

impl<M> ComposedBuffer for MemBlock<M>
where
    M: ComposedMetadata,
    Self: BufferOps,
{
    type DataItemHeader = M::ItemHeader;

    // Writes a new item to a composed buffer.
    // SAFETY: `last_used_offset` keeps track of where the last cell was
    // written. By substracting the total size of the new cell to
    // `last_used_offset` we get a valid pointer within the page where we
    // write the new cell.
    unsafe fn write_item_to_offset(
        &self,
        offset: u64,
        item: impl Writable<Header = Self::DataItemHeader>,
    ) {
        unsafe {
            let dest = self.data.byte_add(offset as usize).cast::<u8>().as_ptr();
            let dest_slice = slice::from_raw_parts_mut(dest, item.total_size());

            item.write_to(dest_slice);
        }
    }

    unsafe fn item_at_offset(&self, offset: u64) -> NonNull<Self::DataItemHeader> {
        unsafe { self.data.byte_add(offset as usize).cast() }
    }
}

impl<M> Buffer for MemBlock<M>
where
    M: Identifiable + Copy + AsRef<[u8]>,
{
    type Header = M;

    fn header(&self) -> &Self::Header {
        self.metadata()
    }

    fn header_mut(&mut self) -> &mut Self::Header {
        self.metadata_mut()
    }

    fn data_non_null(&self) -> NonNull<[u8]> {
        self.data
    }

    fn data(&self) -> &[u8] {
        self.data()
    }

    fn data_mut(&mut self) -> &mut [u8] {
        self.data_mut()
    }

    fn capacity(&self) -> usize {
        Self::usable_space(self.size as usize) as usize
    }

    fn usable_space(&self, size: usize) -> usize {
        Self::usable_space(size)
    }
}

// Macro to create aligned buffers inline.
#[macro_export]
macro_rules! aligned_buf {
    ($size:expr) => {{
        use $crate::storage::core::buffer::MemBlock;

        let buffer: MemBlock<()> = MemBlock::new($size);
        buffer
    }};

    ($size:expr, $val: expr) => {{
        use $crate::storage::core::buffer::MemBlock;

        let mut buffer: MemBlock<()> = MemBlock::new($size);

        buffer.data_mut().fill($val);

        buffer
    }};
}
