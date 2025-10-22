use std::{
    alloc::{self, AllocError, Allocator, Layout},
    any,
    fmt::{self, Debug},
    mem::{self, ManuallyDrop},
    ptr::NonNull,
};

pub(crate) struct BufferWithMetadata<M> {
    /// Pointer to the header located at the beginning of the buffer.
    pub(crate) metadata: NonNull<M>,
    /// Pointer to the data located right after the header.
    pub(crate) data: NonNull<[u8]>,
    /// Total size of the buffer (size of header + size of data).
    size: u32, // We need four bytes to store the max size of a page (2^32) which would not fit in 2 bytes.
    /// Alignment of the buffer
    alignment: u16,
    /// Length of the used content in the buffer
    len: u32,
}

impl<M> BufferWithMetadata<M> {
    // This function must be used to allocate 'owned' Buffers.
    fn allocate_owned(size: usize, alignment: usize) -> Result<NonNull<[u8]>, AllocError> {
        assert!(
            size >= mem::size_of::<M>(),
            "Attempted to allocate {} of insufficient size: size of {} is {} while allocation size is {}",
            any::type_name::<Self>(),
            any::type_name::<M>(),
            mem::size_of::<M>(),
            size,
        );

        dbg!(size);

        // Return the error to the client and probably perform rollback in the pager at this level.
        alloc::Global.allocate_zeroed(alloc::Layout::from_size_align(size, alignment).unwrap())
    }

    /// Allocate a zeroed buffer.
    pub fn with_capacity(capacity: usize, alignment: usize) -> Self {
        Self::new_unchecked(capacity, alignment)
    }

    /// Returns how many bytes of the data section are in use.
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Returns how many bytes can still be written without reallocating.
    pub fn capacity(&self) -> usize {
        Self::usable_space(self.size as usize) as usize
    }

    /// Returns a slice to the used portion of the data region.
    pub fn used(&self) -> &[u8] {
        &self.data()[..self.len as usize]
    }

    /// Returns a mutable slice to the used portion of the data region.
    pub fn used_mut(&mut self) -> &mut [u8] {
        let length = self.len;
        &mut self.data_mut()[..length as usize]
    }

    pub fn set_len(&mut self, new_len: usize) {
        self.len = new_len as u32;
    }

    /// Returns the actual size of the buffer
    pub fn size(&self) -> usize {
        self.size as usize
    }

    pub fn alignment(&self) -> usize {
        self.alignment as usize
    }

    /// Appends bytes to the buffer. Panics if there isn’t enough capacity.
    pub fn push_bytes(&mut self, bytes: &[u8]) {
        let len = self.len as usize;

        let new_len = len + bytes.len();
        assert!(
            new_len <= self.capacity(),
            "buffer overflow: attempted to push {} bytes into capacity {}",
            bytes.len(),
            self.capacity()
        );
        self.data_mut()[len..new_len].copy_from_slice(bytes);
        self.len = new_len as u32;
    }

    /// Returns a new buffer of the given size where all the bytes are 0.
    ///
    /// If the fields of the header need values other than 0 for initialization
    /// then they should be written manually after this function call.
    pub fn new_unchecked(size: usize, alignment: usize) -> Self {

        dbg!(size);
        let ptr = Self::allocate_owned(size, alignment)
            .expect("Allocation error. Unable to allocate buffer");
        unsafe { Self::from_non_null(ptr, alignment) }
    }

    /// Constructs a new buffer from the given [`NonNull`] pointer.
    pub unsafe fn from_non_null(pointer: NonNull<[u8]>, alignment: usize) -> Self {
        assert!(
            pointer.len() >= mem::size_of::<M>(),
            "attempt to construct {} from invalid pointer of size {} when size of {} is {}",
            any::type_name::<Self>(),
            pointer.len(),
            any::type_name::<M>(),
            mem::size_of::<M>(),
        );

        assert!(
            pointer.is_aligned_to(alignment),
            "attempt to create {} from unaligned pointer {:?}",
            any::type_name::<Self>(),
            pointer
        );

        let data = NonNull::slice_from_raw_parts(
            pointer.byte_add(mem::size_of::<M>()).cast::<u8>(),
            Self::usable_space(pointer.len()) as usize,
        );

        Self {
            metadata: pointer.cast(),
            data,
            size: pointer.len() as u32,
            alignment: alignment as u16,
            len: 0,
        }
    }

    /// Transforms this BUFFER into another page with a different META type.
    pub fn cast<T>(self) -> BufferWithMetadata<T> {
        let Self {
            metadata,
            data,
            size,
            alignment,
            len,
        } = self;

        assert!(
            size as usize > mem::size_of::<T>(),
            "cannot cast {} of total size {size} to {} where the size of {} is {}",
            any::type_name::<Self>(),
            any::type_name::<BufferWithMetadata<T>>(),
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
                BufferWithMetadata::<T>::usable_space(size as usize) as usize,
            )
        };

        BufferWithMetadata {
            metadata,
            data,
            size,
            alignment,
            len,
        }
    }

    /// Number of bytes that can be used to store data.
    pub fn usable_space(size: usize) -> u32 {
        (size - mem::size_of::<M>()) as u32
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

    /// Grows the buffer to at least `new_capacity` usable bytes.
    /// Preserves existing data and metadata. Panics if `new_capacity` is smaller than current usable space.
    pub fn grow(&mut self, new_capacity: usize) {
        let current_usable = self.capacity();
        assert!(
            new_capacity > current_usable,
            "new capacity ({new_capacity}) must be larger than current used space({current_usable})"
        );

        let total_size = mem::size_of::<M>() + new_capacity;
        let layout = Layout::from_size_align(total_size, self.alignment()).unwrap();

        // Allocate new memory
        let new_ptr = unsafe { alloc::alloc_zeroed(layout) };
        if new_ptr.is_null() {
            panic!("Allocation failed while growing buffer");
        }

        unsafe {
            // Copy metadata
            std::ptr::copy_nonoverlapping(self.metadata.as_ptr(), new_ptr as *mut M, 1);

            // Copy existing data
            std::ptr::copy_nonoverlapping(
                self.data.as_ptr() as *const u8,  // cast to *const u8
                new_ptr.add(mem::size_of::<M>()), // destination after metadata
                self.len(),                       // number of bytes to copy
            );
            // Free old memory
            let old_layout = Layout::from_size_align(self.size as usize, self.alignment()).unwrap();
            alloc::dealloc(self.metadata.as_ptr() as *mut u8, old_layout);

            // Update pointers
            self.metadata = NonNull::new_unchecked(new_ptr as *mut M);
            self.data = NonNull::slice_from_raw_parts(
                NonNull::new_unchecked(new_ptr.add(mem::size_of::<M>()).cast::<u8>()),
                new_capacity,
            );
            self.size = (mem::size_of::<M>() + new_capacity) as u32
        }
    }

    /// Shrinks the buffer to the specified new capacity.
    /// The new capacity must be at least as large as the current length.
    pub fn shrink_to(&mut self, new_capacity: usize) {
        assert!(
            new_capacity >= self.len(),
            "new capacity ({}) must be at least current length ({})",
            new_capacity,
            self.len()
        );

        let total_size = mem::size_of::<M>() + new_capacity;
        let layout = Layout::from_size_align(total_size, self.alignment()).unwrap();

        unsafe {
            // Allocate new smaller memory
            let new_ptr = alloc::alloc_zeroed(layout);
            if new_ptr.is_null() {
                panic!("Allocation failed while shrinking buffer");
            }

            // Copy metadata
            std::ptr::copy_nonoverlapping(self.metadata.as_ptr(), new_ptr as *mut M, 1);

            // Copy existing data (only up to length, not full capacity)
            std::ptr::copy_nonoverlapping(
                self.data.as_ptr() as *const u8,
                new_ptr.add(mem::size_of::<M>()),
                self.len(),
            );

            // Free old memory
            let old_layout = Layout::from_size_align(self.size as usize, self.alignment()).unwrap();
            alloc::dealloc(self.metadata.as_ptr() as *mut u8, old_layout);

            // Update pointers and size
            self.metadata = NonNull::new_unchecked(new_ptr as *mut M);
            self.data = NonNull::slice_from_raw_parts(
                NonNull::new_unchecked(new_ptr.add(mem::size_of::<M>()).cast::<u8>()),
                new_capacity,
            );
            self.size = total_size as u32;
        }
    }

    /// Creates a new buffer from a slice of data with exact sizing
    pub fn from_slice_exact(data: &[u8], metadata: M, alignment: usize) -> Self {
        let capacity = data.len() + mem::size_of::<M>();
        dbg!(capacity);
        let mut buffer = Self::with_capacity(capacity, alignment);
        *buffer.metadata_mut() = metadata;

        assert_eq!(buffer.len(), 0);
        dbg!(capacity);
        dbg!(buffer.capacity());
        dbg!(buffer.len());
        buffer.push_bytes(data);
        buffer
    }
}

impl<M> AsRef<[u8]> for BufferWithMetadata<M> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<M> AsMut<[u8]> for BufferWithMetadata<M> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_slice_mut()
    }
}

impl<M> PartialEq for BufferWithMetadata<M> {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl<M> Clone for BufferWithMetadata<M> {
    fn clone(&self) -> Self {
        let mut cloned = Self::new_unchecked(self.size as usize, self.alignment as usize);
        cloned.len = self.len;
        cloned.size = self.size;
        cloned.alignment = self.alignment;

        cloned.as_slice_mut().copy_from_slice(self.as_slice());
        cloned
    }
}

impl<M: Debug> Debug for BufferWithMetadata<M> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Buffer")
            .field("size", &self.size)
            .field("alignment", &self.alignment)
            .field("metadata", self.metadata())
            .field("data content", &self.data())
            .finish()
    }
}

/// Destructor.
/// As noted above, this destructor is only intended to be run for fully owned buffers.
/// Buffers intended to be a portion of a bigger one should be deallocated using [std::mem::forget].
/// Otherwise, dropping the buffer portion would invalidate the owned pointer.
impl<M> Drop for BufferWithMetadata<M> {
    fn drop(&mut self) {
        unsafe {
            alloc::Global.deallocate(
                self.metadata.cast(),
                alloc::Layout::from_size_align(self.size as usize, self.alignment as usize)
                    .unwrap(),
            )
        }
    }
}
