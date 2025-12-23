#[macro_export]
macro_rules! bytemuck_slice {
    ($t:ty) => {
        // Safety: requires $t to implement Pod
        unsafe impl bytemuck::Zeroable for $t {}
        unsafe impl bytemuck::Pod for $t {}

        impl From<&[u8]> for $t {
            fn from(value: &[u8]) -> Self {
                bytemuck::pod_read_unaligned::<$t>(&value[..std::mem::size_of::<$t>()])
            }
        }

        impl AsRef<[u8]> for $t {
            fn as_ref(&self) -> &[u8] {
                bytemuck::bytes_of(self)
            }
        }

        impl AsMut<[u8]> for $t {
            fn as_mut(&mut self) -> &mut [u8] {
                bytemuck::bytes_of_mut(self)
            }
        }
    };
}

#[macro_export]
macro_rules! bytemuck_struct {
    ($t:ty) => {
        impl $t {
            pub const SIZE: usize = std::mem::size_of::<Self>();
            pub const ALIGN: usize = std::mem::align_of::<Self>();

            #[inline]
            pub fn aligned_offset(offset: usize) -> usize {
                (offset + Self::ALIGN - 1) & !(Self::ALIGN - 1)
            }

            /// Writes the header to a buffer, starting at the next valid offset.
            ///
            /// Returns the offset where the header ends.
            #[inline]
            pub fn write_to(self, buffer: &mut [u8], offset: usize) -> usize {
                let aligned = Self::aligned_offset(offset);

                // Write the byte representation directly to the aligned buffer.
                let data_bytes = self.as_ref();

                buffer[aligned..aligned + Self::SIZE].copy_from_slice(data_bytes);

                aligned + Self::SIZE
            }

            /// Read the header from an slice.
            ///
            /// Bytemuck copies the data internally if the offset is not aligned to the alignment requirements in memory.
            ///
            /// It is not ideal but as we have a very specific layout for the tuples we can deal with it for now.
            #[inline]
            pub fn read_from(buffer: &[u8], offset: usize) -> (Self, usize) {
                let aligned = Self::aligned_offset(offset);
                let data = Self::from(&buffer[aligned..]);
                (data, aligned + Self::SIZE)
            }
        }

        $crate::bytemuck_slice!($t);
    };
}
