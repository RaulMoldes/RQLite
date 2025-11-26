#[macro_export]
macro_rules! as_slice {
    ($t:ty) => {
        impl AsRef<[u8]> for $t {
            fn as_ref(&self) -> &[u8] {
                unsafe {
                    std::slice::from_raw_parts(
                        self as *const $t as *const u8,
                        std::mem::size_of::<$t>(),
                    )
                }
            }
        }

        impl AsMut<[u8]> for $t {
            fn as_mut(&mut self) -> &mut [u8] {
                unsafe {
                    std::slice::from_raw_parts_mut(
                        self as *mut $t as *mut u8,
                        std::mem::size_of::<$t>(),
                    )
                }
            }
        }
    };
}
