#[macro_export]
macro_rules! make_shared {
    ($shared_ty:ident, $inner_ty:ty) => {
        #[repr(transparent)]
        pub struct $shared_ty(std::sync::Arc<parking_lot::RwLock<$inner_ty>>);

        impl From<std::sync::Arc<parking_lot::RwLock<$inner_ty>>> for $shared_ty {
            fn from(value: std::sync::Arc<parking_lot::RwLock<$inner_ty>>) -> Self {
                Self(std::sync::Arc::clone(&value))
            }
        }

        impl From<$inner_ty> for $shared_ty {
            fn from(value: $inner_ty) -> Self {
                Self(std::sync::Arc::new(parking_lot::RwLock::new(value)))
            }
        }

        impl Clone for $shared_ty {
            fn clone(&self) -> Self {
                Self(std::sync::Arc::clone(&self.0))
            }
        }

        impl $shared_ty {
            pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, $inner_ty> {
                self.0.read()
            }

            pub fn write(&self) -> parking_lot::RwLockWriteGuard<'_, $inner_ty> {
                self.0.write()
            }

            pub fn strong_count(&self) -> usize {
                std::sync::Arc::strong_count(&self.0)
            }
        }

        unsafe impl Send for $inner_ty {}
        unsafe impl Sync for $inner_ty {}
    };
}
