use std::ffi::CString;
use std::os::raw::{c_char, c_int};

#[link(name = "fiu")]
unsafe extern "C" {
    unsafe fn fiu_init(flags: c_int) -> c_int;
    unsafe fn fiu_enable(
        name: *const c_char,
        failnum: c_int,
        failinfo: *const c_char,
        flags: c_int,
    ) -> c_int;
    unsafe fn fiu_disable(name: *const c_char) -> c_int;
}
pub(crate) fn init() -> bool {
    unsafe { fiu_init(0) == 0 }
}

pub(crate) fn enable_fsync_failure() -> bool {
    let name = CString::new("posix/io/oc/fsync").unwrap();
    unsafe { fiu_enable(name.as_ptr(), 1, std::ptr::null(), 0) == 0 }
}

pub(crate) fn disable_fsync_failure() -> bool {
    let name = CString::new("posix/io/oc/fsync").unwrap();
    unsafe { fiu_disable(name.as_ptr()) == 0 }
}
