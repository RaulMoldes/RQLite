//! Module for file management operations.
use libc::{
    c_void, free, lseek, open as libc_open, posix_memalign, read as libc_read, write as libc_write,
    O_CREAT, O_DIRECT, O_RDONLY, O_RDWR, O_WRONLY, SEEK_SET,
};
use std::ffi::CString;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;

use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
    os::fd::RawFd,
    path::Path,
};

use super::{FOpenMode, FileOperations};

fn open_direct(path: impl AsRef<Path>, mode: FOpenMode, create: bool) -> io::Result<File> {
    let path_cstr = CString::new(path.as_ref().as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid path"))?;

    let mut flags = O_DIRECT;

    if create {
        flags |= O_CREAT;
    };

    match mode {
        FOpenMode::Read => flags |= O_RDONLY,
        FOpenMode::Write => flags |= O_WRONLY,
        FOpenMode::ReadWrite => flags |= O_RDWR,
    };

    let fd = unsafe { libc_open(path_cstr.as_ptr(), flags, 0o644) };
    if fd >= 0 {
        let file = unsafe { File::from_raw_fd(fd) };
        Ok(file)
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Aligned file wrapper for O_DIRECT
pub struct DirectIO {
    file: File,
}

impl DirectIO {
    /// Minimum block size for O_DIRECT
    pub const BLOCK_SIZE: usize = 4096;

    pub fn validate_alignment(size: usize) -> bool {
        size.is_multiple_of(Self::BLOCK_SIZE)
    }

    /// Allocates a new aligned buffer of at least [`size`] bytes.
    pub fn alloc_aligned(size: usize) -> io::Result<&'static mut [u8]> {
        // Rounds up to the first multiple of BLOCK SIZE
        let target_size = size.next_multiple_of(Self::BLOCK_SIZE);
        unsafe {
            let mut ptr: *mut c_void = std::ptr::null_mut();
            let res = posix_memalign(&mut ptr, Self::BLOCK_SIZE, target_size);
            if res != 0 || ptr.is_null() {
                return Err(io::Error::other("posix_memalign failed"));
            }

            // Zero out the memory
            std::ptr::write_bytes(ptr, 0, target_size);

            Ok(std::slice::from_raw_parts_mut(ptr as *mut u8, target_size))
        }
    }

    /// Free an aligned buffer
    pub unsafe fn free_aligned(ptr: *mut u8) {
        free(ptr as *mut c_void)
    }

    /// Returns the raw file descriptor
    pub fn fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

impl Read for DirectIO {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.as_ptr() as usize % Self::BLOCK_SIZE != 0 || buf.len() % Self::BLOCK_SIZE != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "buffer not aligned for O_DIRECT on Read.",
            ));
        }

        let ret = unsafe {
            libc_read(
                self.file.as_raw_fd(),
                buf.as_mut_ptr() as *mut c_void,
                buf.len(),
            )
        };
        if ret < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(ret as usize)
        }
    }
}

impl Write for DirectIO {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.as_ptr() as usize % Self::BLOCK_SIZE != 0 || buf.len() % Self::BLOCK_SIZE != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "buffer not aligned for O_DIRECT on Write.",
            ));
        }

        let ret = unsafe {
            libc_write(
                self.file.as_raw_fd(),
                buf.as_ptr() as *const c_void,
                buf.len(),
            )
        };
        if ret < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(ret as usize)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        let ret = unsafe { libc::fsync(self.file.as_raw_fd()) };
        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

impl Seek for DirectIO {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let (offset, whence) = match pos {
            SeekFrom::Start(off) => (off as i64, SEEK_SET),
            SeekFrom::End(off) => (off, libc::SEEK_END),
            SeekFrom::Current(off) => (off, libc::SEEK_CUR),
        };

        let new_off = unsafe { lseek(self.file.as_raw_fd(), offset, whence) };
        if new_off < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(new_off as u64)
        }
    }
}

impl FileOperations for DirectIO {
    fn create(path: impl AsRef<Path>, mode: FOpenMode) -> io::Result<Self> {
        let file = open_direct(path, mode, true)?;
        Ok(Self { file })
    }

    fn open(path: impl AsRef<Path>, mode: FOpenMode) -> io::Result<Self> {
        let file = open_direct(path, mode, false)?;
        Ok(Self { file })
    }

    fn remove(path: impl AsRef<Path>) -> io::Result<()> {
        use std::ffi::CString;
        let path_cstr = CString::new(path.as_ref().as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid path"))?;
        let ret = unsafe { libc::unlink(path_cstr.as_ptr()) };
        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    fn truncate(&mut self) -> io::Result<()> {
        let fd = self.file.as_raw_fd();
        let ret = unsafe { libc::ftruncate(fd, 0) };
        if ret == 0 {
            self.seek(SeekFrom::Start(0))?;
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    fn sync_all(&self) -> io::Result<()> {
        let fd = self.file.as_raw_fd();
        let ret = unsafe { libc::fsync(fd) };
        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}
