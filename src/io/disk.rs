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
/// Minimum block size for O_DIRECT
const BLOCK_SIZE: usize = 4096;

pub(crate) enum FOpenMode {
    Read = 0,
    ReadWrite = 1,
    Write = 2,
}

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

pub(crate) trait FileOperations: Seek + Read + Write {
    /// Creates a file on the filesystem at the given `path`.
    ///
    /// If the file already exists it should be truncated and if the parent
    /// directories are not present they will be creates as well.
    fn create(path: impl AsRef<Path>, mode: FOpenMode) -> io::Result<Self>
    where
        Self: Sized;

    /// Opens the file "as is", no truncation.
    fn open(path: impl AsRef<Path>, mode: FOpenMode) -> io::Result<Self>
    where
        Self: Sized;

    /// Removes the file located at `path`.
    fn remove(path: impl AsRef<Path>) -> io::Result<()>;

    /// Truncates the file to 0 length.
    fn truncate(&mut self) -> io::Result<()>;

    /// Attempts to persist the data to its destination.
    ///
    /// For disk filesystems this should use the necessary syscalls to send
    /// everything to the hardware. On Unix systems there are two main ways to
    /// achieve this: [`fflush()`] and [`fsync()`]. FLushing is already implemented in [`Ẁrite`],
    /// but this is not enough to ensure content is fully written.
    ///
    /// Additionally, it might not be enough on some systems to use the provided [`fsync`] call,
    /// as on some UNIX operating systems, this call might silently fail, as was reported in this blogpost: https://wiki.postgresql.org/wiki/Fsync_Errors.
    ///
    /// PostgresQL patch: https://git.postgresql.org/gitweb/?p=postgresql.git;a=commitdiff;h=9ccdd7f66e3324d2b6d3dec282cfa9ff084083f1;hp=1556cb2fc5c774c3f7390dd6fb19190ee0c73f8b
    fn sync_all(&self) -> io::Result<()>;
}

/// Aligned file wrapper for O_DIRECT
pub struct DirectIO {
    file: File,
}

impl DirectIO {
    pub fn ensure_aligned(src: &mut [u8]) -> io::Result<&mut [u8]> {
        // Redondea tamaño al múltiplo de BLOCK_SIZE
        let target_size = src.len().div_ceil(BLOCK_SIZE) * BLOCK_SIZE;

        // Si ya está alineado en memoria y tamaño
        if (src.as_ptr() as usize) % BLOCK_SIZE == 0 && src.len() % BLOCK_SIZE == 0 {
            return Ok(src);
        }

        unsafe {
            let mut ptr: *mut c_void = std::ptr::null_mut();
            let res = posix_memalign(&mut ptr, BLOCK_SIZE, target_size);
            if res != 0 || ptr.is_null() {
                return Err(io::Error::other("posix_memalign failed"));
            }

            // Copia solo los bytes reales
            std::ptr::copy_nonoverlapping(src.as_ptr(), ptr as *mut u8, src.len());

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
        if buf.as_ptr() as usize % BLOCK_SIZE != 0 || buf.len() % BLOCK_SIZE != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "buffer not aligned for O_DIRECT",
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
        if buf.as_ptr() as usize % BLOCK_SIZE != 0 || buf.len() % BLOCK_SIZE != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "buffer not aligned for O_DIRECT",
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
