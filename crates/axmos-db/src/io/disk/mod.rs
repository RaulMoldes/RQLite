#[cfg(unix)]
pub mod unix;
#[cfg(windows)]
pub mod windows;

use std::{
    fs::{self, File, Metadata},
    io::{self, Read, Seek, Write},
    os::{fd::AsRawFd, unix::prelude::RawFd},
    path::{Path, PathBuf},
};

/// Trait to obtain the file system block size.
/// It is the job of the target platform to implement it.
pub(crate) trait FileSystemBlockSize {
    fn block_size(path: impl AsRef<Path>) -> io::Result<usize>;
}

/// Trait for opening a file depending on the OS.
pub(crate) trait Open {
    fn open(self, path: impl AsRef<Path>) -> io::Result<File>;
}

pub(crate) struct FileSystem;

pub(crate) struct OpenOptions {
    /// Inner [`std::fs::OpenOptions`] instance.
    inner: fs::OpenOptions,
    /// Bypasses OS cache.
    bypass_cache: bool,
    /// When calling write() makes sure that data reaches disk before returning.
    sync_on_write: bool,
    /// Locks the file exclusively for the calling process.
    lock: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            inner: std::fs::OpenOptions::new(),
            bypass_cache: false,
            sync_on_write: false,
            lock: false,
        }
    }
}

impl FileSystem {
    pub fn options() -> OpenOptions {
        OpenOptions::default()
    }
}

impl OpenOptions {
    /// Disables the OS cache for reads and writes.
    pub fn bypass_cache(mut self, bypass_cache: bool) -> Self {
        self.bypass_cache = bypass_cache;
        self
    }

    /// Every call to write() will make sure that the data reaches the disk.
    pub fn sync_on_write(mut self, sync_on_write: bool) -> Self {
        self.sync_on_write = sync_on_write;
        self
    }

    /// Locks the file with exclusive access for the calling process.
    pub fn lock(mut self, lock: bool) -> Self {
        self.lock = lock;
        self
    }

    /// Create if doesn't exist.
    pub fn create(mut self, create: bool) -> Self {
        self.inner.create(create);
        self
    }

    /// Open for reading.
    pub fn read(mut self, read: bool) -> Self {
        self.inner.read(read);
        self
    }

    /// Open for writing.
    pub fn write(mut self, write: bool) -> Self {
        self.inner.write(write);
        self
    }

    /// Set the length of the file to 0 bytes if it exists.
    pub fn truncate(mut self, truncate: bool) -> Self {
        self.inner.truncate(truncate);
        self
    }
}

/// We need additional operations over the File than what Rust [fs::File] provides us.
///
/// This trait includes operations for opening, creating, truncating and syncing files to disk.
pub(crate) trait FileOperations: Seek + Read + Write {
    /// Create a new empty file
    fn create(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized;

    /// Open an existing file
    fn open(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized;

    /// Remove the file at the target path
    fn remove(path: impl AsRef<Path>) -> io::Result<()>;

    /// Truncate the file to 0 bytes
    fn truncate(&mut self) -> io::Result<()>;

    /// Sync the file content to disk.
    fn sync_all(&self) -> io::Result<()>;
}

/// Wrapper over file that contains also path information
#[derive(Debug)]
pub struct DBFile {
    f: File,
    p: PathBuf,
}

impl DBFile {
    pub fn path(&self) -> &Path {
        self.p.as_path()
    }

    pub fn metadata(&self) -> io::Result<Metadata> {
        self.f.metadata()
    }
}

impl Seek for DBFile {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.f.seek(pos)
    }
}

impl Read for DBFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.f.read(buf)
    }
}

impl Write for DBFile {
    fn flush(&mut self) -> io::Result<()> {
        self.f.flush()
    }

    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.f.write(buf)
    }
}

impl AsRawFd for DBFile {
    fn as_raw_fd(&self) -> RawFd {
        self.f.as_raw_fd()
    }
}

impl FileOperations for DBFile {
    /// Create the DBFile
    fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent)?
        }

        // We create the file with the following flags:
        let f = FileSystem::options()
            .create(true)
            .truncate(true)
            .read(true) // O_READ in Linux
            .write(true) // O_WRITE
            .bypass_cache(true) // This is basically O_DIRECT. Forces the writes directly to SSD instead of being buffered by the OS cache
            .sync_on_write(false) // This is O_DSYNC (not used for now)
            .open(&path)?;

        Ok(Self {
            f,
            p: path.as_ref().to_path_buf(),
        })
    }

    fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        // Same flags as for create except here we are reading an existing file
        let f = FileSystem::options()
            .read(true)
            .write(true)
            .bypass_cache(true)
            .sync_on_write(false)
            .open(&path)?;
        Ok(Self {
            f,
            p: path.as_ref().to_path_buf(),
        })
    }

    // Forcefully remove he file
    fn remove(path: impl AsRef<Path>) -> io::Result<()> {
        fs::remove_file(path)
    }

    // truncate the file to 0 len
    fn truncate(&mut self) -> io::Result<()> {
        self.f.set_len(0)
    }

    // sync the file to disk
    fn sync_all(&self) -> io::Result<()> {
        File::sync_all(&self.f)
    }
}
