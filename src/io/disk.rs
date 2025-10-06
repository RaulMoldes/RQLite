//! Module for file management operations.
use std::{
    fs::{self, File},
    io::{self, Cursor, Read, Seek, SeekFrom, Write},
    path::Path,
};

pub(crate) trait FileOps: Seek + Read + Write {
    /// Creates a file on the filesystem at the given `path`.
    ///
    /// If the file already exists it should be truncated and if the parent
    /// directories are not present they will be creates as well.
    fn create(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized;

    /// Opens the file "as is", no trunc.
    fn open(path: impl AsRef<Path>) -> io::Result<Self>
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

impl FileOps for File {
    /// Create a new file.
    fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent)?
        }

        File::options()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)
    }

    /// Open the file
    fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        File::options().read(true).write(false).open(path)
    }

    /// Remove the file
    fn remove(path: impl AsRef<Path>) -> io::Result<()> {
        fs::remove_file(path)
    }

    /// Reset the length of the file
    fn truncate(&mut self) -> io::Result<()> {
        self.set_len(0)
    }

    /// Sync makes the calling thread wait until the content has been written to the disk.
    /// https://stackoverflow.com/questions/69819990/whats-the-difference-between-flush-and-sync-all
    fn sync_all(&self) -> io::Result<()> {
        self.sync_all()
    }
}

/// In-memory buffer with the same trait implementations as a normal disk file.
/// Idea from: https://github.com/antoniosarosi/mkdb/blob/master/src/paging/io.rs
#[derive(Clone)]
pub(crate) struct Buffer(io::Cursor<Vec<u8>>);

impl Buffer {
    pub fn with_capacity(capacity: usize) -> Self {
        let inner = Vec::with_capacity(capacity);
        Self(Cursor::new(inner))
    }

    pub fn reserve_capacity(&mut self, additional: usize) {
        self.0.get_mut().reserve(additional);
    }

    pub fn reserve_capacity_exact(&mut self, additional: usize) {
        self.0.get_mut().reserve_exact(additional);
    }

    pub fn shrink_to_fit(&mut self) {
        self.0.get_mut().shrink_to_fit();
    }

    pub fn capacity(&self) -> usize {
        self.0.get_ref().capacity()
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.get_ref()
    }
}

impl Read for Buffer {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl Seek for Buffer {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.0.seek(pos)
    }
}
impl FileOps for Buffer {
    /// For in-memory buffers, create and open have the same behaviour.
    fn create(_path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Buffer(io::Cursor::new(Vec::new())))
    }

    fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut temp_file = File::options().read(true).write(false).open(path)?;
        let mut contents = Vec::new();
        temp_file.read_to_end(&mut contents)?;

        Ok(Buffer(io::Cursor::new(contents)))
    }

    // Sets the position of the buffer at 0 and clears the content
    fn truncate(&mut self) -> io::Result<()> {
        self.0.set_position(0);
        self.0.get_mut().clear();

        Ok(())
    }

    fn remove(_path: impl AsRef<Path>) -> io::Result<()> {
        Ok(())
    }

    /// You cannot actually sync an in-memory buffer
    fn sync_all(&self) -> io::Result<()> {
        Ok(())
    }
}
