#[cfg(target_os = "linux")]
#[cfg(not(miri))]
pub mod linux;
#[cfg(target_os = "linux")]
#[cfg(not(miri))]
pub use linux::DirectIO;

use std::{
    io::{self, Read, Seek, Write},
    path::Path,
};

pub(crate) enum FOpenMode {
    Read = 0,
    ReadWrite = 1,
    Write = 2,
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

#[cfg(miri)]
mod miri_stub {
    use std::fs::File;
    use std::io::{self, Read, Seek, SeekFrom, Write};
    use std::path::Path;

    use super::*;

    #[derive(Debug)]
    pub struct FileStub(pub File);

    impl FileStub {
        pub const BLOCK_SIZE: usize = 4096;

        pub fn validate_alignment(size: usize) -> bool {
            size.is_multiple_of(Self::BLOCK_SIZE)
        }

        pub fn as_inner(&self) -> &File {
            &self.0
        }

        pub fn as_inner_mut(&mut self) -> &mut File {
            &mut self.0
        }
    }

    impl FileOperations for FileStub {
        fn create(path: impl AsRef<Path>, _mode: FOpenMode) -> io::Result<Self> {
            if let Some(parent) = path.as_ref().parent() {
                std::fs::create_dir_all(parent)?;
            }

            let file = File::options()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(path)?;
            Ok(Self(file))
        }

        fn open(path: impl AsRef<Path>, _mode: FOpenMode) -> io::Result<Self> {
            let file = File::options().read(true).write(true).open(path)?;
            Ok(Self(file))
        }

        fn remove(path: impl AsRef<Path>) -> io::Result<()> {
            std::fs::remove_file(path)
        }

        fn truncate(&mut self) -> io::Result<()> {
            self.0.set_len(0)
        }

        fn sync_all(&self) -> io::Result<()> {
            self.0.sync_all()
        }
    }

    impl Read for FileStub {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.0.read(buf)
        }
    }

    impl Write for FileStub {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.0.flush()
        }
    }

    impl Seek for FileStub {
        fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
            self.0.seek(pos)
        }
    }

    pub type DirectIO = FileStub;
}

#[cfg(miri)]
pub use miri_stub::DirectIO;
