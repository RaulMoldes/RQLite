use std::{
    fs::File,
    io,
    os::{
        fd::AsRawFd,
        unix::{fs::OpenOptionsExt, prelude::MetadataExt},
    },
    path::Path,
};

use super::{FileSystem, FileSystemBlockSize, Open, OpenOptions};

impl FileSystemBlockSize for FileSystem {
    fn block_size(path: impl AsRef<Path>) -> io::Result<usize> {
        Ok(File::open(&path)?.metadata()?.blksize() as usize)
    }
}

impl Open for OpenOptions {
    fn open(mut self, path: impl AsRef<Path>) -> io::Result<File> {
        let mut flags = 0;

        if self.bypass_cache {
            flags |= libc::O_DIRECT;
        }

        if self.sync_on_write {
            flags |= libc::O_DSYNC;
        }

        if flags != 0 {
            self.inner.custom_flags(flags);
        }

        let file = self.inner.open(&path)?;

        if self.lock {
            let lock = unsafe {
                libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB)
            };
            if lock != 0 {
                return Err(io::Error::other(
                    format!("could not lock file {}", path.as_ref().display()),
                ));
            }
        }

        Ok(file)
    }
}
