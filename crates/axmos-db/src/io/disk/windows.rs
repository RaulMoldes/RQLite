use std::{
    fs::File,
    io,
    os::windows::{ffi::OsStrExt, fs::OpenOptionsExt},
    path::Path,
};

use windows::{
    Win32::{Foundation::MAX_PATH, Storage::FileSystem},
    core::PCWSTR,
};

use super::{FileSystem as Fs, FileSystemBlockSize, Open, OpenOptions};

impl FileSystemBlockSize for Fs {
    fn block_size(path: impl AsRef<Path>) -> io::Result<usize> {
        unsafe {
            let mut volume = [0u16; MAX_PATH as usize];
            let mut windows_file_path = path
                .as_ref()
                .as_os_str()
                .encode_wide()
                .collect::<Vec<u16>>();
            windows_file_path.push(0);

            FileSystem::GetVolumePathNameW(
                PCWSTR::from_raw(windows_file_path.as_ptr()),
                &mut volume,
            )?;

            let mut bytes_per_sector: u32 = 0;
            let mut sectors_per_cluster: u32 = 0;

            FileSystem::GetDiskFreeSpaceW(
                PCWSTR::from_raw(volume.as_ptr()),
                Some(&mut sectors_per_cluster),
                Some(&mut bytes_per_sector),
                None,
                None,
            )?;

            Ok((bytes_per_sector * sectors_per_cluster) as usize)
        }
    }
}

impl Open for OpenOptions {
    fn open(mut self, path: impl AsRef<Path>) -> io::Result<File> {
        let mut flags = FileSystem::FILE_FLAGS_AND_ATTRIBUTES(0);

        if self.bypass_cache {
            flags |= FileSystem::FILE_FLAG_NO_BUFFERING;
        }

        if self.sync_on_write {
            flags |= FileSystem::FILE_FLAG_WRITE_THROUGH;
        }

        if flags.0 != 0 {
            self.inner.custom_flags(flags.0);
        }

        if self.lock {
            self.inner.share_mode(0);
        }

        self.inner.open(path)
    }
}
