use crate::io::MemBuffer;
use crate::io::disk::FileSystemBlockSize;
use crate::io::disk::{DBFile, FileOperations, FileSystem};
use std::io::{Read, Write};
use std::io::{Seek, SeekFrom};
const TEST_PAGE_SIZE: u32 = 4096;

#[test]
#[cfg(target_os = "linux")]
fn test_direct_io() -> std::io::Result<()> {
    const BLOCK_SIZE: usize = 4096;
    let path = "/tmp/test_odirect.bin";

    // Ensure clean slate
    let _ = std::fs::remove_file(path);

    // Create file with O_DIRECT
    let mut f = DBFile::create(path)?;
    let block_size = FileSystem::block_size(path)?;
    // Original data to write (aligned buffer)
    let mut buffer = MemBuffer::alloc(BLOCK_SIZE, block_size)?;
    buffer.as_mut()[..14].copy_from_slice(b"Hello O_DIRECT");

    // Write to file
    let written = f.write(buffer.as_ref())?;
    assert_eq!(written, BLOCK_SIZE);

    // Flush to disk
    f.sync_all()?;

    // Seek to start
    f.seek(SeekFrom::Start(0))?;

    // Prepare read buffer (aligned)
    let mut zero_buf = MemBuffer::alloc(BLOCK_SIZE, block_size)?;

    // Read back
    let read_bytes = f.read(zero_buf.as_mut())?;
    assert_eq!(read_bytes, BLOCK_SIZE);

    // Check that the data matches
    assert_eq!(&zero_buf.as_ref()[..14], b"Hello O_DIRECT");

    // Cleanup file
    DBFile::remove(path)?;

    Ok(())
}
