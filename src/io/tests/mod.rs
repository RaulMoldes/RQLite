use crate::io::disk::{DirectIO, FOpenMode, FileOperations};

use std::io::SeekFrom;
use std::io::{Read, Result, Seek, Write};
use tempfile::TempDir;

const TEST_PAGE_SIZE: u32 = 4096;

#[cfg(target_os = "linux")]
mod libfiu;

// TODO: FIND A WAY TO PROPERLY TEST THIS
#[test]
#[cfg(target_os = "linux")]
#[ignore = "Ignored due to libfiu not working properly."]
fn test_sync_all_with_fiu_injection() -> Result<()> {
    if !libfiu::init() {
        eprintln!("This test requires libfiu installed: ");
        eprintln!("sudo apt-get update && sudo apt-get install libfiu-dev fiu-utils");
        panic!("libfiu not available, skipping test");
    }

    let temp_dir = TempDir::new()?;
    let file_path = temp_dir.path().join("test_fiu.db");

    let mut file = DirectIO::create(&file_path, FOpenMode::ReadWrite)?;
    let buffer = DirectIO::alloc_aligned(4096)?;
    buffer[..14].copy_from_slice(b"Hello O_DIRECT");

    file.write_all(buffer)?;
    file.flush()?;

    // Activate fault injection for fsync
    assert!(libfiu::enable_fsync_failure());

    // sync_all should fail now
    let result = file.sync_all();
    assert!(result.is_err(), "sync_all should fail with fault injection");

    // Deactivate fault injection
    libfiu::disable_fsync_failure();

    // Now should work
    let result = file.sync_all();
    assert!(
        result.is_ok(),
        "sync_all should succeed after disabling fault injection"
    );

    Ok(())
}

#[test]
#[cfg(target_os = "linux")]
fn test_direct_io() -> std::io::Result<()> {
    const BLOCK_SIZE: usize = 4096;
    let path = "/tmp/test_odirect.bin";

    // Ensure clean slate
    let _ = std::fs::remove_file(path);

    // Create DirectIO file
    let mut file = DirectIO::create(path, FOpenMode::ReadWrite)?;

    // Original data to write
    let buffer = DirectIO::alloc_aligned(4096)?;
    buffer[..14].copy_from_slice(b"Hello O_DIRECT");

    // Write to file
    let written = file.write(buffer)?;
    assert_eq!(written, BLOCK_SIZE);

    // Flush to disk
    file.sync_all()?;

    // Seek to start
    file.seek(SeekFrom::Start(0))?;

    // Prepare read buffer using ensure_aligned
    let zero_buf = DirectIO::alloc_aligned(4096)?;
    // Read back
    let read_bytes = file.read(zero_buf)?;
    assert_eq!(read_bytes, BLOCK_SIZE);

    // Check that the data matches the original
    assert_eq!(&zero_buf[..buffer.len()], buffer);

    // Cleanup file
    DirectIO::remove(path)?;

    Ok(())
}
