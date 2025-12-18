use crate::{
    MAX_PAGE_SIZE, aligned_buf,
    io::disk::{DBFile, FileOperations, FileSystem, FileSystemBlockSize},
    param_tests,
};
use std::io::{self, Read, Seek, SeekFrom, Write};
use tempfile::tempdir;

fn create_test_file() -> io::Result<(DBFile, tempfile::TempDir)> {
    let dir = tempdir()?;
    let path = dir.path().join("test.db");
    let file = DBFile::create(&path)?;
    Ok((file, dir))
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_create_and_open() -> io::Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("create_test.db");

    // Create file
    let file = DBFile::create(&path)?;
    drop(file);

    // Open existing file
    let _file = DBFile::open(&path)?;

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_file_remove() -> io::Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("remove_test.db");

    DBFile::create(&path)?;
    assert!(path.exists());

    DBFile::remove(&path)?;
    assert!(!path.exists());

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_file_truncate() -> io::Result<()> {
    let (mut file, _dir) = create_test_file()?;
    let block_size = FileSystem::block_size(file.path())?;

    // Write some data
    let data = aligned_buf!(block_size, 0xAB);
    file.write_all(data.as_ref())?;
    file.sync_all()?;

    assert!(file.metadata()?.len() >= block_size as u64);

    // Truncate
    file.truncate()?;
    assert_eq!(file.metadata()?.len(), 0);

    Ok(())
}

fn test_write_read_aligned(block_size: usize) {
    let dir = tempdir().expect("Failed to create file");
    let path = dir.path().join("aligned_rw.db");
    let mut file = DBFile::create(&path).expect("Failed to create file");

    let fs_block_size = FileSystem::block_size(&path).unwrap();
    let write_size = block_size.next_multiple_of(fs_block_size);

    // Write aligned data
    let write_data = aligned_buf!(write_size, 0x42);
    file.write_all(write_data.as_ref()).unwrap();
    file.sync_all().unwrap();

    // Read back
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut read_data = aligned_buf!(write_size);
    file.read_exact(read_data.as_mut()).unwrap();

    assert_eq!(write_data, read_data);
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_seek_operations() -> io::Result<()> {
    let (mut file, _dir) = create_test_file()?;
    let block_size = FileSystem::block_size(file.path())?;

    // Write multiple blocks
    for i in 0..3u8 {
        let data = aligned_buf!(block_size, i);
        file.write_all(data.as_ref())?;
    }
    file.sync_all()?;

    // Seek to start
    let pos = file.seek(SeekFrom::Start(0))?;
    assert_eq!(pos, 0);

    // Seek to middle block
    let pos = file.seek(SeekFrom::Start(block_size as u64))?;
    assert_eq!(pos, block_size as u64);

    // Read and verify it's block 1 data
    let mut buf = aligned_buf!(block_size);
    file.read_exact(buf.as_mut())?;
    assert!(buf.data().iter().all(|&b| b == 1));

    // Seek from end
    let pos = file.seek(SeekFrom::End(-(block_size as i64)))?;
    assert_eq!(pos, 2 * block_size as u64);

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_multiple_writes_same_position() -> io::Result<()> {
    let (mut file, _dir) = create_test_file()?;
    let block_size = FileSystem::block_size(file.path())?;

    // Write initial data
    let data1 = aligned_buf!(block_size, 0xAA);
    file.write_all(data1.as_ref())?;
    file.sync_all()?;

    // Overwrite same position
    file.seek(SeekFrom::Start(0))?;
    let data2 = aligned_buf!(block_size, 0xBB);
    file.write_all(data2.as_ref())?;
    file.sync_all()?;

    // Verify overwrite
    file.seek(SeekFrom::Start(0))?;
    let mut read_buf = aligned_buf!(block_size);
    file.read_exact(read_buf.as_mut())?;
    assert!(read_buf.data().iter().all(|&b| b == 0xBB));

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_block_size_detection() -> io::Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("block_size_test.db");
    let _file = DBFile::create(&path)?;

    let block_size = FileSystem::block_size(&path)?;

    // Block size should be lower than [MAX PAGE SIZE]
    assert!(block_size <= MAX_PAGE_SIZE as usize);
    assert!(block_size.is_power_of_two());

    Ok(())
}

fn test_large_ops(num_blocks: usize) {
    let dir = tempdir().expect("Failed to create temp file");
    let path = dir.path().join("large_file.db");
    let mut file = DBFile::create(&path).expect("Failed to create temp file");
    let block_size = FileSystem::block_size(&path).unwrap();

    // Write many blocks
    for i in 0..num_blocks {
        let data = aligned_buf!(block_size, (i % 256) as u8);
        file.write_all(data.as_ref()).unwrap();
    }
    file.sync_all().unwrap();

    // Random access reads
    for i in (0..num_blocks).rev() {
        file.seek(SeekFrom::Start((i * block_size) as u64)).unwrap();
        let mut buf = vec![0u8; block_size];
        file.read_exact(&mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == (i % 256) as u8));
    }
}

// Parameterized write/read tests with different sizes
param_tests!(test_write_read_aligned, size => [4096, 8192, 16384, 32768]);

// Parameterized large file tests
param_tests!(test_large_ops, blocks => [10, 50, 100]);
