use std::fs::File;
use std::io::{Read, Result, Write};
use tempfile::TempDir;
mod utils;

const TEST_PAGE_SIZE: u32 = 4096;

#[cfg(target_os = "linux")]
mod libfiu;

#[test]
fn test_sync_all() -> Result<()> {
    // Create a temporary directory for the test
    let temp_dir = TempDir::new()?;
    let file_path = temp_dir.path().join("test_sync.db");

    // Write the data and call `sync`
    {
        let mut file = File::create(&file_path)?;
        let test_data = b"Hello, persistent world!";
        file.write_all(test_data)?;
        file.flush()?;
        file.sync_all()?;
        // File closes here
    }

    // Verify the data is persisted after opening the file.
    let mut file = File::open(&file_path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    assert_eq!(contents, "Hello, persistent world!");

    Ok(())
}

// TODO: FIND A WAY TO PROPERLY TEST THIS
#[test]
#[cfg(target_os = "linux")]
#[ignore = "Using LIBFIU to test syscall failures on userspace. For now it does not seem to be properly reporting failures on sync error fails so I disabled it."]
fn test_sync_all_with_fiu_injection() -> Result<()> {
    if !libfiu::init() {
        eprintln!("This test requires libfiu installed: ");
        eprintln!("sudo apt-get update && sudo apt-get install libfiu-dev fiu-utils");
        panic!("libfiu not available, skipping test");
    }

    let temp_dir = TempDir::new()?;
    let file_path = temp_dir.path().join("test_fiu.db");

    let mut file = File::create(&file_path)?;
    file.write_all(b"test data")?;
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
