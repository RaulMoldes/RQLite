use crate::io::frames::{Frame, IOFrame, IndexFrame, OverflowFrame, TableFrame};
use crate::test_frame_conversion;
use crate::{
    types::{Key, PageId},
    PageType,
};
use std::fs::File;
use std::io::{Read, Result, Write};
use tempfile::TempDir;
mod utils;

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

test_frame_conversion!(
    test_io_to_table_frame_conversion,
    IOFrame => TableFrame,
    [
        (PageId::new_key(), PageType::TableLeaf, 4096, None),
        (PageId::new_key(), PageType::TableInterior, 4096, Some(PageId::new_key()))
    ],
    |frame: &TableFrame, idx: usize| {
        assert!(frame.is_table(), "Test case {}: Frame should be a table frame", idx);
        assert!(!frame.is_index(), "Test case {}: Frame should not be an index frame", idx);

        // Validar que el page_type es correcto
        match idx {
            0 => assert_eq!(frame.page_type(), PageType::TableLeaf),
            1 => assert_eq!(frame.page_type(), PageType::TableInterior),
            _ => panic!("Unexpected test case index"),
        }
    }
);

test_frame_conversion!(
    test_io_to_index_frame_conversion,
    IOFrame => IndexFrame,
    [
        (PageId::new_key(), PageType::IndexLeaf, 4096, None),
        (PageId::new_key(), PageType::IndexInterior, 4096, Some(PageId::new_key()))
    ],
    |frame: &IndexFrame, idx: usize| {
        assert!(frame.is_index(), "Test case {}: Frame should be an index frame", idx);
        assert!(!frame.is_table(), "Test case {}: Frame should not be a table frame", idx);
        assert!(!frame.is_dirty(), "Test case {}: Newly converted frame should not be dirty", idx);
    }
);

test_frame_conversion!(
    test_io_to_overflow_frame_conversion,
    IOFrame => OverflowFrame,
    [
        (PageId::new_key(), PageType::Overflow, 4096, None)
    ],
    |frame: &OverflowFrame, _idx: usize| {
        assert!(frame.is_overflow(), "Frame should be an overflow frame");
        assert_eq!(frame.page_type(), PageType::Overflow);
    }
);
