
use crate::io::pager::Pager;


use crate::PageType;
use std::fs::File;
use std::io::{Read, Result, Write};
use tempfile::TempDir;
mod utils;
use crate::io::cache::StaticPool;
use crate::io::disk::{Buffer, FileOps};
use tempfile::NamedTempFile;

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

#[test]
fn test_pager() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();
    let mut pager: Pager<Buffer, StaticPool> = Pager::create(path).unwrap();

    // Force to use a small cache size to force eviction
    pager.start_with(2).unwrap();

    // Create three pages (greater than cache capacity)
    let mut page_ids = Vec::new();
    for i in 0..3 {
        let mut frame = pager.alloc_page(PageType::TableLeaf).unwrap();
        let page_id = frame.id();
        println!("Generado page id: {}", page_id);
        page_ids.push(page_id);

        // Mark the page as dirty to force writing to disk
        frame.mark_dirty();
    }

    // Verify that there are only two pages on cache
    let cached_count = page_ids
        .iter()
        .filter(|id| pager.try_get_from_cache(**id).is_some())
        .count();
    assert!(
        cached_count <= 2,
        "There should be two pages on cache only!"
    );

    // Flush should evict all dirty pages to the disk
    assert!(pager.flush().is_ok());

    // After clearing the cache it will get empty.
    // We should be able to read from the disk now.
    pager.clear_cache();

    // Verify that we are able to do so
    for page_id in page_ids {
        let read_frame = pager.get_single(page_id);
        assert!(
            read_frame.is_ok(),
            "Should be able to read page {:?} from disk",
            page_id
        );
        assert_eq!(read_frame.unwrap().id(), page_id);
    }

    // Sync all
    assert!(pager.sync_all().is_ok());
}
