use crate::{
    AxmosDBConfig, DEFAULT_BTREE_MIN_KEYS, DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE, DEFAULT_CACHE_SIZE,
    DEFAULT_PAGE_SIZE, default_num_workers,
    io::{disk::FileOperations, pager::Pager},
    matrix_tests, param_tests,
    storage::{
        BtreeBuffer,
        cell::OwnedCell,
        page::{BtreePage, PageZero},
    },
    types::PageId,
};
use std::io;
use tempfile::tempdir;

fn create_test_config(page_size: u32, cache_size: u16) -> AxmosDBConfig {
    AxmosDBConfig::new(
        page_size,
        cache_size,
        default_num_workers() as u8,
        DEFAULT_BTREE_MIN_KEYS,
        DEFAULT_BTREE_NUM_SIBLINGS_PER_SIDE,
    )
}

fn create_test_pager(page_size: u32, cache_size: u16) -> io::Result<(Pager, tempfile::TempDir)> {
    let dir = tempdir()?;
    let path = dir.path().join("test.db");
    let config = create_test_config(page_size, cache_size);
    let pager = Pager::from_config(config, &path)?;
    Ok((pager, dir))
}

fn create_default_pager() -> io::Result<(Pager, tempfile::TempDir)> {
    create_test_pager(DEFAULT_PAGE_SIZE, DEFAULT_CACHE_SIZE as u16)
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_pager_create_and_open() -> io::Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("pager_test.db");

    // Create pager
    let config = create_test_config(DEFAULT_PAGE_SIZE, 10);
    let pager = Pager::from_config(config, &path)?;
    drop(pager);

    // Open existing
    let _pager = Pager::open(&path)?;

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_pager_initialization() -> io::Result<()> {
    let (pager, _dir) = create_default_pager()?;

    assert!(pager.is_initialized());

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_pager_remove() -> io::Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("remove_test.db");
    let wal_path = dir.path().join("axmos.log");

    let config = create_test_config(DEFAULT_PAGE_SIZE, 10);
    let _pager = Pager::from_config(config, &path)?;
    drop(_pager);

    assert!(path.exists());
    assert!(wal_path.exists());

    Pager::remove(&path)?;

    assert!(!path.exists());
    assert!(!wal_path.exists());

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_allocate_single_page() -> io::Result<()> {
    let (mut pager, _dir) = create_default_pager()?;

    let page_id = pager.allocate_page::<BtreePage>()?;

    // Should not be page zero
    assert_ne!(page_id, PageId::default());

    // Should be readable
    let frame = pager.read_page::<BtreePage>(page_id)?;
    assert_eq!(frame.page_number(), page_id);

    Ok(())
}

fn test_allocate_multiple_pages(num_pages: usize) -> io::Result<()> {
    let (mut pager, _dir) = create_default_pager()?;

    let mut page_ids = Vec::with_capacity(num_pages);
    for _ in 0..num_pages {
        let id = pager.allocate_page::<BtreePage>()?;
        page_ids.push(id);
    }

    // All IDs should be unique
    let mut sorted = page_ids.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), num_pages);

    // All pages should be readable
    for id in &page_ids {
        let frame = pager.read_page::<BtreePage>(*id)?;
        assert_eq!(frame.page_number(), *id);
    }

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_deallocate_page() -> io::Result<()> {
    let (mut pager, _dir) = create_default_pager()?;

    let page_id = pager.allocate_page::<BtreePage>()?;
    pager.dealloc_page::<BtreePage>(page_id)?;

    Ok(())
}

fn test_page_reuse_after_dealloc(num_pages: usize) -> io::Result<()> {
    let (mut pager, _dir) = create_default_pager()?;

    // Allocate pages
    let mut original_ids = Vec::new();
    for _ in 0..num_pages {
        original_ids.push(pager.allocate_page::<BtreePage>()?);
    }

    // Deallocate all
    for &id in &original_ids {
        pager.dealloc_page::<BtreePage>(id)?;
    }

    // Allocate again - should reuse
    let mut new_ids = Vec::new();
    for _ in 0..num_pages {
        new_ids.push(pager.allocate_page::<BtreePage>()?);
    }

    // Check reuse
    for new_id in &new_ids {
        assert!(
            original_ids.contains(new_id),
            "Page {} should be reused from {:?}",
            new_id,
            original_ids
        );
    }

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_cannot_deallocate_page_zero() -> io::Result<()> {
    let (mut pager, _dir) = create_default_pager()?;

    let result = pager.dealloc_page::<BtreePage>(PageId::default());
    assert!(result.is_err());

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_write_and_read_page_data() -> io::Result<()> {
    let (mut pager, _dir) = create_default_pager()?;

    let page_id = pager.allocate_page::<BtreePage>()?;
    let test_data = b"Hello, AxmosDB!";

    // Write data
    pager.with_page_mut::<BtreePage, _, _>(page_id, |page| {
        page.as_mut()[..test_data.len()].copy_from_slice(test_data);
    })?;

    // Read back
    pager.with_page::<BtreePage, _, _>(page_id, |page| {
        assert_eq!(&page.as_ref()[..test_data.len()], test_data);
    })?;

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_page_persistence_after_reopen() -> io::Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("persist_test.db");
    let test_data = b"Persistent data!";
    let mut page_id = PageId::default();

    // Create, write, and close
    {
        let config = create_test_config(DEFAULT_PAGE_SIZE, 10);
        let mut pager = Pager::from_config(config, &path)?;

        page_id = pager.allocate_page::<BtreePage>()?;

        pager.with_page_mut::<BtreePage, _, _>(page_id, |page| {
            page.as_mut()[..test_data.len()].copy_from_slice(test_data);
        })?;

        pager.sync_all()?;
    }

    // Reopen and verify
    {
        let mut pager = Pager::open(&path)?;

        pager.with_page::<BtreePage, _, _>(page_id, |page| {
            assert_eq!(&page.as_ref()[..test_data.len()], test_data);
        })?;
    }

    Ok(())
}

fn test_cache_eviction(num_pages: usize, cache_size: usize) -> io::Result<()> {
    let (mut pager, _dir) = create_test_pager(DEFAULT_PAGE_SIZE, cache_size as u16)?;

    // Allocate more pages than cache size
    let pages_to_alloc = num_pages.max(cache_size + 5);
    let mut page_ids = Vec::new();

    for i in 0..pages_to_alloc {
        let page_id = pager.allocate_page::<BtreePage>()?;

        // Write unique data to each page
        let data = format!("Page data {}", i);
        pager.with_page_mut::<BtreePage, _, _>(page_id, |page| {
            page.as_mut()[..data.len()].copy_from_slice(data.as_bytes());
        })?;

        page_ids.push((page_id, data));
    }

    // Sync to ensure evicted pages are written
    pager.sync_all()?;

    // Verify all pages (some will need to be read from disk)
    for (page_id, expected_data) in &page_ids {
        pager.with_page::<BtreePage, _, _>(*page_id, |page| {
            assert_eq!(
                &page.as_ref()[..expected_data.len()],
                expected_data.as_bytes()
            );
        })?;
    }

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_cache_hit_on_repeated_access() -> io::Result<()> {
    let (mut pager, _dir) = create_default_pager()?;

    let page_id = pager.allocate_page::<BtreePage>()?;

    // Access same page multiple times
    for _ in 0..10 {
        let _frame = pager.read_page::<BtreePage>(page_id)?;
    }

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_page_zero_always_available() -> io::Result<()> {
    let (mut pager, _dir) = create_default_pager()?;

    // Reading page zero should always work
    let frame = pager.read_page::<PageZero>(PageId::default())?;
    assert_eq!(frame.page_number(), PageId::default());

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_page_cell_insert() -> io::Result<()> {
    let (mut pager, _dir) = create_default_pager()?;

    let page_id = pager.allocate_page::<BtreePage>()?;
    let cell_data = b"Test cell payload";

    pager.try_with_page_mut::<BtreePage, _, _>(page_id, |page| {
        let cell = OwnedCell::new(cell_data);
        page.push(cell)?;

        assert_eq!(page.num_slots(), 1);
        Ok(())
    })?;

    // Verify cell is readable
    pager.with_page::<BtreePage, _, _>(page_id, |page| {
        let cell = page.cell(crate::storage::cell::Slot(0));
        assert_eq!(cell.payload(), cell_data);
    })?;

    Ok(())
}

fn test_page_multiple_cells(num_cells: usize) -> io::Result<()> {
    let (mut pager, _dir) = create_default_pager()?;

    let page_id = pager.allocate_page::<BtreePage>()?;

    pager.try_with_page_mut::<BtreePage, _, _>(page_id, |page| {
        for i in 0..num_cells {
            let data = format!("Cell {}", i);
            let cell = OwnedCell::new(data.as_bytes());

            if page.has_space_for(cell.storage_size()) {
                page.push(cell)?;
            } else {
                break;
            }
        }
        Ok(())
    })?;

    // Verify cells
    pager.with_page::<BtreePage, _, _>(page_id, |page| {
        let actual_cells = page.num_slots() as usize;
        for i in 0..actual_cells {
            let expected = format!("Cell {}", i);
            let cell = page.cell(crate::storage::cell::Slot(i as u16));
            assert_eq!(cell.payload(), expected.as_bytes());
        }
    })?;

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
#[serial_test::serial]
fn test_page_cell_remove() -> io::Result<()> {
    let (mut pager, _dir) = create_default_pager()?;

    let page_id = pager.allocate_page::<BtreePage>()?;

    // Insert cells
    pager.try_with_page_mut::<BtreePage, _, _>(page_id, |page| {
        for i in 0..5 {
            let cell = OwnedCell::new(format!("Cell {}", i).as_bytes());
            page.push(cell)?;
        }
        Ok(())
    })?;

    // Remove middle cell
    pager.try_with_page_mut::<BtreePage, _, _>(page_id, |page| {
        page.remove(crate::storage::cell::Slot(2))?;
        assert_eq!(page.num_slots(), 4);
        Ok(())
    })?;

    Ok(())
}

fn test_stress_allocate_deallocate(iterations: usize, pages_per_iter: usize) -> io::Result<()> {
    let (mut pager, _dir) = create_default_pager()?;

    for _ in 0..iterations {
        // Allocate
        let mut ids = Vec::new();
        for _ in 0..pages_per_iter {
            ids.push(pager.allocate_page::<BtreePage>()?);
        }

        // Deallocate half
        for id in ids.iter().take(pages_per_iter / 2) {
            pager.dealloc_page::<BtreePage>(*id)?;
        }
    }

    Ok(())
}

fn test_stress_random_access(num_pages: usize, accesses: usize) -> io::Result<()> {
    let (mut pager, _dir) = create_default_pager()?;

    // Allocate pages
    let mut page_ids = Vec::new();
    for _ in 0..num_pages {
        page_ids.push(pager.allocate_page::<BtreePage>()?);
    }

    // Random access pattern (using simple deterministic "random")
    for i in 0..accesses {
        let idx = (i * 7 + 3) % num_pages;
        let page_id = page_ids[idx];

        let _frame = pager.read_page::<BtreePage>(page_id)?;
    }

    Ok(())
}

fn test_different_page_sizes(page_size: usize) -> io::Result<()> {
    let (mut pager, _dir) = create_test_pager(page_size as u32, 10)?;

    let page_id = pager.allocate_page::<BtreePage>()?;

    pager.with_page::<BtreePage, _, _>(page_id, |page| {
        assert!(page.capacity() >= page_size - 128); // Account for header
    })?;

    Ok(())
}

fn test_different_cache_sizes(cache_size: usize) -> io::Result<()> {
    let (mut pager, _dir) = create_test_pager(DEFAULT_PAGE_SIZE, cache_size as u16)?;

    // Allocate and access pages
    let mut page_ids = Vec::new();
    for _ in 0..cache_size * 2 {
        page_ids.push(pager.allocate_page::<BtreePage>()?);
    }

    // All should still be accessible
    for id in &page_ids {
        let _frame = pager.read_page::<BtreePage>(*id)?;
    }

    Ok(())
}

// Parameterized allocation tests
param_tests!(test_allocate_multiple_pages, n => [5, 10, 50, 100]);

// Parameterized reuse tests
param_tests!(test_page_reuse_after_dealloc, n => [3, 5, 10]);

// Parameterized cell tests
param_tests!(test_page_multiple_cells, n => [5, 10, 20, 50]);

// Matrix tests for cache eviction
matrix_tests!(
    test_cache_eviction,
    pages => [10, 50, 100],
    cache => [5, 10, 20]
);

// Matrix tests for stress
matrix_tests!(
    test_stress_allocate_deallocate,
    iters => [5, 10],
    pages => [5, 10, 20]
);

matrix_tests!(
    test_stress_random_access,
    pages => [10, 50],
    accesses => [100, 500]
);

// Page size tests
param_tests!(test_different_page_sizes, size => [4096, 8192, 16384]);

// Cache size tests
param_tests!(test_different_cache_sizes, size => [5, 10, 20, 50]);
