
use super::*;
use crate::header::Header;
use crate::io::cache::StaticPool;
use crate::io::disk::Buffer;
use crate::io::pager::Pager;
use crate::serialization::Serializable;
use crate::types::RowId;
use std::io::Seek;

// Helper to create a test pager in memory
fn setup_test_pager() -> Pager<Buffer, StaticPool> {
    let mut pager = Pager::<Buffer, StaticPool>::create("test.db").unwrap();

    // Initialize the header
    let header = Header::default();
    // Write the header to the pager
    pager.seek(std::io::SeekFrom::Start(0)).unwrap();
    header.write_to(&mut pager).unwrap();

    // Init the pager with cache capacity
    pager.start(100).unwrap();

    pager
}

// Helper para simple test cells.
fn create_test_cell(row_id: RowId, data: Vec<u8>) -> TableLeafCell {
    let payload = VarlenaType::from_raw_bytes(&data, None);
    TableLeafCell {
        row_id,
        payload,
        overflow_page: None,
    }
}




#[test]
fn test_single_record() {
    let mut pager = setup_test_pager();
    let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.8, 0.2).unwrap();

    let row_id = RowId::from(1);
    let cell = create_test_cell(row_id, vec![1, 2, 3, 4]);
    btree.print_tree(&mut pager).unwrap();
    // Insert
    btree.insert(row_id, cell.clone(), &mut pager).unwrap();
    btree.print_tree(&mut pager).unwrap();

    // Search
    let found = btree.search(row_id, &mut pager);
    assert!(found.is_some(), "Inserted cell was not found!");
    assert_eq!(found.unwrap().row_id, row_id);
}

#[test]
fn test_multiple_sequential_inserts() {
    let mut pager = setup_test_pager();
    let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.8, 0.2).unwrap();

    // Insert 100 registers
    for i in 1..=100 {
        let row_id = RowId::from(i);
        let data = vec![i as u8; 10];
        let cell = create_test_cell(row_id, data);
        btree.insert(row_id, cell, &mut pager).unwrap();
    }

    // Verify all can be found
    for i in 1..=100 {
        let row_id = RowId::from(i);
        let found = btree.search(row_id, &mut pager);
        assert!(found.is_some(), "No se encontró el row_id: {}", i);
        assert_eq!(found.unwrap().row_id, row_id);
    }
}

#[test]
fn test_overflow() {
    let mut pager = setup_test_pager();
    let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.3, 0.1).unwrap();

    let row_id = RowId::from(1);

    // Create a big payload
    let large_data: Vec<u8> = (0..2048).map(|i| (i % 256) as u8).collect();
    let cell = create_test_cell(row_id, large_data.clone());

    btree.insert(row_id, cell, &mut pager).unwrap();

    let found = btree.search(row_id, &mut pager);
    assert!(found.is_some());
    assert_eq!(found.unwrap().payload.as_bytes(), large_data.as_slice());
}

#[test]
fn test_root_split() {
    let mut pager = setup_test_pager();
    let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.3, 0.1).unwrap();

    let original_root = btree.root;

    // Insert enough cells to force overflow state at some point.
    let cell_size = 500;

    for i in 1..=10 {
        let row_id = RowId::from(i);
        let data = vec![i as u8; cell_size];
        let cell = create_test_cell(row_id, data);
        btree.insert(row_id, cell, &mut pager).unwrap();
    }

    // Verify that the root changed.
    assert_ne!(
        btree.root, original_root,
        "Root should have changed after split"
    );

    // Verify the new root is an interior page.
    let new_root_frame = pager.get_single(btree.root).unwrap();
    assert!(
        new_root_frame.is_interior(),
        "Root should be an interior page."
    );

    btree.print_tree(&mut pager).unwrap();

    // Verify all cells can still be found.
    for i in 1..=10 {
        let row_id = RowId::from(i);
        let found = btree.search(row_id, &mut pager);
        assert!(
            found.is_some(),
            "Cell with row_id {} should exist after split",
            i
        );
    }
}

#[test]
fn test_insert_duplicate_keys() {
    let mut pager = setup_test_pager();
    let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.8, 0.2).unwrap();

    let row_id = RowId::from(1);
    let cell1 = create_test_cell(row_id, vec![1; 10]);
    let cell2 = create_test_cell(row_id, vec![2; 10]);

    btree.insert(row_id, cell1, &mut pager).unwrap();
    btree.insert(row_id, cell2, &mut pager).unwrap();

    let found = btree.search(row_id, &mut pager).unwrap();
    // Should have the latest inserted value
    assert_eq!(found.payload.as_bytes()[0], 2);
}

#[test]
fn test_insert_varying_sizes() {
    let mut pager = setup_test_pager();
    let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.8, 0.2).unwrap();

    //
    for i in 1..=50 {
        let row_id = RowId::from(i);
        let size = (i * 20) as usize; // Tamaños de 20 a 1000 bytes
        let cell = create_test_cell(row_id, vec![i as u8; size]);
        btree.insert(row_id, cell, &mut pager).unwrap();
    }

    for i in 1..=50 {
        let row_id = RowId::from(i);
        let found = btree.search(row_id, &mut pager);
        assert!(found.is_some());
        assert_eq!(found.unwrap().payload.as_bytes().len(), (i * 20) as usize);
    }
}

#[test]
fn test_delete_single_element() {
    let mut pager = setup_test_pager();
    let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.8, 0.2).unwrap();

    let row_id = RowId::from(1);
    let cell = create_test_cell(row_id, vec![1; 10]);
    btree.insert(row_id, cell, &mut pager).unwrap();

    btree.delete(row_id, &mut pager).unwrap();

    assert!(btree.search(row_id, &mut pager).is_none());
}

#[test]
fn test_delete_all_elements() {
    let mut pager = setup_test_pager();
    let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.8, 0.2).unwrap();

    for i in 1..=50 {
        let row_id = RowId::from(i);
        let cell = create_test_cell(row_id, vec![i as u8; 10]);
        btree.insert(row_id, cell, &mut pager).unwrap();
    }

    for i in 1..=50 {
        let row_id = RowId::from(i);
        btree.delete(row_id, &mut pager).unwrap();
    }

    for i in 1..=50 {
        let row_id = RowId::from(i);
        assert!(btree.search(row_id, &mut pager).is_none());
    }
}

#[test]
fn test_delete_and_reinsert() {
    let mut pager = setup_test_pager();
    let mut btree = TableBTree::create(&mut pager, BTreeType::Table, 0.8, 0.2).unwrap();

    for i in 1..=50 {
        let row_id = RowId::from(i);
        let cell = create_test_cell(row_id, vec![i as u8; 10]);
        btree.insert(row_id, cell, &mut pager).unwrap();
    }

    // Delete half of them
    for i in 1..=25 {
        let row_id: RowId = RowId::from(i);
        btree.delete(row_id, &mut pager).unwrap();
    }

    // Reinsert with different data
    for i in 1..=25 {
        let row_id = RowId::from(i);
        let cell = create_test_cell(row_id, vec![i as u8 + 100; 10]);
        btree.insert(row_id, cell, &mut pager).unwrap();
    }

    // Verify the new data
    for i in 1..=25 {
        let row_id = RowId::from(i);
        let found = btree.search(row_id, &mut pager).unwrap();
        assert_eq!(found.payload.as_bytes()[0], i as u8 + 100);
    }
}
