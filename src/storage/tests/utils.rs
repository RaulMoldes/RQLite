use crate::types::{Key, PageId, RowId, VarlenaType};
use crate::TextEncoding;
use crate::{
    BTreePage, IndexInteriorCell, IndexInteriorPage, IndexLeafCell, IndexLeafPage, InteriorPageOps,
    LeafPageOps, OverflowPage, PageHeader, PageType, TableInteriorCell, TableInteriorPage,
    TableLeafCell, TableLeafPage,
};

pub fn validate_index_leaf_cell(
    original: &IndexLeafCell,
    deserialized: &IndexLeafCell,
    idx: usize,
) {
    assert_eq!(
        original.payload.as_bytes(),
        deserialized.payload.as_bytes(),
        "Test case {}: payload mismatch",
        idx
    );
    assert_eq!(
        original.overflow_page, deserialized.overflow_page,
        "Test case {}: overflow_page mismatch",
        idx
    );
}

pub fn validate_index_interior_cell(
    original: &IndexInteriorCell,
    deserialized: &IndexInteriorCell,
    idx: usize,
) {
    assert_eq!(
        original.left_child_page, deserialized.left_child_page,
        "Test case {}: left_child_page mismatch",
        idx
    );
    assert_eq!(
        original.payload.as_bytes(),
        deserialized.payload.as_bytes(),
        "Test case {}: payload mismatch",
        idx
    );
    assert_eq!(
        original.overflow_page, deserialized.overflow_page,
        "Test case {}: overflow_page mismatch",
        idx
    );
}

pub fn validate_table_interior_cell(
    original: &TableInteriorCell,
    deserialized: &TableInteriorCell,
    idx: usize,
) {
    assert_eq!(
        original.left_child_page, deserialized.left_child_page,
        "Test case {}: left_child_page mismatch",
        idx
    );
    assert_eq!(
        original.key, deserialized.key,
        "Test case {}: key mismatch",
        idx
    );
}

pub fn validate_table_leaf_cell(
    original: &TableLeafCell,
    deserialized: &TableLeafCell,
    idx: usize,
) {
    assert_eq!(
        original.row_id, deserialized.row_id,
        "Test case {}: row_id mismatch",
        idx
    );
    assert_eq!(
        original.payload.as_bytes(),
        deserialized.payload.as_bytes(),
        "Test case {}: payload mismatch",
        idx
    );
    assert_eq!(
        original.overflow_page, deserialized.overflow_page,
        "Test case {}: overflow_page mismatch",
        idx
    );
}

pub fn validate_table_leaf_page(
    original: &BTreePage<TableLeafCell>,

    deserialized: &BTreePage<TableLeafCell>,
    idx: usize,
) {
    // Validate the header
    assert_eq!(
        original.header, deserialized.header,
        "Page header mismatch at idx: {}",
        idx
    );

    // Validate cell_indices
    assert_eq!(
        original.cell_indices.len(),
        deserialized.cell_indices.len(),
        "Test case {}: cell_indices length mismatch",
        idx
    );

    for (i, (orig_idx, deser_idx)) in original
        .cell_indices
        .iter()
        .zip(deserialized.cell_indices.iter())
        .enumerate()
    {
        assert_eq!(
            orig_idx, deser_idx,
            "Test case {}: cell_index[{}] mismatch",
            idx, i
        );
    }

    // Validate cells
    assert_eq!(
        original.cells.len(),
        deserialized.cells.len(),
        "Test case {}: cells length mismatch",
        idx
    );

    for (i, (orig_cell, deser_cell)) in original
        .cells
        .values()
        .zip(deserialized.cells.values())
        .enumerate()
    {
        assert_eq!(
            orig_cell.row_id, deser_cell.row_id,
            "Test case {}: cell[{}] row_id mismatch",
            idx, i
        );
        assert_eq!(
            orig_cell.payload.as_bytes(),
            deser_cell.payload.as_bytes(),
            "Test case {}: cell[{}] payload mismatch",
            idx,
            i
        );
        assert_eq!(
            orig_cell.overflow_page, deser_cell.overflow_page,
            "Test case {}: cell[{}] overflow_page mismatch",
            idx, i
        );
    }
}

/// Validates the equality of two btree interior pages
pub fn validate_table_interior_page(
    original: &BTreePage<TableInteriorCell>,

    deserialized: &BTreePage<TableInteriorCell>,
    idx: usize,
) {
    assert_eq!(
        original.header, deserialized.header,
        "Page header mismatch at idx: {}",
        idx
    );

    assert_eq!(
        original.cell_indices.len(),
        deserialized.cell_indices.len(),
        "Test case {}: cell_indices length mismatch",
        idx
    );

    assert_eq!(
        original.cells.len(),
        deserialized.cells.len(),
        "Test case {}: cells length mismatch",
        idx
    );

    for (i, (orig_cell, deser_cell)) in original
        .cells
        .values()
        .zip(deserialized.cells.values())
        .enumerate()
    {
        assert_eq!(
            orig_cell.left_child_page, deser_cell.left_child_page,
            "Test case {}: cell[{}] left_child_page mismatch",
            idx, i
        );
        assert_eq!(
            orig_cell.key, deser_cell.key,
            "Test case {}: cell[{}] key mismatch",
            idx, i
        );
    }
}

/// Validates the equality of two btree index leaf pages
pub fn validate_index_leaf_page(
    original: &BTreePage<IndexLeafCell>,

    deserialized: &BTreePage<IndexLeafCell>,
    idx: usize,
) {
    assert_eq!(
        original.header, deserialized.header,
        "Page header mismatch at idx: {}",
        idx
    );

    assert_eq!(
        original.cells.len(),
        deserialized.cells.len(),
        "Test case {}: cells length mismatch",
        idx
    );

    for (i, (orig_cell, deser_cell)) in original
        .cells
        .values()
        .zip(deserialized.cells.values())
        .enumerate()
    {
        assert_eq!(
            orig_cell.payload.as_bytes(),
            deser_cell.payload.as_bytes(),
            "Test case {}: cell[{}] payload mismatch",
            idx,
            i
        );

        assert_eq!(
            orig_cell.row_id, deser_cell.row_id,
            "Test case {}: cell[{}] row_id mismatch",
            idx, i
        );

        assert_eq!(
            orig_cell.overflow_page, deser_cell.overflow_page,
            "Test case {}: cell[{}] overflow_page mismatch",
            idx, i
        );
    }
}

/// Validates the equality of two btree index interior pages
pub fn validate_index_interior_page(
    original: &BTreePage<IndexInteriorCell>,

    deserialized: &BTreePage<IndexInteriorCell>,
    idx: usize,
) {
    assert_eq!(
        original.header, deserialized.header,
        "Page header mismatch at idx: {}",
        idx
    );

    assert_eq!(
        original.cells.len(),
        deserialized.cells.len(),
        "Test case {}: cells length mismatch",
        idx
    );

    for (i, (orig_cell, deser_cell)) in original
        .cells
        .values()
        .zip(deserialized.cells.values())
        .enumerate()
    {
        assert_eq!(
            orig_cell.left_child_page, deser_cell.left_child_page,
            "Test case {}: cell[{}] left_child_page mismatch",
            idx, i
        );
        assert_eq!(
            orig_cell.payload.as_bytes(),
            deser_cell.payload.as_bytes(),
            "Test case {}: cell[{}] payload mismatch",
            idx,
            i
        );
        assert_eq!(
            orig_cell.overflow_page, deser_cell.overflow_page,
            "Test case {}: cell[{}] overflow_page mismatch",
            idx, i
        );
    }
}

/// Validates OverflowPage
pub fn validate_overflow_page(original: &OverflowPage, deserialized: &OverflowPage, idx: usize) {
    assert_eq!(
        original.data.as_bytes(),
        deserialized.data.as_bytes(),
        "Test case {}: data mismatch",
        idx
    );
}

pub fn create_table_leaf_page() -> TableLeafPage {
    let mut page = TableLeafPage::default();

    // Add cells
    let cell1 = TableLeafCell {
        row_id: RowId::new_key(),
        payload: VarlenaType::from_str("Hello World", TextEncoding::Utf8),
        overflow_page: None,
    };

    let cell2 = TableLeafCell {
        row_id: RowId::new_key(),
        payload: VarlenaType::from_str("Test Data", TextEncoding::Utf8),
        overflow_page: Some(PageId::from(100)),
    };

    let cell3 = TableLeafCell {
        row_id: RowId::new_key(),
        payload: VarlenaType::from_raw_bytes(&[1, 2, 3, 4, 5], None),
        overflow_page: None,
    };

    page.insert(cell1.row_id, cell1)
        .expect("Failed to add cell1");
    page.insert(cell2.row_id, cell2)
        .expect("Failed to add cell2");
    page.insert(cell3.row_id, cell3)
        .expect("Failed to add cell3");

    page
}

pub fn create_table_interior_page() -> BTreePage<TableInteriorCell> {
    let mut page = TableInteriorPage::default();

    let cell1 = TableInteriorCell::new(PageId::from(10), RowId::from(100));
    let cell2 = TableInteriorCell::new(PageId::from(20), RowId::from(200));
    let cell3 = TableInteriorCell::new(PageId::from(30), RowId::from(300));

    page.insert_child(cell1.key, cell1.left_child_page)
        .expect("Failed to add cell1");
    page.insert_child(cell2.key, cell2.left_child_page)
        .expect("Failed to add cell2");
    page.insert_child(cell3.key, cell3.left_child_page)
        .expect("Failed to add cell3");

    page
}

pub fn create_index_leaf_page() -> BTreePage<IndexLeafCell> {
    let mut page = IndexLeafPage::default();

    let cell1 = IndexLeafCell::new(
        VarlenaType::from_raw_bytes(b"index_key_1", None),
        RowId::from(0),
    );
    let cell2 = IndexLeafCell::new(
        VarlenaType::from_raw_bytes(b"index_key_2", None),
        RowId::from(3),
    );

    page.insert(cell1.payload.clone(), cell1)
        .expect("Failed to add cell1");
    page.insert(cell2.payload.clone(), cell2)
        .expect("Failed to add cell2");

    page
}

pub fn create_index_interior_page() -> BTreePage<IndexInteriorCell> {
    let mut page = IndexInteriorPage::default();

    let cell1 = IndexInteriorCell::new(
        PageId::from(50),
        VarlenaType::from_raw_bytes(b"interior_key_1", None),
    );
    let cell2 = IndexInteriorCell::new(
        PageId::from(60),
        VarlenaType::from_raw_bytes(b"interior_key_2", None),
    );

    page.insert_child(cell1.payload, cell1.left_child_page)
        .expect("Failed to add cell1");
    page.insert_child(cell2.payload, cell2.left_child_page)
        .expect("Failed to add cell2");

    page
}

pub fn create_overflow_page(page_num: PageId, data: &[u8]) -> OverflowPage {
    let header = PageHeader {
        page_size: 4096,
        page_number: page_num,
        right_most_page: PageId::default(),
        next_overflow_page: PageId::default(),
        free_space_ptr: 0,
        cell_count: 0,
        content_start_ptr: 0,
        page_type: PageType::Overflow,
        reserved_space: 0,
    };
    OverflowPage::new(header, VarlenaType::from_raw_bytes(data, None))
}

pub fn create_row_id() -> RowId {
    RowId::new_key()
}
