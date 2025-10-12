use crate::storage::btreepage::BTreePage;
use crate::storage::cell::{IndexInteriorCell, IndexLeafCell, TableInteriorCell, TableLeafCell};
use crate::storage::{IndexInteriorPage, IndexLeafPage, TableInteriorPage, TableLeafPage};
use crate::types::{Key, PageId};
use crate::types::{RowId, VarlenaType};
use crate::{test_serializable, OverflowPage};
use crate::{HeaderOps, InteriorPageOps, LeafPageOps, PageType, TextEncoding};
mod utils;
use utils::*;

#[macro_use]
mod macros;

test_serializable!(
    test_table_leaf_page,
    BTreePage<TableLeafCell>,
    [create_table_leaf_page(), create_table_leaf_page()],
    validate_table_leaf_page
);

test_serializable!(
    test_table_interior_page,
    BTreePage<TableInteriorCell>,
    [create_table_interior_page(), create_table_interior_page()],
    validate_table_interior_page
);

test_serializable!(
    test_index_leaf_page,
    BTreePage<IndexLeafCell>,
    [create_index_leaf_page(), create_index_leaf_page()],
    validate_index_leaf_page
);

test_serializable!(
    test_index_interior_page,
    BTreePage<IndexInteriorCell>,
    [create_index_interior_page(), create_index_interior_page()],
    validate_index_interior_page
);

test_serializable!(
    test_overflow_page,
    OverflowPage,
    [
        create_overflow_page(PageId::from(1000), b"Small data"),
        create_overflow_page(PageId::from(2000), &vec![0xAB; 3000]), // Large data
        create_overflow_page(PageId::from(3000), b"")
    ],
    validate_overflow_page
);

test_serializable!(
    test_index_interior_cell,
    IndexInteriorCell,
    [
        IndexInteriorCell::new(
            crate::types::PageId::from(50),
            VarlenaType::from_raw_bytes(b"interior_key_1", None)
        ),
        IndexInteriorCell::new(
            crate::types::PageId::from(60),
            VarlenaType::from_raw_bytes(b"interior_key_2", None)
        ),
        IndexInteriorCell::new(
            crate::types::PageId::from(100),
            VarlenaType::from_raw_bytes(b"exterior_key_3", None)
        )
    ],
    validate_index_interior_cell
);

test_serializable!(
    test_index_leaf_cell,
    IndexLeafCell,
    [
        IndexLeafCell::new(
            VarlenaType::from_raw_bytes(b"index_key_1", None),
            RowId::from(0)
        ),
        IndexLeafCell::new(
            VarlenaType::from_raw_bytes(b"index_key_2", None),
            RowId::from(1)
        ),
        IndexLeafCell::new(VarlenaType::from_raw_bytes(b"a", None), RowId::from(2)),
        IndexLeafCell::new(VarlenaType::from_raw_bytes(b"", None), RowId::from(3)),
        IndexLeafCell {
            row_id: RowId::from(5),
            payload: VarlenaType::from_raw_bytes(b"key_with_overflow", None),
            overflow_page: Some(PageId::from(777)),
        },
        IndexLeafCell {
            row_id: RowId::from(7),
            payload: VarlenaType::from_raw_bytes(&vec![0x42; 2000], None),
            overflow_page: Some(PageId::from(888)),
        }
    ],
    validate_index_leaf_cell
);

test_serializable!(
    test_table_interior_cell,
    TableInteriorCell,
    [
        TableInteriorCell::new(PageId::from(10), RowId::from(100)),
        TableInteriorCell::new(PageId::from(20), RowId::from(200)),
        TableInteriorCell::new(PageId::from(30), RowId::from(300))
    ],
    validate_table_interior_cell
);

test_serializable!(
    test_table_leaf_cell,
    TableLeafCell,
    [
        TableLeafCell {
            row_id: RowId::new_key(),
            payload: VarlenaType::from_str("Hello World", TextEncoding::Utf8),
            overflow_page: None,
        },
        TableLeafCell {
            row_id: RowId::new_key(),
            payload: VarlenaType::from_str("Test", TextEncoding::Utf8),
            overflow_page: None,
        },
        TableLeafCell {
            row_id: RowId::new_key(),
            payload: VarlenaType::from_str("Data with overflow", TextEncoding::Utf8),
            overflow_page: Some(PageId::from(999)),
        },
        TableLeafCell {
            row_id: RowId::new_key(),
            payload: VarlenaType::from_raw_bytes(&[1, 2, 3, 4, 5], None),
            overflow_page: Some(PageId::from(1234)),
        }
    ],
    validate_table_leaf_cell
);

test_leaf_page_ops!(
    table_leaf,
    TableLeafPage,
    TableLeafCell,
    RowId,
    PageType::TableLeaf,
    (|id: u32| RowId::from(id)),
    (|key: RowId| TableLeafCell {
        row_id: key,
        payload: VarlenaType::from_raw_bytes(&[0u8; 10], None),
        overflow_page: None,
    })
);

test_leaf_page_ops!(
    index_leaf,
    IndexLeafPage,
    IndexLeafCell,
    VarlenaType,
    PageType::IndexLeaf,
    (|id: u32| VarlenaType::from_raw_bytes(&id.to_be_bytes(), None)),
    (|key: VarlenaType| IndexLeafCell {
        row_id: RowId::from(6),
        payload: key,
        overflow_page: None,
    })
);

test_interior_page_ops!(
    index_interior,
    IndexInteriorPage,
    IndexInteriorCell,
    VarlenaType,
    PageType::IndexInterior,
    (|id: u32| VarlenaType::from_raw_bytes(&id.to_be_bytes(), None))
);

test_interior_page_ops!(
    table_interior,
    TableInteriorPage,
    TableInteriorCell,
    RowId,
    PageType::TableInterior,
    (|id: u32| RowId::from(id))
);
