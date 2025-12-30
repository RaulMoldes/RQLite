use crate::{
    CELL_ALIGNMENT,
    io::pager::SharedPager,
    schema::Schema,
    storage::{
        cell::{CELL_HEADER_SIZE, CellRef, OwnedCell, Slot},
        core::traits::BtreeOps,
        page::{BtreePage, OverflowPage},
        tuple::Tuple,
        tuple::TupleHeader,
    },
    types::PageId,
};

use std::{
    cmp::{Ordering, min},
    io, mem, usize,
};

/// The cell builder is a type of accessor that is only used to build cells.
/// As such, it can bypass the accessor and use the pager methods directly.
/// For overflow pages the concerns that exist on normal pages does not apply, since overflow pages re automatically locked once you lock the first page in the overflow chain.
pub(crate) struct CellBuilder {
    /// amount of space available in the first page to store the first cell.
    /// Must be known in advance when creating the builder
    max_cell_storage_size: usize,

    /// Minimum number of cells that any btreepage can have
    minimum_keys_per_page: usize,

    /// Shared access to the pager.
    pager: SharedPager,
}
/// Cell comparator
pub(crate) struct CellComparator<'a> {
    schema: &'a Schema,
    pager: SharedPager,
}

impl<'a> CellComparator<'a> {
    pub(crate) fn new(schema: &'a Schema, pager: SharedPager) -> Self {
        Self { schema, pager }
    }

    /// Compares a byte slice against a cell.
    pub(crate) fn compare_cell_payload(
        &self,
        target: &[u8],
        cell: CellRef<'_>,
        target_cursor: usize,
    ) -> io::Result<Ordering> {
        if cell.metadata().is_overflow() {
            let mut reassembler = Reassembler::new(self.pager.clone());
            reassembler.reassemble(cell)?;
            self.compare_keys(
                target,
                reassembler.into_boxed_slice().as_ref(),
                target_cursor,
            )
        } else {
            self.compare_keys(target, cell.effective_data(), target_cursor)
        }
    }

    /// Compares the keys of the two serialized buffers.
    ///
    /// `target` contiene solo las keys serializadas (como se obtienen de `key_bytes()`).
    /// `cell_data` contiene una tupla completa serializada.
    fn compare_keys(
        &self,
        target: &[u8],
        cell_data: &[u8],
        mut target_cursor: usize,
    ) -> io::Result<Ordering> {
        let num_values = self.schema.num_values();
        let bitmap_size = num_values.div_ceil(8);
        let tuple_header_size = TupleHeader::SIZE;
        let mut cell_cursor = tuple_header_size + bitmap_size;

        // Compare key by key
        for key_col in self.schema.iter_keys() {
            let dtype = key_col.datatype();

            let (target_ref, target_next) = dtype
                .deserialize(target, target_cursor)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

            let (cell_ref, cell_next) = dtype
                .deserialize(cell_data, cell_cursor)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

            // Compare [DataTypeRef<'_>]
            match target_ref.partial_cmp(&cell_ref) {
                Some(Ordering::Equal) => {
                    target_cursor = target_next - target_cursor + target_cursor;
                    cell_cursor = cell_next;
                }
                Some(ord) => return Ok(ord),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Cannot compare null keys",
                    ));
                }
            }
        }

        Ok(Ordering::Equal)
    }
}

/// Struct intended to reassemble cell payloads.
#[derive(Clone)]
pub(crate) struct Reassembler {
    pager: SharedPager,
    max_size: usize,
    reassembled: Vec<u8>,
}

impl Reassembler {
    pub(crate) fn new(pager: SharedPager) -> Self {
        Self {
            pager,
            max_size: usize::MAX,
            reassembled: Vec::new(),
        }
    }

    /// Allocate a vec for reassembling overflow pages into a cell.
    pub(crate) fn with_capacity(pager: SharedPager, cap: usize) -> Self {
        Self {
            pager,
            max_size: usize::MAX,
            reassembled: Vec::with_capacity(cap),
        }
    }

    /// Allocate a vec for reassembling overflow pages into a cell.
    pub(crate) fn with_capacity_and_max_size(pager: SharedPager, cap: usize) -> Self {
        Self {
            pager,
            max_size: cap,
            reassembled: Vec::with_capacity(cap),
        }
    }

    /// Reassembles the full payload from a cell and its overflow chain
    pub(crate) fn reassemble<'b>(&mut self, cell: CellRef<'b>) -> io::Result<()> {
        // Clear the reassembled vec to be able to reuse the same reasssembler accross slots.
        self.reassembled.clear();

        if cell.metadata().is_overflow() {
            // First part is in the cell (minus the overflow page pointer)
            let first_part_len = cell.len() - mem::size_of::<PageId>();
            self.reassembled
                .extend_from_slice(&cell.effective_data()[..first_part_len]);

            // Rest is in overflow pages
            let mut current_page = cell.overflow_page();
            while let Some(page_id) = current_page
                && self.max_size >= self.reassembled.len()
            {
                self.pager
                    .write()
                    .with_page::<OverflowPage, _, _>(page_id, |overflow| {
                        current_page = overflow.metadata().next;
                        self.reassembled
                            .extend_from_slice(&overflow.effective_data());
                    })?;
            }
        } else {
            self.reassembled.extend_from_slice(cell.effective_data());
        }

        Ok(())
    }

    pub(crate) fn into_boxed_slice(self) -> Box<[u8]> {
        self.reassembled.into_boxed_slice()
    }
}

pub(crate) struct CellDeallocator {
    pager: SharedPager,
}

impl CellDeallocator {
    pub(crate) fn new(pager: SharedPager) -> Self {
        Self { pager }
    }

    /// Deallocates a cell.
    /// Iterates over the full overflow chain and uses its reference to the pager in order to deallocate all of them.
    pub(crate) fn deallocate_cell(&self, cell: OwnedCell) -> io::Result<()> {
        if !cell.metadata().is_overflow() {
            return Ok(());
        }

        let mut overflow_page = cell.overflow_page();

        while let Some(page_id) = overflow_page {
            let next = self
                .pager
                .write()
                .with_page::<OverflowPage, _, _>(page_id, |overflow| overflow.metadata().next)?;

            self.pager.write().dealloc_page::<OverflowPage>(page_id)?;

            overflow_page = next;
        }

        Ok(())
    }
}

impl CellBuilder {
    pub fn new(
        max_cell_storage_size: usize,
        minimum_keys_per_page: usize,
        pager: SharedPager,
    ) -> Self {
        Self {
            max_cell_storage_size,
            minimum_keys_per_page,
            pager,
        }
    }

    #[inline]
    /// Utility to align down a number to the closets multiple of alignment
    fn align_down(value: usize, alignment: usize) -> usize {
        if value <= alignment {
            0
        } else {
            (value.saturating_sub(1) / alignment) * alignment
        }
    }

    /// Computes the total available space in the first page to store the cell.
    ///
    /// The total available space computation depends if the cell is or not an overflow cell.
    ///
    /// If the cell is going to be stored normally, the amount of space we need for the payload is just the cell aligned payload size + cell header size + slot size.
    ///
    /// However, if we need to create overflowpages, we also need to reserve 4 bytes at the end of the page in order to store the pointer to the overflow page.
    fn compute_available_space_in_first_page(&self, page_size: usize) -> usize {
        // Available size in the page for the actual cell (including header data)
        let available_cell_size = self
            .max_cell_storage_size
            .saturating_sub(mem::size_of::<Slot>());

        // Align down to CELL_ALIGNMENT since we need to account for the header.
        let mut max_payload_size_in_page: usize =
            Self::align_down(available_cell_size, CELL_ALIGNMENT as usize);

        // Account for the header and the bytes reserved for the overflow page.
        max_payload_size_in_page = max_payload_size_in_page
            .saturating_sub(mem::size_of::<PageId>())
            .saturating_sub(CELL_HEADER_SIZE);

        // Clamp to max payload size in the page.
        // [BtreePage::ideal_maz_payload_size] should be already aligned to [CELL_ALIGNMENT]
        let max_payload_size = min(
            BtreePage::ideal_max_payload_size(page_size, self.minimum_keys_per_page),
            max_payload_size_in_page,
        ); // Clamp

        max_payload_size
    }

    /// Builds a cell from an [Tuple] that implements From<Box<[u8]>> and From <&'a [u8]>>
    pub fn build_cell(&self, data: Tuple, schema: &Schema) -> io::Result<OwnedCell> {
        let page_size = self.pager.write().page_size();
        let max_payload_size: usize = self.compute_available_space_in_first_page(page_size);
        let total_size = data.total_size();

        // If the cell fits in a single page simply return the built cell.
        if total_size <= max_payload_size {
            return Ok(data.into());
        };

        // At this point we have to split the payload.
        let payload = data.effective_data();

        let mut overflow_page_number = self.pager.write().allocate_page::<OverflowPage>()?;

        let cell = OwnedCell::new_overflow(&payload[..max_payload_size], overflow_page_number);

        let mut stored_bytes = max_payload_size;

        // Note that here we cannot release the latch on an overflow page until all the chain is written. This might waste a lot of memory  as it will keep overflow pages pinned. It is a tradeoff between correctness and efficiency, and for a database, we prefer to play the correctness side of the story.
        loop {
            let overflow_bytes = min(
                OverflowPage::usable_space(page_size) as usize,
                payload[stored_bytes..].len(),
            );

            self.pager.write().with_page_mut::<OverflowPage, _, _>(
                overflow_page_number,
                |overflow_page| {
                    overflow_page.push_bytes(&payload[stored_bytes..stored_bytes + overflow_bytes]);
                    overflow_page.metadata_mut().next = None;
                    stored_bytes += overflow_bytes;
                },
            )?;

            // Overflow chain built. Stop here.
            if stored_bytes >= payload.len() {
                break;
            }

            let next_overflow_page = self.pager.write().allocate_page::<OverflowPage>()?;

            self.pager.write().with_page_mut::<OverflowPage, _, _>(
                overflow_page_number,
                |overflow_page| {
                    overflow_page.metadata_mut().next = Some(next_overflow_page);
                },
            )?;

            overflow_page_number = next_overflow_page;
        }

        Ok(cell)
    }
}

#[cfg(test)]
mod cell_comparator_tests {
    use crate::{
        DBConfig,
        io::pager::{Pager, SharedPager},
        schema::{Column, Schema},
        storage::{
            cell::OwnedCell,
            tuple::{Row, TupleBuilder},
        },
        tree::cell_ops::CellComparator,
        types::{Blob, DataType, DataTypeKind, UInt64},
    };
    use std::cmp::Ordering;
    use tempfile::tempdir;

    fn create_test_pager() -> SharedPager {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let config = DBConfig::default();
        Pager::from_config(config, &db_path).unwrap().into()
    }

    fn create_tuple_and_cell(schema: &Schema, key_value: u64, value_data: &str) -> OwnedCell {
        let row = Row::new(Box::new([
            DataType::BigUInt(UInt64(key_value)),
            DataType::Blob(Blob::from(value_data)),
        ]));

        let builder = TupleBuilder::from_schema(schema);
        let tuple = builder.build(row, 0).unwrap();
        OwnedCell::from_tuple(tuple)
    }

    #[test]
    fn test_cell_comparator_equal_keys() {
        let schema = Schema::new_table(vec![
            Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
        ]);

        let pager = create_test_pager();
        let cell = create_tuple_and_cell(&schema, 42, "test");

        let comparator = CellComparator::new(&schema, pager);

        let search_key = UInt64(42);
        let result = comparator
            .compare_cell_payload(search_key.as_ref(), cell.as_cell_ref(), 0)
            .unwrap();

        assert_eq!(result, Ordering::Equal);
    }

    #[test]
    fn test_cell_comparator_less_than() {
        let schema = Schema::new_table(vec![
            Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
        ]);

        let pager = create_test_pager();
        let cell = create_tuple_and_cell(&schema, 100, "test");

        let comparator = CellComparator::new(&schema, pager);

        // Buscar 50 < 100
        let search_key = UInt64(50);
        let result = comparator
            .compare_cell_payload(search_key.as_ref(), cell.as_cell_ref(), 0)
            .unwrap();

        assert_eq!(result, Ordering::Less);
    }

    #[test]
    fn test_cell_comparator_greater_than() {
        let schema = Schema::new_table(vec![
            Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
        ]);

        let pager = create_test_pager();
        let cell = create_tuple_and_cell(&schema, 100, "test");

        let comparator = CellComparator::new(&schema, pager);

        // Buscar 200 > 100
        let search_key = UInt64(200);
        let result = comparator
            .compare_cell_payload(search_key.as_ref(), cell.as_cell_ref(), 0)
            .unwrap();

        assert_eq!(result, Ordering::Greater);
    }

    /// Test 9: CellComparator con Blob keys
    #[test]
    fn test_cell_comparator_blob_keys() {
        let schema = Schema::new_index(
            vec![
                Column::new_with_defaults(DataTypeKind::Blob, "name"),
                Column::new_with_defaults(DataTypeKind::BigUInt, "row_id"),
            ],
            1, // 1 key column (Blob)
        );

        let pager = create_test_pager();

        // Crear celda con key = "banana"
        let row = Row::new(Box::new([
            DataType::Blob(Blob::from("banana")),
            DataType::BigUInt(UInt64(1)),
        ]));
        let builder = TupleBuilder::from_schema(&schema);
        let tuple = builder.build(row, 0).unwrap();
        let cell = OwnedCell::from_tuple(tuple);

        let comparator = CellComparator::new(&schema, pager);

        // "apple" < "banana"
        let search_key = Blob::from("apple");
        let result = comparator
            .compare_cell_payload(search_key.as_ref(), cell.as_cell_ref(), 0)
            .unwrap();
        assert_eq!(result, Ordering::Less);

        // "banana" == "banana"
        let search_key = Blob::from("banana");
        let result = comparator
            .compare_cell_payload(search_key.as_ref(), cell.as_cell_ref(), 0)
            .unwrap();
        assert_eq!(result, Ordering::Equal);

        // "cherry" > "banana"
        let search_key = Blob::from("cherry");
        let result = comparator
            .compare_cell_payload(search_key.as_ref(), cell.as_cell_ref(), 0)
            .unwrap();
        assert_eq!(result, Ordering::Greater);
    }

    /// Test 10: Verificar que la comparación funciona con el offset correcto
    #[test]
    fn test_cell_comparator_uses_correct_offset() {
        let schema = Schema::new_table(vec![
            Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
            Column::new_with_defaults(DataTypeKind::BigUInt, "value1"),
            Column::new_with_defaults(DataTypeKind::BigUInt, "value2"),
        ]);

        let pager = create_test_pager();

        // Crear celda con key=42, values=100,200
        let row = Row::new(Box::new([
            DataType::BigUInt(UInt64(42)),
            DataType::BigUInt(UInt64(100)),
            DataType::BigUInt(UInt64(200)),
        ]));
        let builder = TupleBuilder::from_schema(&schema);
        let tuple = builder.build(row, 0).unwrap();
        let cell = OwnedCell::from_tuple(tuple);

        let comparator = CellComparator::new(&schema, pager);

        // La comparación debe usar solo la key (42), no los values
        let search_key = UInt64(42);
        let result = comparator
            .compare_cell_payload(search_key.as_ref(), cell.as_cell_ref(), 0)
            .unwrap();

        assert_eq!(result, Ordering::Equal);

        // Verificar que 41 < 42
        let search_key = UInt64(41);
        let result = comparator
            .compare_cell_payload(search_key.as_ref(), cell.as_cell_ref(), 0)
            .unwrap();
        assert_eq!(result, Ordering::Less);
    }

    /// Test 11: Ordenamiento de múltiples celdas
    #[test]
    fn test_cell_comparator_ordering_multiple_cells() {
        let schema = Schema::new_table(vec![
            Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "data"),
        ]);

        let pager = create_test_pager();

        let keys = [10u64, 20, 30, 40, 50];
        let cells: Vec<OwnedCell> = keys
            .iter()
            .map(|&k| create_tuple_and_cell(&schema, k, "data"))
            .collect();

        let comparator = CellComparator::new(&schema, pager);

        // Buscar 25 (entre 20 y 30)
        let search_key = UInt64(25);

        for (i, cell) in cells.iter().enumerate() {
            let result = comparator
                .compare_cell_payload(search_key.as_ref(), cell.as_cell_ref(), 0)
                .unwrap();

            let cell_key = keys[i];
            match cell_key.cmp(&25) {
                std::cmp::Ordering::Less => {
                    assert_eq!(result, Ordering::Greater, "25 > {}", cell_key);
                }
                std::cmp::Ordering::Greater => {
                    assert_eq!(result, Ordering::Less, "25 < {}", cell_key);
                }
                std::cmp::Ordering::Equal => {
                    assert_eq!(result, Ordering::Equal);
                }
            }
        }
    }
}

#[cfg(test)]
mod reassembler_tests {
    use crate::{
        DBConfig,
        io::pager::{Pager, SharedPager},
        schema::{Column, Schema},
        storage::{
            cell::OwnedCell,
            tuple::{Row, TupleBuilder, TupleReader, TupleRef},
        },
        tree::cell_ops::{CellComparator, Reassembler},
        types::{Blob, DataType, DataTypeKind, DataTypeRef, UInt64},
    };
    use std::cmp::Ordering;
    use tempfile::tempdir;

    fn create_test_pager() -> SharedPager {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let config = DBConfig::default();
        Pager::from_config(config, &db_path).unwrap().into()
    }

    #[test]
    fn test_reassembler_normal_cell() {
        let schema = Schema::new_table(vec![
            Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
        ]);

        let pager = create_test_pager();

        let row = Row::new(Box::new([
            DataType::BigUInt(UInt64(42)),
            DataType::Blob(Blob::from("test")),
        ]));

        let builder = TupleBuilder::from_schema(&schema);
        let tuple = builder.build(row, 0).unwrap();
        let cell = OwnedCell::from_tuple(tuple);

        // Reassemble
        let mut reassembler = Reassembler::new(pager.clone());
        reassembler.reassemble(cell.as_cell_ref()).unwrap();

        let reassembled_data = reassembler.into_boxed_slice();

        // Verificar que podemos parsear los datos reassembled
        let reader = TupleReader::from_schema(&schema);
        let layout = reader.parse_last_version(&reassembled_data).unwrap();
        let tuple_ref = TupleRef::new(&reassembled_data, layout);

        let key = tuple_ref.key_with_schema(0, &schema).unwrap();
        match key {
            DataTypeRef::BigUInt(v) => assert_eq!(v.value(), 42),
            _ => panic!("Expected BigUInt"),
        }
    }

    #[test]
    fn test_comparison_after_reassembly() {
        let schema = Schema::new_table(vec![
            Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
        ]);

        let pager = create_test_pager();

        let row = Row::new(Box::new([
            DataType::BigUInt(UInt64(100)),
            DataType::Blob(Blob::from("test data")),
        ]));

        let builder = TupleBuilder::from_schema(&schema);
        let tuple = builder.build(row, 0).unwrap();
        let cell = OwnedCell::from_tuple(tuple);

        // Reassemble
        let mut reassembler = Reassembler::new(pager.clone());
        reassembler.reassemble(cell.as_cell_ref()).unwrap();
        let reassembled_data = reassembler.into_boxed_slice();

        let comparator = CellComparator::new(&schema, pager);

        let search_key = UInt64(100);
        let result = comparator
            .compare_keys(search_key.as_ref(), &reassembled_data, 0)
            .unwrap();
        assert_eq!(result, Ordering::Equal);

        let search_key = UInt64(50);
        let result = comparator
            .compare_keys(search_key.as_ref(), &reassembled_data, 0)
            .unwrap();
        assert_eq!(result, Ordering::Less);

        let search_key = UInt64(150);
        let result = comparator
            .compare_keys(search_key.as_ref(), &reassembled_data, 0)
            .unwrap();
        assert_eq!(result, Ordering::Greater);
    }

    #[test]
    fn test_reassembly_preserves_full_structure() {
        let schema = Schema::new_table(vec![
            Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
            Column::new_with_defaults(DataTypeKind::Blob, "name"),
            Column::new_with_defaults(DataTypeKind::BigUInt, "age"),
        ]);

        let pager = create_test_pager();

        let row = Row::new(Box::new([
            DataType::BigUInt(UInt64(1)),
            DataType::Blob(Blob::from("Alice")),
            DataType::BigUInt(UInt64(30)),
        ]));

        let builder = TupleBuilder::from_schema(&schema);
        let tuple = builder.build(row, 5).unwrap();

        // Guardar datos originales
        let original_data = tuple.effective_data().to_vec();

        let cell = OwnedCell::from_tuple(tuple);

        // Reassemble
        let mut reassembler = Reassembler::new(pager);
        reassembler.reassemble(cell.as_cell_ref()).unwrap();
        let reassembled_data = reassembler.into_boxed_slice();

        // Los datos deberían ser idénticos
        assert_eq!(
            original_data.as_slice(),
            reassembled_data.as_ref(),
            "Reassembled data should match original"
        );

        // Verificar que podemos leer todos los campos
        let reader = TupleReader::from_schema(&schema);
        let layout = reader.parse_last_version(&reassembled_data).unwrap();
        let tuple_ref = TupleRef::new(&reassembled_data, layout);

        // Key
        match tuple_ref.key_with_schema(0, &schema).unwrap() {
            DataTypeRef::BigUInt(v) => assert_eq!(v.value(), 1),
            _ => panic!("Expected BigUInt for key"),
        }

        // Values
        match tuple_ref.value_with_schema(0, &schema).unwrap() {
            DataTypeRef::Blob(v) => assert_eq!(v.data().unwrap(), b"Alice"),
            _ => panic!("Expected Blob for name"),
        }

        match tuple_ref.value_with_schema(1, &schema).unwrap() {
            DataTypeRef::BigUInt(v) => assert_eq!(v.value(), 30),
            _ => panic!("Expected BigUInt for age"),
        }
    }
}
