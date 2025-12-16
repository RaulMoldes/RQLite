/// Comparator for composite keys with multiple typed columns.
/// Chains individual comparators and compares column by column.
#[derive(Debug, Clone)]
pub(crate) struct CompositeComparator {
    /// Comparators for each key column, in order
    comparators: Vec<DynComparator>,
}

impl CompositeComparator {
    pub(crate) fn new(comparators: Vec<DynComparator>) -> Self {
        Self { comparators }
    }

    /// Creates a composite comparator from a schema's key columns.
    pub(crate) fn from_schema(schema: &crate::database::schema::Schema) -> Self {
        let comparators: Vec<DynComparator> =
            schema.iter_keys().map(|col| col.comparator()).collect();
        Self { comparators }
    }

    /// Returns the number of key columns.
    pub(crate) fn num_keys(&self) -> usize {
        self.comparators.len()
    }
}

impl Comparator for CompositeComparator {
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Ordering> {
        let mut lhs_offset = 0usize;
        let mut rhs_offset = 0usize;

        for comparator in &self.comparators {
            let lhs_key = &lhs[lhs_offset..];
            let rhs_key = &rhs[rhs_offset..];

            match comparator.compare(lhs_key, rhs_key)? {
                Ordering::Equal => {
                    // Move to next column
                    lhs_offset += comparator.key_size(lhs_key)?;
                    rhs_offset += comparator.key_size(rhs_key)?;
                }
                other => return Ok(other),
            }
        }

        Ok(Ordering::Equal)
    }

    fn key_size(&self, data: &[u8]) -> io::Result<usize> {
        let mut offset = 0usize;

        for comparator in &self.comparators {
            let key_data = &data[offset..];
            offset += comparator.key_size(key_data)?;
        }

        Ok(offset)
    }

    fn is_fixed_size(&self) -> bool {
        self.comparators.iter().all(|c| c.is_fixed_size())
    }
}

impl Ranger for CompositeComparator {
    fn range_bytes(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Box<[u8]>> {
        // For composite keys, we compute the range based on the first differing column
        // This is useful for range scans where we want to estimate distance
        let mut lhs_offset = 0usize;
        let mut rhs_offset = 0usize;

        for comparator in &self.comparators {
            let lhs_key = &lhs[lhs_offset..];
            let rhs_key = &rhs[rhs_offset..];

            let lhs_size = comparator.key_size(lhs_key)?;
            let rhs_size = comparator.key_size(rhs_key)?;

            match comparator.compare(lhs_key, rhs_key)? {
                Ordering::Equal => {
                    lhs_offset += lhs_size;
                    rhs_offset += rhs_size;
                }
                _ => {
                    // Return range for the first differing column
                    return comparator.range_bytes(lhs_key, rhs_key);
                }
            }
        }

        // All columns equal
        Ok(Box::from([]))
    }
}
