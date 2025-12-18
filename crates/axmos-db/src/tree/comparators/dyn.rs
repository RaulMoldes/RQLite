#[derive(Debug, Clone)]
pub(crate) enum DynComparator {
    Variable(VarlenComparator),
    StrictNumeric(NumericComparator),
    FixedSizeBytes(FixedSizeBytesComparator),
    SignedNumeric(SignedNumericComparator),
    Composite(CompositeComparator),
}

impl Comparator for DynComparator {
    fn compare(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Ordering> {
        match self {
            Self::FixedSizeBytes(c) => c.compare(lhs, rhs),
            Self::Variable(c) => c.compare(lhs, rhs),
            Self::StrictNumeric(c) => c.compare(lhs, rhs),
            Self::SignedNumeric(c) => c.compare(lhs, rhs),
            Self::Composite(c) => c.compare(lhs, rhs),
        }
    }

    fn is_fixed_size(&self) -> bool {
        match self {
            Self::FixedSizeBytes(c) => c.is_fixed_size(),
            Self::Variable(c) => c.is_fixed_size(),
            Self::StrictNumeric(c) => c.is_fixed_size(),
            Self::SignedNumeric(c) => c.is_fixed_size(),
            Self::Composite(c) => c.is_fixed_size(),
        }
    }

    fn key_size(&self, data: &[u8]) -> io::Result<usize> {
        match self {
            Self::FixedSizeBytes(c) => c.key_size(data),
            Self::Variable(c) => c.key_size(data),
            Self::StrictNumeric(c) => c.key_size(data),
            Self::SignedNumeric(c) => c.key_size(data),
            Self::Composite(c) => c.key_size(data),
        }
    }
}

impl Ranger for DynComparator {
    fn range_bytes(&self, lhs: &[u8], rhs: &[u8]) -> io::Result<Box<[u8]>> {
        match self {
            Self::FixedSizeBytes(c) => c.range_bytes(lhs, rhs),
            Self::Variable(c) => c.range_bytes(lhs, rhs),
            Self::StrictNumeric(c) => c.range_bytes(lhs, rhs),
            Self::SignedNumeric(c) => c.range_bytes(lhs, rhs),
            Self::Composite(c) => c.range_bytes(lhs, rhs),
        }
    }
}
