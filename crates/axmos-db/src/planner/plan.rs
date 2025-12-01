use crate::{
    UInt64,
    database::{SharedCatalog, schema::Table},
    io::pager::SharedPager,
    transactions::TransactionCoordinator,
};

use std::fmt::{self, Display, Formatter};

pub trait NodePlan: Display {}
pub struct SequentialScan {
    table: Table,
    start: UInt64,
    end: UInt64,
}

impl Display for SequentialScan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SeqScan on [{}] from: {} to {}",
            self.table.name(),
            self.start,
            self.end
        )
    }
}
impl NodePlan for SequentialScan {}

impl SequentialScan {
    pub fn new(table: Table, start: UInt64, end: UInt64) -> Self {
        Self { table, start, end }
    }
}
