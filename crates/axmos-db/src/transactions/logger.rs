use crate::{
    io::wal::{
        Abort, Begin, Commit, Delete, End, Insert, LogRecord, LogRecordType, Operation, Update,
    },
    storage::tuple::OwnedTuple,
    types::{LogId, ObjectId, TransactionId},
};

use super::accessor::RcPageAccessor;
use std::io;

pub(crate) struct Logger {
    transaction_id: TransactionId,
    last_lsn: Option<LogId>,
    accessor: RcPageAccessor,
}

impl Logger {
    pub(crate) fn new(transaction_id: TransactionId, accessor: RcPageAccessor) -> Self {
        Self {
            transaction_id,
            last_lsn: None,
            accessor,
        }
    }

    pub fn build_rec<O>(&mut self, log_record_type: LogRecordType, operation: O) -> LogRecord
    where
        O: Operation,
    {
        let log = LogRecord::new(
            self.transaction_id,
            self.last_lsn,
            operation.object_id(),
            operation.op_type(),
            operation.redo(),
            operation.undo(),
        );

        self.last_lsn = Some(log.lsn());
        log
    }

    /// Write BEGIN record to WAL
    pub(crate) fn begin(&mut self) -> io::Result<()> {
        let operation = Begin;
        let rec = self.build_rec(LogRecordType::Begin, operation);
        self.accessor.borrow().push_to_log(rec)
    }

    /// Write COMMIT record to WAL
    pub(crate) fn commit(&mut self) -> io::Result<()> {
        let operation = Commit;
        let rec = self.build_rec(LogRecordType::Commit, operation);
        self.accessor.borrow().push_to_log(rec)
    }

    /// Write ABORT record to WAL
    pub(crate) fn rollback(&mut self) -> io::Result<()> {
        let operation = Abort;
        let rec = self.build_rec(LogRecordType::Abort, operation);
        self.accessor.borrow().push_to_log(rec)
    }

    /// Write END record to WAL
    pub(crate) fn end(&mut self) -> io::Result<()> {
        let operation = End;
        let rec = self.build_rec(LogRecordType::End, operation);
        self.accessor.borrow().push_to_log(rec)
    }

    pub(crate) fn log_insert(&mut self, table: ObjectId, data: OwnedTuple) -> io::Result<()> {
        let op = Insert::new(table, data);
        let rec = self.build_rec(LogRecordType::Insert, op);
        self.accessor.borrow().push_to_log(rec)
    }

    pub(crate) fn log_update(
        &mut self,
        table: ObjectId,
        old_data: OwnedTuple,
        new_data: OwnedTuple,
    ) -> io::Result<()> {
        let op = Update::new(table, old_data, new_data);
        let rec = self.build_rec(LogRecordType::Insert, op);
        self.accessor.borrow().push_to_log(rec)
    }

    pub(crate) fn log_delete(&mut self, table: ObjectId, old_data: OwnedTuple) -> io::Result<()> {
        let op = Delete::new(table, old_data);
        let rec = self.build_rec(LogRecordType::Insert, op);
        self.accessor.borrow().push_to_log(rec)
    }
}
