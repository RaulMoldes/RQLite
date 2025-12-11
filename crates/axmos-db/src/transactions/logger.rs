use crate::{
    io::{
        pager::SharedPager,
        wal::{
            Abort, Begin, Commit, Delete, End, Insert, LogRecord, LogRecordType, Operation, Update,
        },
    },
    storage::tuple::OwnedTuple,
    types::{LogId, ObjectId, TransactionId},
};

use std::{cell::Cell, io};

#[derive(Clone, Debug)]
pub(crate) struct Logger {
    transaction_id: TransactionId,
    last_lsn: Cell<Option<LogId>>,
    pager: SharedPager,
}

impl Logger {
    pub(crate) fn new(transaction_id: TransactionId, pager: SharedPager) -> Self {
        Self {
            transaction_id,
            last_lsn: Cell::new(None),
            pager,
        }
    }

    pub fn build_rec<O>(&self, log_record_type: LogRecordType, operation: O) -> LogRecord
    where
        O: Operation,
    {
        let log = LogRecord::new(
            self.transaction_id,
            self.last_lsn.get(),
            operation.object_id(),
            operation.op_type(),
            operation.redo(),
            operation.undo(),
        );

        self.last_lsn.set(Some(log.lsn()));
        log
    }

    /// Write BEGIN record to WAL
    pub(crate) fn begin(&self) -> io::Result<()> {
        let operation = Begin;
        let rec = self.build_rec(LogRecordType::Begin, operation);
        self.pager.write().push_to_log(rec)
    }

    /// Write COMMIT record to WAL
    pub(crate) fn commit(&self) -> io::Result<()> {
        let operation = Commit;
        let rec = self.build_rec(LogRecordType::Commit, operation);
        self.pager.write().push_to_log(rec)
    }

    /// Write ABORT record to WAL
    pub(crate) fn rollback(&self) -> io::Result<()> {
        let operation = Abort;
        let rec = self.build_rec(LogRecordType::Abort, operation);
        self.pager.write().push_to_log(rec)
    }

    /// Write END record to WAL
    pub(crate) fn end(&self) -> io::Result<()> {
        let operation = End;
        let rec = self.build_rec(LogRecordType::End, operation);
        self.pager.write().push_to_log(rec)
    }

    pub(crate) fn log_insert(&self, table: ObjectId, data: OwnedTuple) -> io::Result<()> {
        let op = Insert::new(table, data);
        let rec = self.build_rec(LogRecordType::Insert, op);
        self.pager.write().push_to_log(rec)
    }

    pub(crate) fn log_update(
        &self,
        table: ObjectId,
        old_data: OwnedTuple,
        new_data: OwnedTuple,
    ) -> io::Result<()> {
        let op = Update::new(table, old_data, new_data);
        let rec = self.build_rec(LogRecordType::Insert, op);
        self.pager.write().push_to_log(rec)
    }

    pub(crate) fn log_delete(&self, table: ObjectId, old_data: OwnedTuple) -> io::Result<()> {
        let op = Delete::new(table, old_data);
        let rec = self.build_rec(LogRecordType::Insert, op);
        self.pager.write().push_to_log(rec)
    }
}
