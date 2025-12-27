use crate::{
    RowId, TransactionId,
    io::disk::{DBFile, FileOperations, FileSystem, FileSystemBlockSize},
    multithreading::coordinator::TransactionState,
    storage::{
        Allocatable, AvailableSpace, WalMetadata, WalOps, Writable,
        wal::{BlockZero, OwnedRecord, RecordRef, RecordType, WAL_BLOCK_SIZE, WalBlock},
    },
    types::{BlockId, Lsn, ObjectId},
};

pub trait Operation {
    fn op_type(&self) -> RecordType;
    fn object_id(&self) -> Option<ObjectId> {
        None
    }
    fn row_id(&self) -> Option<RowId> {
        None
    }
    fn undo(&self) -> &[u8] {
        &[]
    }
    fn redo(&self) -> &[u8] {
        &[]
    }
}

pub struct Begin;

impl Operation for Begin {
    fn op_type(&self) -> RecordType {
        RecordType::Begin
    }
}

pub struct End;

impl Operation for End {
    fn op_type(&self) -> RecordType {
        RecordType::End
    }
}

pub struct Commit;

impl Operation for Commit {
    fn op_type(&self) -> RecordType {
        RecordType::Commit
    }
}

pub struct Abort;

impl Operation for Abort {
    fn op_type(&self) -> RecordType {
        RecordType::Abort
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct Update {
    oid: ObjectId,
    rowid: RowId,
    old: Box<[u8]>,
    new: Box<[u8]>,
}

impl Update {
    pub(crate) fn new(oid: ObjectId, rowid: RowId, old: Box<[u8]>, new: Box<[u8]>) -> Self {
        Self {
            oid,
            rowid,
            old,
            new,
        }
    }
}

impl Operation for Update {
    fn op_type(&self) -> RecordType {
        RecordType::Update
    }
    fn object_id(&self) -> Option<ObjectId> {
        Some(self.oid)
    }
    fn row_id(&self) -> Option<RowId> {
        Some(self.rowid)
    }
    fn redo(&self) -> &[u8] {
        self.new.as_ref()
    }
    fn undo(&self) -> &[u8] {
        self.old.as_ref()
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct Insert {
    oid: ObjectId,
    rowid: RowId,
    new: Box<[u8]>,
}

impl Insert {
    pub(crate) fn new(oid: ObjectId, rowid: RowId, new: Box<[u8]>) -> Self {
        Self { oid, rowid, new }
    }
}

impl Operation for Insert {
    fn op_type(&self) -> RecordType {
        RecordType::Insert
    }
    fn object_id(&self) -> Option<ObjectId> {
        Some(self.oid)
    }
    fn row_id(&self) -> Option<RowId> {
        Some(self.rowid)
    }
    fn redo(&self) -> &[u8] {
        self.new.as_ref()
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct Delete {
    oid: ObjectId,
    rowid: RowId,
    old: Box<[u8]>,
}

impl Delete {
    pub(crate) fn new(oid: ObjectId, rowid: RowId, old: Box<[u8]>) -> Self {
        Self { oid, rowid, old }
    }
}

impl Operation for Delete {
    fn op_type(&self) -> RecordType {
        RecordType::Delete
    }
    fn object_id(&self) -> Option<ObjectId> {
        Some(self.oid)
    }
    fn row_id(&self) -> Option<RowId> {
        Some(self.rowid)
    }
    fn undo(&self) -> &[u8] {
        self.old.as_ref()
    }
}
