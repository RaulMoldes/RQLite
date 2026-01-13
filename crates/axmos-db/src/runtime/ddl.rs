//! DDL (Data Definition Language) module.
//!
//! This module provides execution of DDL statements (CREATE, ALTER, DROP)
//! that modify the database schema. DDL statements bypass the query optimizer
//! and are executed directly against the catalog.
//!
//! The module also provides serializable DDL instruction types that can be
//! stored in the WAL for crash recovery.

use crate::{
    PageId,
    runtime::{
        RuntimeError, RuntimeResult,
        context::{ThreadContext, TransactionLogger},
        eval::eval_literal_expr,
    },
    schema::{
        DatabaseItem,
        base::{
            Column, ForeignKeyInfo, Relation, Schema, SchemaError, SchemaResult, TableConstraint,
        },
        catalog::CatalogError,
    },
    sql::binder::bounds::*,
    storage::{
        page::BtreePage,
        tuple::{Row, TupleBuilder},
    },
    types::{
        Blob, DataType, DataTypeKind, ObjectId, SerializationError, SerializationResult, UInt64,
    },
};

use rkyv::{
    Archive, Deserialize, Serialize, from_bytes, rancor::Error as RkyvError, to_bytes,
    util::AlignedVec,
};

use std::fmt::{Display, Formatter, Result as FmtResult};

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) enum DdlInstruction {
    CreateTable(CreateTableInstr),
    DropTable(DropTableInstr),
    CreateIndex(CreateIndexInstr),
    DropIndex(DropIndexInstr),
    AlterTable(AlterTableInstr),
}

impl DdlInstruction {
    pub fn to_bytes(&self) -> SerializationResult<Box<[u8]>> {
        let bytes = to_bytes::<RkyvError>(self)?;
        Ok(bytes.into_boxed_slice())
    }

    pub fn from_bytes(bytes: &[u8]) -> SerializationResult<Self> {
        if bytes.is_empty() {
            return Err(SerializationError::Other(
                "Cannot deserialize empty bytes".to_string(),
            ));
        }
        let mut aligned = AlignedVec::<4>::new();
        aligned.extend_from_slice(bytes);
        let instruction = from_bytes::<Self, RkyvError>(&aligned)?;
        Ok(instruction)
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct CreateTableInstr {
    pub(crate) table_name: String,
    pub(crate) columns: Vec<Column>,
    pub(crate) constraints: Vec<TableConstraint>,
    pub(crate) if_not_exists: bool,
}

impl CreateTableInstr {
    pub(crate) fn new(
        table_name: String,
        columns: Vec<Column>,
        constraints: Vec<TableConstraint>,
        if_not_exists: bool,
    ) -> Self {
        Self {
            table_name,
            columns,
            constraints,
            if_not_exists,
        }
    }

    pub fn to_bytes(&self) -> SerializationResult<Box<[u8]>> {
        let bytes = to_bytes::<RkyvError>(self)?;
        Ok(bytes.into_boxed_slice())
    }

    pub fn from_bytes(bytes: &[u8]) -> SerializationResult<Self> {
        if bytes.is_empty() {
            return Err(SerializationError::Other(
                "Cannot deserialize empty bytes".to_string(),
            ));
        }
        let mut aligned = AlignedVec::<4>::new();
        aligned.extend_from_slice(bytes);
        let instr = from_bytes::<Self, RkyvError>(&aligned)?;
        Ok(instr)
    }

    pub fn inverse(&self, object_id: ObjectId) -> DropTableInstr {
        DropTableInstr {
            table_name: self.table_name.clone(),
            table_id: object_id,
            cascade: true,
            if_exists: false,
        }
    }
}

impl From<&BoundCreateTable> for CreateTableInstr {
    fn from(stmt: &BoundCreateTable) -> Self {
        let columns: Vec<Column> = stmt.columns.iter().map(Column::from).collect();
        let constraints: Vec<TableConstraint> = stmt
            .constraints
            .iter()
            .map(TableConstraint::from)
            .collect();

        Self::new(
            stmt.table_name.clone(),
            columns,
            constraints,
            stmt.if_not_exists,
        )
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct DropTableInstr {
    pub(crate) table_name: String,
    pub(crate) table_id: ObjectId,
    pub(crate) cascade: bool,
    pub(crate) if_exists: bool,
}

impl DropTableInstr {
    pub fn new(table_name: String, table_id: ObjectId, cascade: bool, if_exists: bool) -> Self {
        Self {
            table_name,
            table_id,
            cascade,
            if_exists,
        }
    }

    pub fn to_bytes(&self) -> SerializationResult<Box<[u8]>> {
        let bytes = to_bytes::<RkyvError>(self)?;
        Ok(bytes.into_boxed_slice())
    }

    pub fn from_bytes(bytes: &[u8]) -> SerializationResult<Self> {
        if bytes.is_empty() {
            return Err(SerializationError::Other(
                "Cannot deserialize empty bytes".to_string(),
            ));
        }
        let mut aligned = AlignedVec::<4>::new();
        aligned.extend_from_slice(bytes);
        let instr = from_bytes::<Self, RkyvError>(&aligned)?;
        Ok(instr)
    }
}

impl From<&BoundDropTable> for DropTableInstr {
    fn from(stmt: &BoundDropTable) -> Self {
        Self::new(
            stmt.table_name.clone(),
            stmt.table_id.unwrap_or(0),
            stmt.cascade,
            stmt.if_exists,
        )
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct CreateIndexInstr {
    pub(crate) index_name: String,
    pub(crate) table_id: ObjectId,
    pub(crate) columns: Vec<usize>,
    pub(crate) if_not_exists: bool,
}

impl CreateIndexInstr {
    pub fn new(
        index_name: String,
        table_id: ObjectId,
        columns: Vec<usize>,
        if_not_exists: bool,
    ) -> Self {
        Self {
            index_name,
            table_id,
            columns,
            if_not_exists,
        }
    }

    pub fn to_bytes(&self) -> SerializationResult<Box<[u8]>> {
        let bytes = to_bytes::<RkyvError>(self)?;
        Ok(bytes.into_boxed_slice())
    }

    pub fn from_bytes(bytes: &[u8]) -> SerializationResult<Self> {
        if bytes.is_empty() {
            return Err(SerializationError::Other(
                "Cannot deserialize empty bytes".to_string(),
            ));
        }
        let mut aligned = AlignedVec::<4>::new();
        aligned.extend_from_slice(bytes);
        let instr = from_bytes::<Self, RkyvError>(&aligned)?;
        Ok(instr)
    }

    pub fn inverse(&self, object_id: ObjectId) -> DropIndexInstr {
        DropIndexInstr {
            index_name: self.index_name.clone(),
            index_id: object_id,
            if_exists: false,
        }
    }
}

impl From<&BoundCreateIndex> for CreateIndexInstr {
    fn from(stmt: &BoundCreateIndex) -> Self {
        Self::new(
            stmt.index_name.clone(),
            stmt.table_id,
            stmt.columns.clone(),
            stmt.if_not_exists,
        )
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct DropIndexInstr {
    pub index_name: String,
    pub index_id: ObjectId,
    pub if_exists: bool,
}

impl DropIndexInstr {
    pub fn new(index_name: String, index_id: ObjectId, if_exists: bool) -> Self {
        Self {
            index_name,
            index_id,
            if_exists,
        }
    }

    pub fn to_bytes(&self) -> SerializationResult<Box<[u8]>> {
        let bytes = to_bytes::<RkyvError>(self)?;
        Ok(bytes.into_boxed_slice())
    }

    pub fn from_bytes(bytes: &[u8]) -> SerializationResult<Self> {
        if bytes.is_empty() {
            return Err(SerializationError::Other(
                "Cannot deserialize empty bytes".to_string(),
            ));
        }
        let mut aligned = AlignedVec::<4>::new();
        aligned.extend_from_slice(bytes);
        let instr = from_bytes::<Self, RkyvError>(&aligned)?;
        Ok(instr)
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct AlterTableInstr {
    pub(crate) table_id: ObjectId,
    pub(crate) table_name: String,
    pub(crate) action: AlterActionInstr,
}

impl AlterTableInstr {
    pub(crate) fn new(table_id: ObjectId, table_name: String, action: AlterActionInstr) -> Self {
        Self {
            table_id,
            table_name,
            action,
        }
    }

    pub fn to_bytes(&self) -> SerializationResult<Box<[u8]>> {
        let bytes = to_bytes::<RkyvError>(self)?;
        Ok(bytes.into_boxed_slice())
    }

    pub fn from_bytes(bytes: &[u8]) -> SerializationResult<Self> {
        if bytes.is_empty() {
            return Err(SerializationError::Other(
                "Cannot deserialize empty bytes".to_string(),
            ));
        }
        let mut aligned = AlignedVec::<4>::new();
        aligned.extend_from_slice(bytes);
        let instr = from_bytes::<Self, RkyvError>(&aligned)?;
        Ok(instr)
    }

    pub fn try_inverse_with(&self, schema: &Schema) -> SchemaResult<Self> {
        Ok(Self {
            table_id: self.table_id,
            table_name: self.table_name.clone(),
            action: self.action.try_inverse_with(schema)?,
        })
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) enum AlterActionInstr {
    AddColumn {
        column: Column,
    },
    DropColumn {
        column_idx: usize,
    },
    AlterColumn {
        column_idx: usize,
        action: AlterColumnActionInstr,
    },
    AddConstraint(TableConstraint),
}

impl AlterActionInstr {
    pub fn try_inverse_with(&self, schema: &Schema) -> SchemaResult<Self> {
        match self {
            Self::AddColumn { column, .. } => {
                let idx = schema.bind_column(column.name())?;
                Ok(Self::DropColumn { column_idx: idx })
            }
            Self::DropColumn { column_idx, .. } => {
                let column = schema
                    .column(*column_idx)
                    .ok_or(SchemaError::ColumnIndexOutOfBounds(*column_idx))?;
                Ok(Self::AddColumn {
                    column: column.clone(),
                })
            }
            Self::AlterColumn { column_idx, action } => Ok(Self::AlterColumn {
                column_idx: *column_idx,
                action: action.inverse(),
            }),
            Self::AddConstraint(constraint) => Ok(Self::AddConstraint(constraint.clone())),
        }
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum AlterColumnActionInstr {
    SetDefault {
        old_default: Option<Box<[u8]>>,
        new_default: Option<Box<[u8]>>,
    },
    DropDefault {
        old_default: Option<Box<[u8]>>,
    },
    SetNotNull {
        was_nullable: bool,
    },
    DropNotNull {
        was_non_null: bool,
    },
    SetDataType {
        old_type: u8,
        new_type: u8,
    },
}

impl AlterColumnActionInstr {
    pub fn inverse(&self) -> Self {
        match self {
            Self::SetDefault {
                old_default,
                new_default,
            } => Self::SetDefault {
                old_default: new_default.clone(),
                new_default: old_default.clone(),
            },
            Self::DropDefault { old_default } => Self::SetDefault {
                old_default: None,
                new_default: old_default.clone(),
            },
            Self::SetNotNull { was_nullable } => {
                if *was_nullable {
                    Self::DropNotNull {
                        was_non_null: false,
                    }
                } else {
                    Self::SetNotNull {
                        was_nullable: false,
                    }
                }
            }
            Self::DropNotNull { was_non_null } => {
                if *was_non_null {
                    Self::SetNotNull {
                        was_nullable: false,
                    }
                } else {
                    Self::DropNotNull {
                        was_non_null: false,
                    }
                }
            }
            Self::SetDataType { old_type, new_type } => Self::SetDataType {
                old_type: *new_type,
                new_type: *old_type,
            },
        }
    }
}


impl From<&BoundTableConstraint> for TableConstraint {
    fn from(constraint: &BoundTableConstraint) -> Self {
        match constraint {
            BoundTableConstraint::PrimaryKey(cols) => Self::PrimaryKey(cols.clone()),
            BoundTableConstraint::Unique(cols) => Self::Unique(cols.clone()),
            BoundTableConstraint::ForeignKey {
                columns,
                ref_table_id,
                ref_columns,
            } => Self::ForeignKey {
                columns: columns.clone(),
                info: ForeignKeyInfo::for_table_and_columns(*ref_table_id, ref_columns.clone())
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum DdlResult {
    TableCreated { name: String, object_id: ObjectId },
    TableAltered { name: String, action: String },
    TableDropped { name: String },
    IndexCreated { name: String, object_id: ObjectId },
    IndexDropped { name: String },
    TransactionCommitted,
    TransactionStarted,
    TransactionRolledBack,
    NoOp,
}

impl Display for DdlResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::TableCreated { name, .. } => write!(f, "CREATE TABLE {}", name),
            Self::TableAltered { name, action } => write!(f, "ALTER TABLE {} {}", name, action),
            Self::TableDropped { name } => write!(f, "DROP TABLE {}", name),
            Self::IndexCreated { name, .. } => write!(f, "CREATE INDEX {}", name),
            Self::IndexDropped { name } => write!(f, "DROP INDEX {}", name),
            Self::TransactionCommitted => f.write_str("COMMIT"),
            Self::TransactionStarted => f.write_str("BEGIN"),
            Self::TransactionRolledBack => f.write_str("ROLLBACK"),
            Self::NoOp => f.write_str("OK"),
        }
    }
}

impl DdlResult {
    pub fn to_row(&self) -> Row {
        Row::from(vec![DataType::Blob(Blob::from(self.to_string()))])
    }
}

fn index_name(col_names: &[String], table_name: &str) -> String {
    format!("{}_{}_unique_idx", table_name, col_names.join("_"))
}

pub struct DdlExecutor {
    ctx: ThreadContext,
    logger: TransactionLogger,
}

impl DdlExecutor {
    pub(crate) fn new(ctx: ThreadContext, logger: TransactionLogger) -> Self {
        Self { ctx, logger }
    }

    pub fn is_ddl(stmt: &BoundStatement) -> bool {
        matches!(
            stmt,
            BoundStatement::CreateTable(_)
                | BoundStatement::CreateIndex(_)
                | BoundStatement::AlterTable(_)
                | BoundStatement::DropTable(_)
                | BoundStatement::Transaction(_)
        )
    }

    pub fn execute(&mut self, stmt: &BoundStatement) -> RuntimeResult<DdlResult> {
        match stmt {
            BoundStatement::CreateTable(create) => {
                let instr = CreateTableInstr::from(create);
                self.execute_create_table(&instr)
            }
            BoundStatement::CreateIndex(create) => {
                let instr = CreateIndexInstr::from(create);
                self.execute_create_index(&instr)
            }
            BoundStatement::AlterTable(alter) => {
                let instr = self.alter_table_instr_from_bound(alter)?;
                self.execute_alter_table(&instr)
            }
            BoundStatement::DropTable(drop) => {
                let instr = DropTableInstr::from(drop);
                self.execute_drop_table(&instr)
            }
            _ => Err(RuntimeError::InvalidStatement),
        }
    }

    pub(crate) fn execute_instruction(&mut self, instr: &DdlInstruction) -> RuntimeResult<DdlResult> {
        match instr {
            DdlInstruction::CreateTable(create) => self.execute_create_table(create),
            DdlInstruction::DropTable(drop) => self.execute_drop_table(drop),
            DdlInstruction::CreateIndex(create) => self.execute_create_index(create),
            DdlInstruction::DropIndex(drop) => self.execute_drop_index(drop),
            DdlInstruction::AlterTable(alter) => self.execute_alter_table(alter),
        }
    }

    fn alter_table_instr_from_bound(
        &self,
        stmt: &BoundAlterTable,
    ) -> RuntimeResult<AlterTableInstr> {
        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        let relation = self
            .ctx
            .catalog()
            .get_relation(stmt.table_id, &tree_builder, &snapshot)?;

        let table_name = relation.name().to_string();
        let action = self.alter_action_instr_from_bound(&stmt.action, relation.schema())?;

        Ok(AlterTableInstr::new(stmt.table_id, table_name, action))
    }

    fn alter_action_instr_from_bound(
        &self,
        action: &BoundAlterAction,
        schema: &Schema,
    ) -> RuntimeResult<AlterActionInstr> {
        match action {
            BoundAlterAction::AddColumn(col_def) => {
                let column = Column::from(col_def);
                Ok(AlterActionInstr::AddColumn { column })
            }
            BoundAlterAction::DropColumn(col_idx) => {
                let column = schema
                    .column(*col_idx)
                    .ok_or(SchemaError::ColumnIndexOutOfBounds(*col_idx))?
                    .clone();
                Ok(AlterActionInstr::DropColumn {
                    column_idx: *col_idx,
                })
            }
            BoundAlterAction::AlterColumn {
                column_idx,
                action_type,
            } => {
                let column = schema
                    .column(*column_idx)
                    .ok_or(SchemaError::ColumnIndexOutOfBounds(*column_idx))?;

                let action = match action_type {
                    BoundAlterColumnAction::SetDefault(expr) => {
                        let new_default =
                            eval_literal_expr(expr).and_then(|dt| dt.serialize().ok());
                        AlterColumnActionInstr::SetDefault {
                            old_default: column.default.clone(),
                            new_default,
                        }
                    }
                    BoundAlterColumnAction::DropDefault => AlterColumnActionInstr::DropDefault {
                        old_default: column.default.clone(),
                    },
                    BoundAlterColumnAction::SetNotNull => AlterColumnActionInstr::SetNotNull {
                        was_nullable: !column.is_non_null,
                    },
                    BoundAlterColumnAction::DropNotNull => AlterColumnActionInstr::DropNotNull {
                        was_non_null: column.is_non_null,
                    },
                    BoundAlterColumnAction::SetDataType(dt) => {
                        AlterColumnActionInstr::SetDataType {
                            old_type: column.dtype,
                            new_type: *dt as u8,
                        }
                    }
                };

                Ok(AlterActionInstr::AlterColumn {
                    column_idx: *column_idx,
                    action,
                })
            }
            BoundAlterAction::AddConstraint(constraint) => {
                let instr = TableConstraint::from(constraint);
                Ok(AlterActionInstr::AddConstraint(instr))
            }
        }
    }

    fn execute_create_table(&mut self, instr: &CreateTableInstr) -> RuntimeResult<DdlResult> {
        if self.relation_exists(&instr.table_name) {
            if instr.if_not_exists {
                return Ok(DdlResult::NoOp);
            }
            return Err(RuntimeError::AlreadyExists(DatabaseItem::Table(
                instr.table_name.clone(),
            )));
        }

        let mut columns: Vec<Column> = Vec::with_capacity(instr.columns.len() + 1);
        columns.push(Column::new_with_defaults(DataTypeKind::BigUInt, "row_id"));
        columns.extend(instr.columns.iter().cloned());

        let object_id = self.ctx.catalog().get_next_object_id();
        let root_page = self.ctx.pager().write().allocate_page::<BtreePage>()?;

        let relation = Relation::table(object_id, &instr.table_name, root_page, columns);

        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        self.ctx
            .catalog()
            .store_relation(relation, &tree_builder, snapshot.xid())?;

        let mut relation = self
            .ctx
            .catalog()
            .get_relation(object_id, &tree_builder, &snapshot)?;

        for constraint in &instr.constraints {
            self.add_constraint(&mut relation, constraint)?;
        }

        let redo_bytes = instr.to_bytes()?;
        let undo_instr = instr.inverse(object_id);
        let undo_bytes = undo_instr.to_bytes()?;
        self.logger.log_create(object_id, redo_bytes, undo_bytes)?;

        Ok(DdlResult::TableCreated {
            name: instr.table_name.clone(),
            object_id,
        })
    }

    fn execute_drop_table(&mut self, instr: &DropTableInstr) -> RuntimeResult<DdlResult> {
        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        let relation =
            match self
                .ctx
                .catalog()
                .get_relation(instr.table_id, &tree_builder, &snapshot)
            {
                Ok(rel) => rel,
                Err(_) => {
                    if instr.if_exists {
                        return Ok(DdlResult::NoOp);
                    }
                    return Err(RuntimeError::Schema(SchemaError::NotFound(
                        DatabaseItem::Table(instr.table_name.clone()),
                    )));
                }
            };

        let table_name = relation.name().to_string();
        let undo_instr =  relation.to_create_table_instr()?;

        let id = relation.object_id();

        self.ctx
            .catalog()
            .remove_relation(relation, &tree_builder, &snapshot, instr.cascade)?;

        let undo_bytes = undo_instr.to_bytes()?;
        let redo_instr = undo_instr.inverse(id);
        let redo_bytes = redo_instr.to_bytes()?;
        self.logger.log_drop(id, redo_bytes, undo_bytes)?;

        Ok(DdlResult::TableDropped { name: table_name })
    }

    fn execute_create_index(&mut self, instr: &CreateIndexInstr) -> RuntimeResult<DdlResult> {
        if self.relation_exists(&instr.index_name) {
            if instr.if_not_exists {
                return Ok(DdlResult::NoOp);
            }
            return Err(RuntimeError::AlreadyExists(DatabaseItem::Index(
                instr.index_name.clone(),
            )));
        }

        if instr.columns.is_empty() {
            return Err(RuntimeError::Other(
                "Index must have at least one column".to_string(),
            ));
        }

        let index_id =
            self.create_unique_index(instr.table_id, &instr.index_name, &instr.columns)?;

        let redo_bytes = instr.to_bytes()?;
        let undo_instr = instr.inverse(index_id);
        let undo_bytes = undo_instr.to_bytes()?;

        self.logger.log_create(index_id, redo_bytes, undo_bytes)?;

        Ok(DdlResult::IndexCreated {
            name: instr.index_name.clone(),
            object_id: index_id,
        })
    }

    fn execute_drop_index(&mut self, instr: &DropIndexInstr) -> RuntimeResult<DdlResult> {
        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        let relation =
            match self
                .ctx
                .catalog()
                .get_relation(instr.index_id, &tree_builder, &snapshot)
            {
                Ok(rel) => rel,
                Err(_) => {
                    if instr.if_exists {
                        return Ok(DdlResult::NoOp);
                    }
                    return Err(RuntimeError::Schema(SchemaError::NotFound(
                        DatabaseItem::Index(instr.index_name.clone()),
                    )));
                }
            };

        let index_name = relation.name().to_string();

        self.ctx
            .catalog()
            .remove_relation(relation, &tree_builder, &snapshot, false)?;

        Ok(DdlResult::IndexDropped { name: index_name })
    }

    fn execute_alter_table(&mut self, instr: &AlterTableInstr) -> RuntimeResult<DdlResult> {
        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        let mut relation =
            self.ctx
                .catalog()
                .get_relation(instr.table_id, &tree_builder, &snapshot)?;

        if !relation.is_table() {
            return Err(RuntimeError::Catalog(CatalogError::TableNotFound(
                instr.table_id,
            )));
        }

        let undo_instr = instr.try_inverse_with(relation.schema())?;
        let action_desc = self.apply_alter_action(&mut relation, &instr.action)?;

        let id = relation.object_id();
        self.ctx.catalog().update_relation(
            id,
            None,
            Some(relation.into_schema()),
            None,
            &tree_builder,
            self.ctx.snapshot(),
        )?;

        let redo_bytes = instr.to_bytes()?;
        let undo_bytes = undo_instr.to_bytes()?;

        self.logger
            .log_alter(instr.table_id, redo_bytes, undo_bytes)?;

        Ok(DdlResult::TableAltered {
            name: instr.table_name.clone(),
            action: action_desc,
        })
    }

    fn apply_alter_action(
        &mut self,
        relation: &mut Relation,
        action: &AlterActionInstr,
    ) -> RuntimeResult<String> {
        match action {
            AlterActionInstr::AddColumn { column } => {
                let name = column.name().to_string();
                relation.add_column(column.clone())?;
                Ok(format!("ADD COLUMN {}", name))
            }
            AlterActionInstr::DropColumn { column_idx } => {
                let schema = relation.schema_mut();
                if *column_idx < schema.num_keys() {
                    return Err(RuntimeError::CannotUpdateKeyColumn);
                }

                let name = schema
                    .column(*column_idx)
                    .ok_or(SchemaError::ColumnIndexOutOfBounds(*column_idx))?
                    .name()
                    .to_string();
                schema.remove_column_unchecked(*column_idx);
                Ok(format!("DROP COLUMN {}", name))
            }
            AlterActionInstr::AlterColumn { column_idx, action } => {
                self.apply_column_alter(relation, *column_idx, action)
            }
            AlterActionInstr::AddConstraint(constraint) => {
                self.add_constraint(relation, constraint)
            }
        }
    }

    fn apply_column_alter(
        &self,
        relation: &mut Relation,
        column_idx: usize,
        action: &AlterColumnActionInstr,
    ) -> RuntimeResult<String> {
        let schema = relation.schema_mut();
        let column = schema
            .columns
            .get_mut(column_idx)
            .ok_or(SchemaError::ColumnIndexOutOfBounds(column_idx))?;

        let column_name = column.name().to_string();

        match action {
            AlterColumnActionInstr::SetDefault { new_default, .. } => {
                column.default = new_default.clone();
            }
            AlterColumnActionInstr::DropDefault { .. } => {
                column.default = None;
            }
            AlterColumnActionInstr::SetNotNull { .. } => {
                column.is_non_null = true;
            }
            AlterColumnActionInstr::DropNotNull { .. } => {
                column.is_non_null = false;
            }
            AlterColumnActionInstr::SetDataType { new_type, .. } => {
                column.dtype = *new_type;
            }
        }

        Ok(format!("ALTER COLUMN {}", column_name))
    }

    fn add_constraint(
        &mut self,
        relation: &mut Relation,
        constraint: &TableConstraint,
    ) -> RuntimeResult<String> {
        match constraint {
            TableConstraint::PrimaryKey(col_indices) => {
                if relation.schema().has_primary_key() {
                    return Err(RuntimeError::Schema(SchemaError::DuplicatePrimaryKey));
                }

                {
                    let schema_mut = relation.schema_mut();
                    if let Some(constraints) = schema_mut.table_constraints.as_mut() {
                        constraints.push(TableConstraint::PrimaryKey(col_indices.clone()));
                    }

                    for index in col_indices {
                        let col = schema_mut
                            .column_mut(*index)
                            .ok_or(SchemaError::ColumnIndexOutOfBounds(*index))?;
                        col.is_non_null = true;
                    }
                }

                let col_names: Vec<String> = col_indices
                    .iter()
                    .filter_map(|&idx| relation.schema().column(idx).map(|c| c.name().to_string()))
                    .collect();

                let idx_name = index_name(&col_names, relation.name());
                self.create_unique_index(relation.object_id(), &idx_name, col_indices)?;

                Ok(format!(
                    "ADDED PRIMARY KEY CONSTRAINT ON COLUMNS: {}",
                    col_names.join(", ")
                ))
            }
            TableConstraint::Unique(col_indices) => {
                if let Some(constraints) = relation.schema_mut().table_constraints.as_mut() {
                    constraints.push(TableConstraint::Unique(col_indices.clone()));
                }

                let col_names: Vec<String> = col_indices
                    .iter()
                    .filter_map(|&idx| relation.schema().column(idx).map(|c| c.name().to_string()))
                    .collect();

                let idx_name = index_name(&col_names, relation.name());
                self.create_unique_index(relation.object_id(), &idx_name, col_indices)?;

                Ok(format!(
                    "ADDED UNIQUE CONSTRAINT ON COLUMNS: {}",
                    col_names.join(", ")
                ))
            }
            TableConstraint::ForeignKey {
                columns,
                info,
            } => {

                if let Some(constraints) = relation.schema_mut().table_constraints.as_mut() {
                    constraints.push(TableConstraint::ForeignKey {
                        columns: columns.clone(),
                        info: info.clone(),
                    });
                }

                let table_name = relation.name().to_string();

                let col_names: Vec<String> = columns
                    .iter()
                    .filter_map(|&idx| relation.schema().column(idx).map(|c| c.name().to_string()))
                    .collect();

                Ok(format!(
                    "ADDED FOREIGN KEY CONSTRAINT TO {}  ON {}",
                    table_name,
                    col_names.join(", ")
                ))
            }
        }
    }




    fn create_unique_index(
        &mut self,
        table_id: ObjectId,
        index_name: &str,
        indexed_column_ids: &[usize],
    ) -> RuntimeResult<ObjectId> {
        let num_keys = indexed_column_ids.len();
        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        let mut table_relation =
            self.ctx
                .catalog()
                .get_relation(table_id, &tree_builder, &snapshot)?;

        let table_root = table_relation.root();

        let mut index_columns: Vec<Column> = Vec::with_capacity(indexed_column_ids.len() + 1);

        {
            let table_schema = table_relation.schema();
            for idx in indexed_column_ids {
                if let Some(column) = table_schema.column(*idx) {
                    index_columns.push(Column::new_with_defaults(column.datatype(), column.name()));
                }
            }
        }

        index_columns.push(Column::new_with_defaults(DataTypeKind::BigUInt, "row_id"));

        let object_id = self.ctx.catalog().get_next_object_id();
        let root_page = self.ctx.pager().write().allocate_page::<BtreePage>()?;

        let index_relation =
            Relation::index(object_id, index_name, root_page, index_columns, num_keys);

        let index_root = index_relation.root();
        let index_schema = index_relation.schema().clone();

        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();

        self.ctx
            .catalog()
            .store_relation(index_relation, &tree_builder, snapshot.xid())?;

        {
            let table_schema = table_relation.schema_mut();

            if let Some(constraints) = table_schema.table_constraints.as_mut() {
                let already_exists = constraints.iter().any(
                    |c| matches!(c, TableConstraint::Unique(cols) if cols == indexed_column_ids),
                );
                if !already_exists {
                    constraints.push(TableConstraint::Unique(indexed_column_ids.to_vec()));
                }
            }

            if let Some(indexes) = table_schema.table_indexes.as_mut() {
                indexes.insert(object_id, indexed_column_ids.to_vec());
            }
        }

        self.ctx.catalog().update_relation(
            table_id,
            None,
            Some(table_relation.schema().clone()),
            None,
            &tree_builder,
            &snapshot,
        )?;

        self.populate_index(
            table_root,
            table_relation.schema(),
            index_root,
            &index_schema,
            indexed_column_ids,
        )?;

        Ok(object_id)
    }

    fn populate_index(
        &mut self,
        table_root: PageId,
        table_schema: &Schema,
        index_root: PageId,
        index_schema: &Schema,
        indexed_column_ids: &[usize],
    ) -> RuntimeResult<()> {
        let snapshot = self.ctx.snapshot();
        let tid = snapshot.xid();

        let mut index_entries: Vec<Row> = Vec::new();

        {
            let mut table_btree = self.ctx.build_tree(table_root);

            if table_btree.is_empty()? {
                return Ok(());
            }

            let positions: Vec<crate::tree::accessor::BtreePagePosition> = table_btree
                .iter_forward()?
                .filter(|p| p.is_ok())
                .map(|f| f.unwrap())
                .collect();

            for pos in positions {
                let row = table_btree.get_row_at(pos, table_schema, &snapshot)?;

                let Some(row) = row else { continue };

                let row_id = row.first();
                let Some(DataType::BigUInt(UInt64(value))) = row_id else {
                    return Err(RuntimeError::Other(
                        "Tables should have row id type for the first column!".to_string(),
                    ));
                };

                let mut entry_values: Vec<DataType> =
                    Vec::with_capacity(indexed_column_ids.len() + 1);
                for &col_idx in indexed_column_ids {
                    if col_idx < row.len() {
                        entry_values.push(row[col_idx].clone());
                    }
                }
                entry_values.push(DataType::BigUInt(UInt64(*value)));

                index_entries.push(Row::new(entry_values.into_boxed_slice()));
            }
        }

        let mut index_btree = self.ctx.build_tree_mut(index_root);
        for entry in index_entries {
            let tuple = TupleBuilder::from_schema(index_schema).build(&entry, tid)?;
            index_btree.insert(index_root, tuple, index_schema)?;
        }

        Ok(())
    }

    fn relation_exists(&self, name: &str) -> bool {
        let snapshot = self.ctx.snapshot();
        let tree_builder = self.ctx.tree_builder();
        self.ctx
            .catalog()
            .get_relation_by_name(name, &tree_builder, &snapshot)
            .is_ok()
    }
}
