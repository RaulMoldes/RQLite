use crate::{
     UInt64,
    io::{
        logger::{Alter, Create, Delete, DropOp, Insert, Operation, Update},
        wal::AnalysisResult,
    },
    runtime::{
        RuntimeResult,
        context::{ThreadContext, TransactionLogger},
        ddl::{
            AlterTableInstr, CreateIndexInstr, CreateTableInstr, DdlExecutor, DdlInstruction,
            DropIndexInstr, DropTableInstr,
        },
        dml::DmlExecutor,
    },

    storage::tuple::Row,
};

use std::io::{Error as IoError, ErrorKind};

pub struct WalRecuperator {
    dml_executor: DmlExecutor,
    ddl_executor: DdlExecutor,
}

impl WalRecuperator {
    pub(crate) fn new(ctx: ThreadContext, logger: TransactionLogger) -> Self {
        Self {
            dml_executor: DmlExecutor::new(ctx.clone(), logger.clone()),
            ddl_executor: DdlExecutor::new(ctx, logger),
        }
    }

    /// Runs the recovery
    pub(crate) fn run_recovery(&mut self, analysis: &AnalysisResult) -> RuntimeResult<()> {
        self.run_undo(&analysis)?;
        self.run_redo(&analysis)?;

        Ok(())
    }

    /// Run all the undo.
    pub(crate) fn run_undo(&mut self, analysis: &AnalysisResult) -> RuntimeResult<()> {
        for redo_transaction in analysis.needs_undo.iter() {
            // Reapply all the operations of this transaction
            for lsn in analysis.try_iter_lsn(redo_transaction).ok_or(IoError::new(
                ErrorKind::NotSeekable,
                "transaction not found in th write ahead analysis",
            ))? {
                if let Some(delete_operation) = analysis.delete_ops.get(&lsn) {
                    self.undo_delete(delete_operation)?;
                }
                if let Some(update_operation) = analysis.update_ops.get(&lsn) {
                    self.undo_update(update_operation)?;
                }

                if let Some(insert_operation) = analysis.insert_ops.get(&lsn) {
                    self.undo_insert(insert_operation)?;
                }

                if let Some(create_operation) = analysis.create_ops.get(&lsn) {
                    self.undo_create(create_operation)?;
                }
                if let Some(alter_operation) = analysis.alter_ops.get(&lsn) {
                    self.undo_alter(alter_operation)?;
                }
                if let Some(drop_operation) = analysis.drop_ops.get(&lsn) {
                    self.undo_drop(drop_operation)?;
                }
            }
        }

        Ok(())
    }

    /// Run all the redo.
    pub(crate) fn run_redo(&mut self, analysis: &AnalysisResult) -> RuntimeResult<()> {
        for redo_transaction in analysis.needs_redo.iter() {

            // Reapply all the operations of this transaction
            for lsn in analysis.try_iter_lsn(redo_transaction).ok_or(IoError::new(
                ErrorKind::NotSeekable,
                "transaction not found in th write ahead analysis",
            ))? {


                if let Some(create_operation) = analysis.create_ops.get(&lsn) {

                    self.redo_create(create_operation)?;
                }
                if let Some(alter_operation) = analysis.alter_ops.get(&lsn) {

                    self.redo_alter(alter_operation)?;
                }
                if let Some(drop_operation) = analysis.drop_ops.get(&lsn) {

                    self.redo_drop(drop_operation)?;
                }

                if let Some(delete_operation) = analysis.delete_ops.get(&lsn) {

                    self.redo_delete(delete_operation)?;
                }

                if let Some(update_operation) = analysis.update_ops.get(&lsn) {

                    self.redo_update(update_operation)?;
                }

                if let Some(insert_operation) = analysis.insert_ops.get(&lsn) {

                    self.redo_insert(insert_operation)?;
                }
            }
        }

        Ok(())
    }

    /// Recovers a CREATE operation during redo phase.
    ///
    /// Deserializes the CreateTableInstr or CreateIndexInstr from the redo
    /// payload and executes it to recreate the table/index.
    fn redo_create(&mut self, create_op: &Create) -> RuntimeResult<()> {
        let redo_bytes = create_op.redo();

        if redo_bytes.is_empty() {
            return Ok(());
        }

        // Try to deserialize as CreateTableInstr first
        if let Ok(create_table_instr) = CreateTableInstr::from_bytes(redo_bytes) {
            let instr = DdlInstruction::CreateTable(create_table_instr);
            self.ddl_executor.execute_instruction(&instr)?;
            return Ok(());
        }

        // Try CreateIndexInstr
        if let Ok(create_index_instr) = CreateIndexInstr::from_bytes(redo_bytes) {
            let instr = DdlInstruction::CreateIndex(create_index_instr);
            self.ddl_executor.execute_instruction(&instr)?;
            return Ok(());
        }

        // Fallback: try generic DdlInstruction
        if let Ok(instruction) = DdlInstruction::from_bytes(redo_bytes) {
            self.ddl_executor.execute_instruction(&instruction)?;
        }

        Ok(())
    }

    /// Recovers a CREATE operation during undo phase.
    ///
    /// Deserializes the DropTableInstr or DropIndexInstr from the undo
    /// payload and executes it to remove the created table/index.
    fn undo_create(&mut self, create_op: &Create) -> RuntimeResult<()> {
        let object_id = create_op
            .row_id()
            .expect("Object ID must be set for CREATE logs");

        // The undo of CREATE is DROP
        // We need to get the redo bytes to know what type of object was created
        let redo_bytes = create_op.redo();

        if redo_bytes.is_empty() {
            return Ok(());
        }

        // Determine if it's a table or index and execute the inverse
        if let Ok(create_table_instr) = CreateTableInstr::from_bytes(redo_bytes) {
            let drop_instr = create_table_instr.inverse(object_id);
            let instr = DdlInstruction::DropTable(drop_instr);
            self.ddl_executor.execute_instruction(&instr)?;
            return Ok(());
        }

        if let Ok(create_index_instr) = CreateIndexInstr::from_bytes(redo_bytes) {
            let drop_instr = create_index_instr.inverse(object_id);
            let instr = DdlInstruction::DropIndex(drop_instr);
            self.ddl_executor.execute_instruction(&instr)?;
        }

        Ok(())
    }

    /// Recovers a DROP operation during redo phase.
    ///
    /// Deserializes the DropTableInstr or DropIndexInstr from the redo
    /// payload and executes it to remove the table/index.
    fn redo_drop(&mut self, drop_op: &DropOp) -> RuntimeResult<()> {
        let object_id = drop_op
            .row_id()
            .expect("Object ID must be set for DROP logs");

        // The undo bytes contain the CreateTableInstr needed to restore
        // But for redo, we just need to drop by object_id
        let undo_bytes = drop_op.undo();

        if undo_bytes.is_empty() {
            return Ok(());
        }

        // Get the table/index name from the undo instruction
        if let Ok(create_table_instr) = CreateTableInstr::from_bytes(undo_bytes) {
            let drop_instr = DropTableInstr::new(
                create_table_instr.table_name,
                object_id,
                true,
                true, // if_exists = true for recovery
            );
            let instr = DdlInstruction::DropTable(drop_instr);
            self.ddl_executor.execute_instruction(&instr)?;
            return Ok(());
        }

        if let Ok(create_index_instr) = CreateIndexInstr::from_bytes(undo_bytes) {
            let drop_instr = DropIndexInstr::new(create_index_instr.index_name, object_id, true);
            let instr = DdlInstruction::DropIndex(drop_instr);
            self.ddl_executor.execute_instruction(&instr)?;
        }

        Ok(())
    }

    /// Recovers a DROP operation during undo phase.
    ///
    /// Deserializes the CreateTableInstr or CreateIndexInstr from the undo
    /// payload and executes it to restore the dropped table/index.
    fn undo_drop(&mut self, drop_op: &DropOp) -> RuntimeResult<()> {
        let undo_bytes = drop_op.undo();

        if undo_bytes.is_empty() {
            return Ok(());
        }

        // The undo of DROP is CREATE - restore from the saved instruction
        if let Ok(create_table_instr) = CreateTableInstr::from_bytes(undo_bytes) {
            let instr = DdlInstruction::CreateTable(create_table_instr);
            self.ddl_executor.execute_instruction(&instr)?;
            return Ok(());
        }

        if let Ok(create_index_instr) = CreateIndexInstr::from_bytes(undo_bytes) {
            let instr = DdlInstruction::CreateIndex(create_index_instr);
            self.ddl_executor.execute_instruction(&instr)?;
        }

        Ok(())
    }

    /// Recovers an ALTER operation during redo phase.
    ///
    /// Deserializes the AlterTableInstr from the redo payload and applies
    /// the alteration to the table.
    fn redo_alter(&mut self, alter_op: &Alter) -> RuntimeResult<()> {
        let redo_bytes = alter_op.redo();

        if redo_bytes.is_empty() {
            return Ok(());
        }

        if let Ok(alter_instr) = AlterTableInstr::from_bytes(redo_bytes) {
            let instr = DdlInstruction::AlterTable(alter_instr);
            self.ddl_executor.execute_instruction(&instr)?;
        }

        Ok(())
    }

    /// Recovers an ALTER operation during undo phase.
    ///
    /// Deserializes the AlterTableInstr from the undo payload (which contains
    /// the inverse operation) and applies it to restore the previous state.
    fn undo_alter(&mut self, alter_op: &Alter) -> RuntimeResult<()> {
        let undo_bytes = alter_op.undo();

        if undo_bytes.is_empty() {
            return Ok(());
        }

        if let Ok(alter_instr) = AlterTableInstr::from_bytes(undo_bytes) {
            let instr = DdlInstruction::AlterTable(alter_instr);
            self.ddl_executor.execute_instruction(&instr)?;
        }

        Ok(())
    }

    // DML Undo operations
    fn undo_delete(&mut self, delete_op: &Delete) -> RuntimeResult<()> {
        let table_id = delete_op
            .object_id()
            .expect("Table id must be set for DML logs");

        let builder = self.dml_executor.ctx().tree_builder();
        let snapshot = self.dml_executor.ctx().snapshot();

        let table = self
            .dml_executor
            .ctx()
            .catalog()
            .get_relation(table_id, &builder, &snapshot)?;

        let schema = table.schema();

        if let Some(row) =
            Row::from_bytes_checked_with_snapshot(delete_op.undo(), schema, &snapshot)?
        {
            let columns = schema.column_indexes();
            self.dml_executor.insert(table_id, &columns, &row)?;
        }
        Ok(())
    }

    fn undo_update(&mut self, update_op: &Update) -> RuntimeResult<()> {
        let table_id = update_op
            .object_id()
            .expect("Table id must be set for DML logs");
        let row_id = update_op
            .row_id()
            .map(|r| UInt64::from(r))
            .expect("Row id must be set for DML logs");

        let builder = self.dml_executor.ctx().tree_builder();
        let snapshot = self.dml_executor.ctx().snapshot();

        let table = self
            .dml_executor
            .ctx()
            .catalog()
            .get_relation(table_id, &builder, &snapshot)?;

        let schema = table.schema();

        if let Some(undo_row) =
            Row::from_bytes_checked_with_snapshot(update_op.undo(), schema, &snapshot)?
        {
            if let Some(redo_row) =
                Row::from_bytes_checked_with_snapshot(update_op.redo(), schema, &snapshot)?
            {
                self.dml_executor
                    .update_row(table_id, &row_id, &redo_row, &undo_row)?;
            }
        }
        Ok(())
    }

    fn undo_insert(&mut self, insert_op: &Insert) -> RuntimeResult<()> {
        let table_id = insert_op
            .object_id()
            .expect("Table id must be set for DML logs");
        let row_id = insert_op
            .row_id()
            .map(|r| UInt64::from(r))
            .expect("Row id must be set for DML logs");

        self.dml_executor.delete(table_id, &row_id)?;
        Ok(())
    }

    // DML Redo operations

    fn redo_delete(&mut self, delete_op: &Delete) -> RuntimeResult<()> {
        let table_id = delete_op
            .object_id()
            .expect("Table id must be set for DML logs");
        let row_id = delete_op
            .row_id()
            .map(|r| UInt64::from(r))
            .expect("Row id must be set for DML logs");

        self.dml_executor.delete(table_id, &row_id)?;
        Ok(())
    }

    fn redo_update(&mut self, update_op: &Update) -> RuntimeResult<()> {
        let table_id = update_op
            .object_id()
            .expect("Table id must be set for DML logs");
        let row_id = update_op
            .row_id()
            .map(|r| UInt64::from(r))
            .expect("Row id must be set for DML logs");

        let builder = self.dml_executor.ctx().tree_builder();
        let snapshot = self.dml_executor.ctx().snapshot();

        let table = self
            .dml_executor
            .ctx()
            .catalog()
            .get_relation(table_id, &builder, &snapshot)?;

        let schema = table.schema();

        if let Some(undo_row) =
            Row::from_bytes_checked_with_snapshot(update_op.undo(), schema, &snapshot)?
        {
            if let Some(redo_row) =
                Row::from_bytes_checked_with_snapshot(update_op.redo(), schema, &snapshot)?
            {   
                // Redo: apply new state
                self.dml_executor
                    .update_row(table_id, &row_id, &undo_row, &redo_row)?;
            }
        }
        Ok(())
    }

    fn redo_insert(&mut self, insert_op: &Insert) -> RuntimeResult<()> {
        let table_id = insert_op
            .object_id()
            .expect("Table id must be set for DML logs");

        let builder = self.dml_executor.ctx().tree_builder();
        let snapshot = self.dml_executor.ctx().snapshot();

        let table = self
            .dml_executor
            .ctx()
            .catalog()
            .get_relation(table_id, &builder, &snapshot)?;

        let schema = table.schema();

        if let Some(row) =
            Row::from_bytes_checked_with_snapshot(insert_op.redo(), schema, &snapshot)?
        {
            let columns = schema.column_indexes();
            self.dml_executor.insert(table_id, &columns, &row)?;
        }
        Ok(())
    }
}
