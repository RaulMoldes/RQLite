use crate::{
   UInt64,
    io::{

        logger::{Create, Operation, Alter, DropOp, Insert, Update, Delete},
        wal::AnalysisResult,
    },

    runtime::{
        RuntimeResult, context::{ThreadContext, TransactionLogger},
        dml::DmlExecutor
    },
    storage::{

        tuple::Row,
    },
    schema::{
        meta_table_schema,
        base::Relation
    }
};

use std::{
    io::{Error as IoError, ErrorKind},

};

pub struct WalRecuperator {
    dml_executor: DmlExecutor,

}

impl WalRecuperator {
    pub(crate) fn new(ctx: ThreadContext, logger: TransactionLogger) -> Self {
        Self {
            dml_executor: DmlExecutor::new(ctx.clone(), logger.clone()),

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
                 if let Some(delete_operation) = analysis.delete_ops.get(&lsn) {
                    self.redo_delete(delete_operation)?;
                }

                if let Some(update_operation) = analysis.update_ops.get(&lsn) {
                    self.redo_update(update_operation)?;
                }

                if let Some(insert_operation) = analysis.insert_ops.get(&lsn) {
                    self.redo_insert(insert_operation)?;
                }


               if let Some(create_operation) = analysis.create_ops.get(&lsn) {
                    self.redo_create(create_operation)?;

                }
                if let Some(alter_operation) = analysis.alter_ops.get(&lsn) {
                    self.redo_alter(alter_operation)?;

                }
                if let Some(drop_operation) = analysis.drop_ops.get(&lsn) {
                    self.redo_drop(drop_operation)?;
                }
            }
        }

        Ok(())
    }

    /// Recovers a CREATE operation during redo phase.
    /// Parses the redo payload as a meta_table row and stores the relation in the catalog.
    fn redo_create(&mut self, create_op: &Create) -> RuntimeResult<()> {
        let object_id = create_op.row_id().expect("Object ID must be set for CREATE logs");
        let redo_bytes = create_op.redo();

        if redo_bytes.is_empty() {
            return Ok(());
        }

        let builder = self.dml_executor.ctx().tree_builder();
        let snapshot = self.dml_executor.ctx().snapshot();
        let schema = meta_table_schema();

        // Parse the redo payload as a Row from the meta_table schema
        if let Some(row) = Row::from_bytes_checked_with_snapshot(redo_bytes, &schema, &snapshot)? {
            let relation = Relation::from_meta_table_row(row);
            self.dml_executor
                .ctx()
                .catalog()
                .store_relation(relation, &builder, snapshot.xid(), Some(&self.dml_executor.logger()))?;
        }

        Ok(())
    }

    /// Recovers a CREATE operation during undo phase.
    /// Removes the relation from the catalog.
    fn undo_create(&mut self, create_op: &Create) -> RuntimeResult<()> {
        let object_id = create_op.row_id().expect("Object ID must be set for CREATE logs");

        let builder = self.dml_executor.ctx().tree_builder();
        let snapshot = self.dml_executor.ctx().snapshot();

        // Try to get and remove the relation (ignore if not found)
        if let Ok(relation) = self.dml_executor
            .ctx()
            .catalog()
            .get_relation(object_id, &builder, &snapshot)
        {
            self.dml_executor
                .ctx()
                .catalog()
                .remove_relation(relation, &builder, &snapshot, false,Some(&self.dml_executor.logger()))?;
        }

        Ok(())
    }

    /// Recovers a DROP operation during redo phase.
    /// Removes the relation from the catalog.
    fn redo_drop(&mut self, drop_op: &DropOp) -> RuntimeResult<()> {
        let object_id = drop_op.row_id().expect("Object ID must be set for DROP logs");

        let builder = self.dml_executor.ctx().tree_builder();
        let snapshot = self.dml_executor.ctx().snapshot();

        // Try to get and remove the relation (ignore if not found)
        if let Ok(relation) = self.dml_executor
            .ctx()
            .catalog()
            .get_relation(object_id, &builder, &snapshot)
        {
            self.dml_executor
                .ctx()
                .catalog()
                .remove_relation(relation, &builder, &snapshot, false, Some(&self.dml_executor.logger()))?;
        }

        Ok(())
    }

    /// Recovers a DROP operation during undo phase.
    /// Restores the relation to the catalog from the undo payload.
    fn undo_drop(&mut self, drop_op: &DropOp) -> RuntimeResult<()> {
        let object_id = drop_op.row_id().expect("Object ID must be set for DROP logs");
        let undo_bytes = drop_op.undo();

        if undo_bytes.is_empty() {
            return Ok(());
        }

        let builder = self.dml_executor.ctx().tree_builder();
        let snapshot = self.dml_executor.ctx().snapshot();
        let schema = meta_table_schema();

        // Parse the undo payload as a Row and restore the relation
        if let Some(row) = Row::from_bytes_checked_with_snapshot(undo_bytes, &schema, &snapshot)? {
            let relation = Relation::from_meta_table_row(row);
            self.dml_executor
                .ctx()
                .catalog()
                .store_relation(relation, &builder, snapshot.xid(),Some(&self.dml_executor.logger()))?;
        }

        Ok(())
    }

    /// Recovers an ALTER operation during redo phase.
    /// Updates the relation with the new schema from the redo payload.
    fn redo_alter(&mut self, alter_op: &Alter) -> RuntimeResult<()> {
        let object_id = alter_op.row_id().expect("Object ID must be set for ALTER logs");
        let redo_bytes = alter_op.redo();

        if redo_bytes.is_empty() {
            return Ok(());
        }

        let builder = self.dml_executor.ctx().tree_builder();
        let snapshot = self.dml_executor.ctx().snapshot();
        let meta_schema = meta_table_schema();

        // Parse the redo payload as the new relation state
        if let Some(row) = Row::from_bytes_checked_with_snapshot(redo_bytes, &meta_schema, &snapshot)? {
            let new_relation = Relation::from_meta_table_row(row);

            // Remove old and store new
            if let Ok(old_relation) = self.dml_executor
                .ctx()
                .catalog()
                .get_relation(object_id, &builder, &snapshot)
            {
                let _ = self.dml_executor
                    .ctx()
                    .catalog()
                    .remove_relation(old_relation, &builder, &snapshot, false,Some(&self.dml_executor.logger()));
            }

            self.dml_executor
                .ctx()
                .catalog()
                .store_relation(new_relation, &builder, snapshot.xid(), Some(&self.dml_executor.logger()))?;
        }

        Ok(())
    }

    /// Recovers an ALTER operation during undo phase.
    /// Restores the relation to its previous state from the undo payload.
    fn undo_alter(&mut self, alter_op: &Alter) -> RuntimeResult<()> {
        let object_id = alter_op.row_id().expect("Object ID must be set for ALTER logs");
        let undo_bytes = alter_op.undo();

        if undo_bytes.is_empty() {
            return Ok(());
        }

        let builder = self.dml_executor.ctx().tree_builder();
        let snapshot = self.dml_executor.ctx().snapshot();
        let meta_schema = meta_table_schema();

        // Parse the undo payload as the old relation state
        if let Some(row) = Row::from_bytes_checked_with_snapshot(undo_bytes, &meta_schema, &snapshot)? {
            let old_relation = Relation::from_meta_table_row(row);

            // Remove current and restore old
            if let Ok(current_relation) = self.dml_executor
                .ctx()
                .catalog()
                .get_relation(object_id, &builder, &snapshot)
            {
                let _ = self.dml_executor
                    .ctx()
                    .catalog()
                    .remove_relation(current_relation, &builder, &snapshot, false, Some(&self.dml_executor.logger()));
            }

            self.dml_executor
                .ctx()
                .catalog()
                .store_relation(old_relation, &builder, snapshot.xid(), Some(&self.dml_executor.logger()))?;
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
                // Undo: swap redo->undo (restore old state)
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
