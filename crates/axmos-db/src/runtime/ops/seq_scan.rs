use crate::{
    runtime::{
        ExecutionStats, Executor, RuntimeError, RuntimeResult, context::ThreadContext,
        eval::ExpressionEvaluator,
    },
    schema::Schema,
    sql::{binder::bounds::BoundExpression, planner::physical::SeqScanOp},
    storage::tuple::Row,
    tree::bplustree::BtreePositionalIterator,
    types::PageId,
};

pub(crate) struct SeqScan {
    ctx: ThreadContext,
    output_schema: Schema,
    table_schema: Schema,
    table_root: PageId,
    cursor: Option<BtreePositionalIterator>,
    predicate: Option<BoundExpression>,
    stats: ExecutionStats,
}

impl SeqScan {
    pub(crate) fn new(op: &SeqScanOp, ctx: ThreadContext) -> RuntimeResult<Self> {
        let tree_builder = ctx.tree_builder();
        let snapshot = ctx.snapshot();
        let table = ctx
            .catalog()
            .get_relation(op.table_id, &tree_builder, &snapshot)?;
        let table_root = table.root();
        let table_schema = table.schema().clone();

        Ok(Self {
            ctx,
            output_schema: op.output_schema.clone(),
            table_schema,
            table_root,
            cursor: None,
            predicate: op.predicate.clone(),
            stats: ExecutionStats::default(),
        })
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl SeqScan {
    fn evaluate_predicate(&self, row: &Row) -> RuntimeResult<bool> {
        match &self.predicate {
            None => Ok(true),
            Some(pred) => {
                let evaluator = ExpressionEvaluator::new(row, &self.table_schema);
                let bool = evaluator.evaluate_as_bool(pred)?;
                Ok(bool)
            }
        }
    }
}

impl Executor for SeqScan {
    fn open(&mut self) -> RuntimeResult<()> {
        let mut table = self.ctx.build_tree(self.table_root);
        self.cursor = if table.is_empty()? {
            None
        } else {
            Some(table.iter_forward()?)
        };

        Ok(())
    }
    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        // If building the cursor failed, it means the table was empty
        if self.cursor.is_none() {
            return Ok(None);
        };

        loop {
            let next_pos = match self
                .cursor
                .as_mut()
                .ok_or(RuntimeError::CursorUninitialized)?
                .next()
            {
                Some(res) => res?,
                None => {
                    return Ok(None);
                }
            };

            self.stats.rows_scanned += 1;
            let snapshot = self.ctx.snapshot();

            let mut tree = self
                .cursor
                .as_ref()
                .ok_or(RuntimeError::CursorUninitialized)?
                .get_tree();

            let maybe_row = tree
                .get_row_at(next_pos, &self.output_schema, &snapshot)?
                .filter(|r| {
                    self.evaluate_predicate(r)
                        .expect("Predicate evaluation failed")
                });

            if maybe_row.is_none() {
                continue;
            }

            self.stats.rows_produced += 1;
            return Ok(Some(maybe_row.unwrap()));
        }
    }

    fn close(&mut self) -> RuntimeResult<()> {
        let _ = self.cursor.take();
        Ok(())
    }
}
