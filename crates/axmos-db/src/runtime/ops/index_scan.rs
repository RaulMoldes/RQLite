use crate::{
    TypeSystemError, UInt64,
    core::SerializableType,
    runtime::{
        ExecutionStats, Executor, RuntimeError, RuntimeResult, context::TransactionContext,
        eval::ExpressionEvaluator,
    },
    schema::{Schema, catalog::CatalogTrait},
    sql::{binder::bounds::BoundExpression, planner::physical::PhysIndexScanOp},
    storage::tuple::Row,
    tree::bplustree::{BtreePositionalIterator, SearchResult},
    types::PageId,
};

pub(crate) struct IndexScan {
    ctx: TransactionContext,
    table_root: PageId,
    index_root: PageId,
    cursor: Option<BtreePositionalIterator>,
    index_schema: Schema,
    table_schema: Schema,
    range_start: Option<BoundExpression>,
    range_end: Option<BoundExpression>,
    indexed_columns: Vec<usize>,
    stats: ExecutionStats,
}

impl IndexScan {
    pub(crate) fn new(op: &PhysIndexScanOp, ctx: TransactionContext) -> RuntimeResult<Self> {
        let tree_builder = ctx.tree_builder();
        let snapshot = ctx.snapshot();
        let table = ctx
            .catalog()
            .get_relation(op.table_id, &tree_builder, &snapshot)?;

        let index = ctx
            .catalog()
            .get_relation(op.index_id, &tree_builder, &snapshot)?;

        let table_root_page = table.root();
        let index_root_page = index.root();
        let table_schema = table.schema().clone();
        let index_schema = index.schema().clone();
        Ok(Self {
            ctx,
            table_schema,
            index_schema,
            stats: ExecutionStats::default(),
            table_root: table_root_page,
            index_root: index_root_page,
            cursor: None,
            range_end: op.range_end.clone(),
            range_start: op.range_start.clone(),
            indexed_columns: op.index_columns.clone(),
        })
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    fn range_start(&self) -> Option<&BoundExpression> {
        self.range_start.as_ref()
    }

    fn range_end(&self) -> Option<&BoundExpression> {
        self.range_end.as_ref()
    }

    fn evaluate_index_predicate(&self, row: &Row, column_index: usize) -> RuntimeResult<bool> {
        let evaluator = ExpressionEvaluator::new(row, &self.index_schema);

        let value = row[self.indexed_columns[column_index]].clone();
        let lhs = self
            .range_start()
            .map(|expr| evaluator.evaluate(expr).expect("Evaluation failed"))
            .map(|l| l <= value)
            .unwrap_or(true);

        let rhs = self
            .range_end()
            .map(|expr| evaluator.evaluate(expr).expect("Evaluation failed"))
            .map(|l| l >= value)
            .unwrap_or(true);

        Ok(lhs && rhs)
    }

    fn get_row_id_checked<'a>(&self, row: &'a Row) -> RuntimeResult<&'a UInt64> {
        let row_id = row[row.len() - 1]
            .as_big_u_int()
            .ok_or(RuntimeError::TypeError(
                TypeSystemError::UnexpectedDataType(row[row.len() - 1].kind()),
            ))?;
        Ok(row_id)
    }
}

impl Executor for IndexScan {
    fn open(&mut self) -> RuntimeResult<()> {
        let mut index_tree = self.ctx.build_tree(self.index_root);
        self.cursor = if index_tree.is_empty()? {
            None
        } else {
            Some(index_tree.iter_forward()?)
        };

        Ok(())
    }

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
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
                .get_row_at(next_pos, &self.index_schema, &snapshot)?
                .filter(|r| {
                    self.evaluate_index_predicate(r, 0)
                        .expect("Predicate evaluation failed")
                });

            if maybe_row.is_none() {
                continue;
            }

            let found_row = maybe_row.unwrap();
            let row_key_bytes = self.get_row_id_checked(&found_row)?.serialize()?;

            // Build the table tree
            let mut table_tree = self.ctx.build_tree(self.table_root);
            let actual_row_result =
                table_tree.search(row_key_bytes.as_ref(), &self.table_schema)?;

            if let SearchResult::Found(found_pos) = actual_row_result {
                let actual_row = tree.get_row_at(found_pos, &self.table_schema, &snapshot)?;

                if actual_row.is_none() {
                    continue;
                }

                self.stats.rows_produced += 1;
                return Ok(Some(actual_row.unwrap()));
            } else {
                continue;
            }
        }
    }

    fn close(&mut self) -> RuntimeResult<()> {
        let _ = self.cursor.take();
        Ok(())
    }
}
