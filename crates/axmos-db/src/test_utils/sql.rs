use crate::{
    database::{
        Database,
        errors::{AnalyzerResult, BinderResult, ExecutionResult, IntoBoxError, OptimizerResult},
        stats::CatalogStatsProvider,
    },
    sql::{
        analyzer::Analyzer,
        binder::{Binder, ast::BoundStatement},
        parser::{
            Parser,
            ast::{Simplify, Statement},
        },
        planner::{
            logical::LogicalPlan,
            memo::{GroupId, Memo},
            plan::PlanBuilder,
        },
    },
};

/// Parse a SQL query string into an AST
pub fn parse_sql(sql: &str) -> ExecutionResult<Statement> {
    let mut parser = Parser::new(sql);
    let mut stmt = parser.parse()?;
    stmt.simplify();
    Ok(stmt)
}

pub fn analyze_sql(sql: String, db: &Database) -> AnalyzerResult<()> {
    db.task_runner().run(move |ctx| {
        let stmt = parse_sql(&sql).box_err()?;
        let mut analyzer = Analyzer::new(ctx.catalog(), ctx.accessor());
        analyzer.analyze(&stmt).box_err()?;
        Ok(())
    })?;
    Ok(())
}

pub fn resolve_sql(sql: String, db: &Database) -> BinderResult<BoundStatement> {
    let stmt = db.task_runner().run_with_result(move |ctx| {
        let stmt = parse_sql(&sql).box_err()?;
        let mut binder = Binder::new(ctx.catalog(), ctx.accessor());
        binder.bind(&stmt).box_err()
    })?;
    Ok(stmt)
}

pub fn create_plan(
    bound_stmt: BoundStatement,
    db: &Database,
) -> OptimizerResult<(GroupId, LogicalPlan)> {
    let result = db.task_runner().run_with_result(move |ctx| {
        let provider = CatalogStatsProvider::new(ctx.catalog(), ctx.accessor());

        let mut memo = Memo::new();
        let builder = PlanBuilder::new(&mut memo, ctx.catalog(), ctx.accessor(), &provider);
        let id = builder.build(&bound_stmt).box_err()?;
        let displayer = PlanBuilder::new(&mut memo, ctx.catalog(), ctx.accessor(), &provider);
        let plan = displayer.build_logical_plan(id).box_err()?;
        Ok((id, plan))
    })?;
    Ok(result)
}

fn build_plan(sql: String, db: &Database) -> OptimizerResult<GroupId> {
    let stmt = resolve_sql(sql, db)?;
    let (id, plan) = create_plan(stmt, db)?;
    println!("{plan}");
    Ok(id)
}
