use crate::sql::ast::*;
use crate::sql::parse_sql;
use crate::sql_test;

sql_test!(
    test_parser_delete_basic,
    "DELETE FROM users WHERE id = 1;",
    Statement::Delete(DeleteStatement {
        table: "users".to_string(),
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("id".to_string())),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Number(1.0))
        })
    })
);

sql_test!(
    test_parser_delete_no_where,
    "DELETE FROM logs;",
    Statement::Delete(DeleteStatement {
        table: "logs".to_string(),
        where_clause: None
    })
);

sql_test!(
    test_parser_delete_where,
    "DELETE FROM orders WHERE status = 'cancelled' AND created_at < '2024-01-01';",
    Statement::Delete(DeleteStatement {
        table: "orders".to_string(),
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("status".to_string())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::String("cancelled".to_string()))
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("created_at".to_string())),
                op: BinaryOperator::Lt,
                right: Box::new(Expr::String("2024-01-01".to_string()))
            })
        })
    })
);
