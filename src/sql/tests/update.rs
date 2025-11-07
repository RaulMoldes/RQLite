use crate::sql::ast::*;
use crate::sql::parse_sql;
use crate::sql_test;

sql_test!(
   test_parser_update_basic,
    "UPDATE my_table SET col1 = 5 WHERE col2 < 3;",
    Statement::Update(UpdateStatement {
        table: "my_table".to_string(),
        set_clauses: vec![SetClause {
            column: "col1".to_string(),
            value: Expr::Number(5.0)
        }],
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("col2".to_string())),
            op: BinaryOperator::Lt,
            right: Box::new(Expr::Number(3.0))
        })
    })
);

sql_test!(
   test_parser_update_multiple_columns,
    "UPDATE users SET name = 'John Doe', email = 'john.doe@example.com', updated_at = '2024-01-01' WHERE id = 1;",
    Statement::Update(UpdateStatement {
        table: "users".to_string(),
        set_clauses: vec![
            SetClause {
                column: "name".to_string(),
                value: Expr::String("John Doe".to_string())
            },
            SetClause {
                column: "email".to_string(),
                value: Expr::String("john.doe@example.com".to_string())
            },
            SetClause {
                column: "updated_at".to_string(),
                value: Expr::String("2024-01-01".to_string())
            }
        ],
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("id".to_string())),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Number(1.0))
        })
    })
);

sql_test!(
   test_parser_update_with_expression,
    "UPDATE products SET price = price * 1.1 WHERE category = 'electronics';",
    Statement::Update(UpdateStatement {
        table: "products".to_string(),
        set_clauses: vec![SetClause {
            column: "price".to_string(),
            value: Expr::BinaryOp {
                left: Box::new(Expr::Identifier("price".to_string())),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Number(1.1))
            }
        }],
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("category".to_string())),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::String("electronics".to_string()))
        })
    })
);

sql_test!(
   test_parser_update_no_where,
    "UPDATE settings SET enabled = true;",
    Statement::Update(UpdateStatement {
        table: "settings".to_string(),
        set_clauses: vec![SetClause {
            column: "enabled".to_string(),
            value: Expr::Boolean(true)
        }],
        where_clause: None
    })
);
