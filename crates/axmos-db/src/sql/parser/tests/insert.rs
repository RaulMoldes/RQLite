use crate::sql::parser::ast::*;
use crate::sql_test;

sql_test!(
    test_parser_insert_basic,
    "INSERT INTO my_table(col1, col2) VALUES (1, 2);",
    Statement::Insert(InsertStatement {
        table: "my_table".to_string(),
        columns: Some(vec!["col1".to_string(), "col2".to_string()]),
        values: Values::Values(vec![vec![Expr::Number(1.0), Expr::Number(2.0)]])
    })
);

sql_test!(
    test_parser_insert_with_select,
    "INSERT INTO my_table(col1, col2) SELECT (col1, col2) FROM my_other_table;",
    Statement::Insert(InsertStatement {
        table: "my_table".to_string(),
        columns: Some(vec!["col1".to_string(), "col2".to_string()]),
        values: Values::Query(Box::new(SelectStatement {
            distinct: false,
            columns: vec![SelectItem::ExprWithAlias {
                expr: Expr::List(vec![
                    Expr::Identifier("col1".to_string()),
                    Expr::Identifier("col2".to_string())
                ]),
                alias: None
            }],
            from: Some(TableReference::Table {
                name: "my_other_table".to_string(),
                alias: None
            }),
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None
        }))
    })
);

sql_test!(
    test_parser_insert_multiple_rows,
    "INSERT INTO users (name, email) VALUES ('John', 'john@example.com'), ('Jane', 'jane@example.com');",
    Statement::Insert(InsertStatement {
        table: "users".to_string(),
        columns: Some(vec!["name".to_string(), "email".to_string()]),
        values: Values::Values(vec![
            vec![
                Expr::String("John".to_string()),
                Expr::String("john@example.com".to_string())
            ],
            vec![
                Expr::String("Jane".to_string()),
                Expr::String("jane@example.com".to_string())
            ]
        ])
    })
);

sql_test!(
    test_parser_insert_without_columns,
    "INSERT INTO users VALUES (1, 'John', 'john@example.com');",
    Statement::Insert(InsertStatement {
        table: "users".to_string(),
        columns: None,
        values: Values::Values(vec![vec![
            Expr::Number(1.0),
            Expr::String("John".to_string()),
            Expr::String("john@example.com".to_string())
        ]])
    })
);
