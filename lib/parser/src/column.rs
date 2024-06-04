use std::collections::HashMap;

use sqlparser::ast::{Expr, Ident, SelectItem, SetExpr, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[derive(Debug, PartialEq)]
struct RealColumn {
    name: String,
    table: Option<String> 
}

#[derive(Debug, PartialEq)]
struct Column {
    name: String,
    references: Vec<RealColumn>,
}

impl Column {
    fn new(name: String) -> Column {
        Column {
            name,
            references: Vec::new(),
        }
    }

    fn add_reference(&mut self, reference: RealColumn) {
        self.references.push(reference);
    }
}

struct Table {
    references: Vec<RealColumn>,
}

pub fn extract_columns_from_query(sql: &str) -> Vec<Column> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    println!("Extracting columns from query: {}", sql);

    let mut columns: Vec<Column> = Vec::new();

    for statement in statements {
        match statement {
            Statement::Query(query) => match *query.body {
                SetExpr::Select(select) => {
                    for item in select.projection {
                        match item {
                            SelectItem::UnnamedExpr(expr) => match expr {
                                Expr::Identifier(ident) => {
                                    let mut col = Column::new(ident.value);
                                    col.add_reference(RealColumn {
                                        name: col.name.clone(),
                                        table: None
                                    });

                                    columns.push(col);
                                }
                                Expr::CompoundIdentifier(ident) => {
                                    let mut col = Column::new(ident[1].value.clone());
                                    col.add_reference(RealColumn {
                                        name: ident[1].value.clone(),
                                        table: Some(ident[0].value.clone())
                                    });

                                    columns.push(col);
                                }
                                _ => {
                                    println!("Expr: {:?}", expr);
                                }
                            },
                            SelectItem::ExprWithAlias { expr, alias } => match expr {
                                Expr::Identifier(ident) => {
                                    // columns.push(Column::new(ident.value, Some(alias.value)));
                                }
                                Expr::CompoundIdentifier(ident) => {
                                    let fullName: String = ident
                                        .iter()
                                        .map(|id| id.value.as_str())
                                        .collect::<Vec<&str>>()
                                        .join(".");

                                    // columns.push(Column::new(fullName, None));
                                }
                                Expr::Case {
                                    operand,
                                    conditions,
                                    results,
                                    else_result,
                                } => {
                                    handle_case_expr(alias.value.clone(), operand, conditions, results, else_result, &mut columns);
                                }
                                _ => {}
                            },
                            _ => {}
                        }
                    }

                    // extract columns from where clause
                    if let Some(expr) = select.selection {
                        let idents = find_idents(&expr);
                        for ident in idents {
                            let rc = RealColumn {
                                name: ident.value.clone(),
                                table: None
                            };

                            let mut col = Column::new(ident.value.clone());
                            col.add_reference(rc);
                            columns.push(col);
                        }
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }

    columns
}

fn handle_case_expr(name: String, operand: Option<Box<Expr>>, conditions: Vec<Expr>, results: Vec<Expr>, else_result: Option<Box<Expr>>, columns: &mut Vec<Column>) {
    let mut col = Column::new(name);

    // initialize an empty hashmap for seen idents
    let mut seen_idents = HashMap::new();

    let idents = find_idents_for_case(operand, conditions, results, else_result);
    for ident in idents {
        let rc = RealColumn {
            name: ident.value.clone(),
            table: None
        };

        if seen_idents.contains_key(&ident.value) {
            continue;
        }
    
        col.add_reference(rc);
        seen_idents.insert(ident.value.clone(), true);
    }
    columns.push(col);
}

fn find_idents_for_case(
    operand: Option<Box<Expr>>,
    conditions: Vec<Expr>,
    results: Vec<Expr>,
    else_result: Option<Box<Expr>>,
) -> Vec<Ident> {
    let mut allIdents = Vec::new();

    if let Some(operand) = operand {
        let identsInOperand = find_idents(&operand);
        allIdents.extend(identsInOperand);
    } else {
        println!("No operand");
    }

    for condition in conditions {
        let identsInCondition = find_idents(&condition);
        allIdents.extend(identsInCondition);
    }

    for result in results {
        let identsInResult = find_idents(&result);
        allIdents.extend(identsInResult);
    }

    if let Some(else_result) = else_result {
        let identsInElseResult = find_idents(&else_result);
        allIdents.extend(identsInElseResult);
    } else {
        println!("No else result");
    }

    allIdents
}

fn find_idents(expr: &Expr) -> Vec<Ident> {
    let mut idents = Vec::new();
    match expr {
        Expr::Identifier(ident) => idents.push(ident.clone()),
        Expr::CompoundIdentifier(compound_idents) => {
            let fullName = compound_idents
                .iter()
                .map(|id| id.value.as_str())
                .collect::<Vec<&str>>()
                .join(".");

            idents.push(Ident::new(fullName));
        }
        Expr::BinaryOp { left, right, .. } => {
            idents.extend(find_idents(left));
            idents.extend(find_idents(right));
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            for operand in operand {
                idents.extend(find_idents(operand));
            }
            for condition in conditions {
                idents.extend(find_idents(&condition));
            }
            for result in results {
                idents.extend(find_idents(&result));
            }
            for else_result in else_result {
                idents.extend(find_idents(else_result));
            }
        }
        Expr::UnaryOp { expr, .. } => idents.extend(find_idents(expr)),
        Expr::Nested(expr) => idents.extend(find_idents(expr)),
        _ => {}
    }

    idents
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    #[test]
    fn test_extract_table_names() {
        let query = "select a, b, c, d from dataset.table1";
        let expected: Vec<Column> = vec![
            Column {
                name: "a".to_string(),
                references: vec![RealColumn {
                    name: "a".to_string(),
                    table: None
                }],
            },
            Column {
                name: "b".to_string(),
                references: vec![RealColumn {
                    name: "b".to_string(),
                    table: None
                }],
            },
            Column {
                name: "c".to_string(),
                references: vec![RealColumn {
                    name: "c".to_string(),
                    table: None
                }],
            },
            Column {
                name: "d".to_string(),
                references: vec![RealColumn {
                    name: "d".to_string(),
                    table: None
                }],
            },
        ];

        let res = extract_columns_from_query(query);

        assert_eq!(res, expected);
    }

    #[test]
    fn test_extract_table_names_with_where() {
        let query = "SELECT
            item_id,
            CASE
                WHEN price > 1000 AND t2.somecol < 250 THEN 'high'
                WHEN price > 100 THEN 'medium'
                ELSE 'low'
            END as price_category
        FROM items
        WHERE in_stock = true";

        let expected: Vec<Column> = vec![
            Column {
                name: "item_id".to_string(),
                references: vec![RealColumn {
                    name: "item_id".to_string(),
                    table: None,
                }],
            },
            Column {
                name: "price_category".to_string(),
                references: vec![
                    RealColumn {
                        name: "price".to_string(),
                        table: None,
                    },
                    RealColumn {
                        name: "t2.somecol".to_string(),
                        table: None,
                    }
                ],
            },
        ];

        let res = extract_columns_from_query(query);

        assert_eq!(res, expected);
    }
}
