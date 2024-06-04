use std::clone;
use std::collections::HashMap;

use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Ident, SelectItem, SetExpr, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[derive(Debug, PartialEq, Clone)]
struct RealColumn {
    name: String,
    table: Option<String>,
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

#[derive(Debug, PartialEq)]
struct Table {
    references: Vec<RealColumn>,
}

impl Table {
    fn new() -> Table {
        Table {
            references: Vec::new(),
        }
    }

    fn add_reference(&mut self, reference: RealColumn) {
        self.references.push(reference);
    }
}

#[derive(Debug, PartialEq)]
struct ColumnLineage {
    columns: Vec<Column>,
    table: Table,
}

pub fn extract_columns_from_query(sql: &str) -> ColumnLineage {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    let mut table = Table::new();
    let mut columns: Vec<Column> = Vec::new();

    for statement in statements {
        match statement {
            Statement::Query(query) => match *query.body {
                SetExpr::Select(select) => {
                    for item in select.projection {
                        match item {
                            SelectItem::UnnamedExpr(expr) => {
                                columns.push(handle_expr(expr));
                            }
                            SelectItem::ExprWithAlias { expr, alias } => {
                                columns.push(handle_expr_with_alias(expr, alias));
                            }
                            _ => {}
                        }
                    }

                    // extract columns from where clause
                    if let Some(expr) = select.selection {
                        let idents = find_idents(&expr);
                        for ident in idents {
                            let rc = RealColumn {
                                name: ident.value.clone(),
                                table: None,
                            };

                            table.add_reference(rc);
                        }
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }

    return ColumnLineage { columns, table };
}

fn handle_expr_with_alias(expr: Expr, alias: Ident) -> Column {
    match expr {
        Expr::Identifier(ident) => {
            return handle_identifier(ident, Some(alias.value));
        }
        Expr::CompoundIdentifier(ident) => {
            return handle_compound_identifier(ident, Some(alias.value.clone()));
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            return handle_case_expr(
                alias.value.clone(),
                operand,
                conditions,
                results,
                else_result,
            );
        }
        Expr::Function(func) => {
            return handle_function_expr(func.args, alias);
        }
        _ => {
            println!("Expr: {:?}", expr);
            return Column::new("handle_expr_with_alias - EXCEPTION OCCURRED".to_string());
        }
    }
}

fn handle_expr(expr: Expr) -> Column {
    match expr {
        Expr::Identifier(ident) => {
            return handle_identifier(ident, None);
        }
        Expr::CompoundIdentifier(ident) => {
            return handle_compound_identifier(ident, None);
        }
        _ => {
            println!("Expr: {:?}", expr);
            return Column::new("handle_expr - EXCEPTION OCCURRED".to_string());
        }
    }
}

fn handle_compound_identifier(ident: Vec<Ident>, alias: Option<String>) -> Column {
    let full_name: String = ident
        .iter()
        .map(|id| id.value.as_str())
        .collect::<Vec<&str>>()
        .join(".");

    let name = alias.unwrap_or(full_name);

    let mut col = Column::new(name);
    col.add_reference(RealColumn {
        name: ident[1].value.clone(),
        table: Some(ident[0].value.clone()),
    });

    col
}

fn handle_identifier(ident: Ident, alias: Option<String>) -> Column {
    let mut col = Column::new(alias.unwrap_or(ident.value.clone()));
    col.add_reference(RealColumn {
        name: ident.value.clone(),
        table: None,
    });

    col
}

fn handle_function_expr(args: Vec<FunctionArg>, alias: Ident) -> Column {
    let mut col = Column::new(alias.value.clone());

    let mut columns = Vec::new();

    for arg in args {
        match arg {
            FunctionArg::Unnamed(arg) => match arg {
                FunctionArgExpr::Expr(expr) => {
                    let found_col = handle_expr_with_alias(expr, alias.clone());
                    columns.push(found_col);
                }
                _ => {
                    println!("unexpected function arg stuff found unnamed, take a look")
                }
            },
            FunctionArg::Named { name, arg } => match arg {
                FunctionArgExpr::Expr(expr) => {
                    let found_col = handle_expr_with_alias(expr, alias.clone());
                    columns.push(found_col);
                }
                _ => {
                    println!("unexpected function arg stuff found named, take a look")
                }
            },
        }
    }

    let mut unique_references = HashMap::new();
    for column in columns {
        for reference in column.references {

            let cr = &reference;  // Use a reference to avoid cloning

            let unique_key = match &cr.table {
                Some(table) => format!("{}-{}", cr.name, table),
                None => format!("{}-", cr.name),
            };

            if unique_references.contains_key(&unique_key) {
                continue;
            }

            col.add_reference(reference.clone());
            unique_references.insert(unique_key, true);
        }
    }

    return col;
}

fn handle_case_expr(
    name: String,
    operand: Option<Box<Expr>>,
    conditions: Vec<Expr>,
    results: Vec<Expr>,
    else_result: Option<Box<Expr>>,
) -> Column {
    let mut col = Column::new(name);

    // initialize an empty hashmap for seen idents
    let mut seen_idents = HashMap::new();

    let idents = find_idents_for_case(operand, conditions, results, else_result);
    for ident in idents {
        let rc = RealColumn {
            name: ident.value.clone(),
            table: None,
        };

        if seen_idents.contains_key(&ident.value) {
            continue;
        }

        col.add_reference(rc);
        seen_idents.insert(ident.value.clone(), true);
    }

    return col;
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
        let expected = ColumnLineage {
            columns: vec![
                Column {
                    name: "a".to_string(),
                    references: vec![RealColumn {
                        name: "a".to_string(),
                        table: None,
                    }],
                },
                Column {
                    name: "b".to_string(),
                    references: vec![RealColumn {
                        name: "b".to_string(),
                        table: None,
                    }],
                },
                Column {
                    name: "c".to_string(),
                    references: vec![RealColumn {
                        name: "c".to_string(),
                        table: None,
                    }],
                },
                Column {
                    name: "d".to_string(),
                    references: vec![RealColumn {
                        name: "d".to_string(),
                        table: None,
                    }],
                },
            ],
            table: Table { references: vec![] },
        };

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

        let expected = ColumnLineage {
            columns: vec![
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
                        },
                    ],
                },
            ],
            table: Table {
                references: vec![RealColumn {
                    name: "in_stock".to_string(),
                    table: None,
                }],
            },
        };

        let res = extract_columns_from_query(query);

        assert_eq!(res, expected);
    }

    #[test]
    fn test_ctes() {
        let query = "SELECT 
            COALESCE(`table1`.`a`, `table2`.`a`) AS `a`, 
            `table1`.`b` AS `b`, 
            `table2`.`c` AS `c` 
        FROM `table1` AS `table1` 
            JOIN `table2` AS `table2` 
                ON `table1`.`a` = `table2`.`a`";

        let expected = ColumnLineage {
            columns: vec![
                Column {
                    name: "a".to_string(),
                    references: vec![
                        RealColumn {
                            name: "a".to_string(),
                            table: Some("table1".to_string()),
                        },
                        RealColumn {
                            name: "a".to_string(),
                            table: Some("table2".to_string()),
                        },
                    ],
                },
                Column {
                    name: "b".to_string(),
                    references: vec![RealColumn {
                        name: "b".to_string(),
                        table: Some("table1".to_string()),
                    }],
                },
                Column {
                    name: "c".to_string(),
                    references: vec![RealColumn {
                        name: "c".to_string(),
                        table: Some("table2".to_string()),
                    }],
                },
            ],
            table: Table {
                references: vec![],
            },
        };

        let res = extract_columns_from_query(query);

        assert_eq!(res, expected);
    }
}
