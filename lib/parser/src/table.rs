extern crate serde_json;
extern crate sqlparser;

use sqlparser::ast::SetExpr;
use sqlparser::ast::Statement;
use sqlparser::ast::TableFactor;
use sqlparser::ast::TableWithJoins;
use sqlparser::dialect::BigQueryDialect;
use sqlparser::parser::Parser;

pub fn extract_table_names_vector(query: &str) -> Vec<String> {
    let dialect = BigQueryDialect {};
    let ast = Parser::parse_sql(&dialect, query);
    match ast {
        Ok(ast) => extract_table_names_from_ast(ast),
        Err(_) => Vec::new(),
    }
}

fn extract_table_names_from_ast(ast: Vec<Statement>) -> Vec<String> {
    let mut table_names = Vec::new();
    for statement in &ast {
        if let Statement::Query(query) = statement {
            handle_set_expr(&query.body, &mut table_names)
        }
    }

    let (cte_names, mut cte_ref_tables) = extract_cte_names_from_ast(ast);

    table_names.append(&mut cte_ref_tables);
    for cte_name in cte_names {
        table_names.retain(|x| x != &cte_name);
    }

    table_names
}

fn extract_cte_names_from_ast(ast: Vec<Statement>) -> (Vec<String>, Vec<String>) {
    let mut cte_names = Vec::new();
    let mut referenced_tables = Vec::new();

    for statement in ast {
        if let Statement::Query(query) = statement {
            if let Some(with) = &query.with {
                for cte_table in &with.cte_tables {
                    cte_names.push(cte_table.alias.to_string());
                    handle_set_expr(&cte_table.query.body, &mut referenced_tables);
                }
            }
        }
    }

    (cte_names, referenced_tables)
}

fn handle_set_expr(set_expr: &SetExpr, table_names: &mut Vec<String>) {
    match set_expr {
        SetExpr::Select(select) => {
            for from in &select.from {
                extract_from_table_with_joins(from, table_names);
            }
        }
        SetExpr::SetOperation {
            op, left, right, ..
        } => match op {
            sqlparser::ast::SetOperator::Union {} => {
                handle_set_expr(left, table_names);
                handle_set_expr(right, table_names);
            }
            _ => {}
        },
        _ => {}
    }
}

fn extract_from_table_with_joins(table_with_joins: &TableWithJoins, table_names: &mut Vec<String>) {
    extract_table_factor(&table_with_joins.relation, table_names);
    for join in &table_with_joins.joins {
        extract_table_factor(&join.relation, table_names);
    }
}

fn extract_table_factor(table_factor: &TableFactor, table_names: &mut Vec<String>) {
    match table_factor {
        TableFactor::Table { name, .. } => {
            table_names.push(name.to_string());
        }
        TableFactor::Derived {
            subquery, ..
        } => {
            handle_set_expr(&subquery.body, table_names);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            extract_from_table_with_joins(table_with_joins, table_names);
        }
        _ => {}
    }
}
