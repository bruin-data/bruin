extern crate serde_json;
extern crate sqlparser;

use serde_json::json;
use serde_json::Result as JsonResult;
use sqlparser::ast::SetExpr;
use sqlparser::ast::Statement;
use sqlparser::ast::TableFactor;
use sqlparser::ast::TableWithJoins;
use sqlparser::dialect::BigQueryDialect;
use sqlparser::parser::Parser;
use std::ffi::c_char;
use std::ffi::CStr;
use std::ffi::CString;

#[no_mangle]
pub extern "C" fn parse_sql(name: *const libc::c_char) -> *const c_char {
    let name_cstr = unsafe { CStr::from_ptr(name) };
    let query = name_cstr.to_str().unwrap();

    match ast_to_json(query) {
        Ok(json) => {
            let s = CString::new(json).unwrap();
            let p = s.as_ptr();
            std::mem::forget(s);
            p
        }
        Err(e) => {
            // return a json error string cstring
            let s = CString::new(json!({ "error": e.to_string()}).to_string()).unwrap();
            let p = s.as_ptr();
            std::mem::forget(s);
            p
        }
    }
}

#[no_mangle]
pub extern "C" fn extract_table_names(name: *const libc::c_char) -> *const c_char {
    let name_cstr = unsafe { CStr::from_ptr(name) };
    let query = name_cstr.to_str().unwrap();

    match extract_table_names_to_json(query) {
        Ok(json) => {
            let s = CString::new(json).unwrap();
            let p = s.as_ptr();
            std::mem::forget(s);
            p
        }
        Err(e) => {
            // return a json error string cstring
            let s = CString::new(json!({ "error": e.to_string()}).to_string()).unwrap();
            let p = s.as_ptr();
            std::mem::forget(s);
            p
        }
    }
}

fn ast_to_json(query: &str) -> JsonResult<String> {
    let dialect = BigQueryDialect {};
    let ast = Parser::parse_sql(&dialect, query);
    match ast {
        Ok(ast) => serde_json::to_string(&ast),
        Err(e) => serde_json::to_string(&e.to_string()),
    }
}

fn extract_table_names_to_json(query: &str) -> JsonResult<String> {
    serde_json::to_string(&extract_table_names_vector(query))
}

fn extract_table_names_vector(query: &str) -> Vec<String> {
    let dialect = BigQueryDialect {};
    let ast = Parser::parse_sql(&dialect, query);
    match ast {
        Ok(ast) => extract_table_names_from_ast(ast),
        Err(_) => Vec::new(),
    }
}

fn extract_table_names_from_ast(ast: Vec<Statement>) -> Vec<String> {
    ast[0].to_string()
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_extract_table_names() {
        let query = "
        select * from t1 union all
select * from t2 union all
select * from t3
        ";
        let expected = vec!["t1".to_string(), "t2".to_string(), "t3".to_string()];
        let result = extract_table_names_vector(query);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_extract_table_names_from_ctes() {
        let query = "
        with t1 as (
            select *
            from table1
        ),
        t2 as (
            select *
            from table2
        )
        select *
        from t1
        join t2
            using(a)
        ";
        let expected = vec!["table1".to_string(), "table2".to_string()];
        let result = extract_table_names_vector(query);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_extract_table_names_from_ctes_nested() {
        let query = "
        select *
        from table1
        join (
            select *
            from (
                select *
                from table2
            ) t2
        ) t3
            using(a)
        ";
        let expected = vec!["table1".to_string(), "table2".to_string()];
        let result = extract_table_names_vector(query);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_extract_table_names_from_ctes_with_repeating_aliases() {
        let query = "
        select *
        from table1
        join (
            select *
            from (
                select *
                from table2
            ) t2
        ) t2
            using(a)
        join (
            select *
            from (
                select *
                from table3
            ) t2
        ) t3
            using(b)
        ";
        let expected = vec!["table1".to_string(), "table2".to_string(), "table3".to_string()];
        let result = extract_table_names_vector(query);

        assert_eq!(result, expected);
    }
}
