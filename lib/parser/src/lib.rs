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

mod table;
mod column;
use table::extract_table_names_vector;


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

    match serde_json::to_string(&extract_table_names_vector(query)) {
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
