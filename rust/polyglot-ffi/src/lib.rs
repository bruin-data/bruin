use polyglot_sql::{self as pgsql, DialectType, Expression};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

// ── helpers ──────────────────────────────────────────────────────────
fn to_rust_str(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(ptr) }.to_str().ok().map(String::from)
}

fn to_c_string(s: &str) -> *mut c_char {
    CString::new(s).unwrap_or_default().into_raw()
}

fn json_result<T: Serialize>(val: &T) -> *mut c_char {
    to_c_string(&serde_json::to_string(val).unwrap_or_else(|e| {
        serde_json::to_string(&ErrorResponse {
            error: format!("json serialization error: {e}"),
        })
        .unwrap()
    }))
}

fn json_error(msg: &str) -> *mut c_char {
    json_result(&ErrorResponse {
        error: msg.to_string(),
    })
}

// ── Dialect mapping ─────────────────────────────────────────────────
fn map_dialect(name: &str) -> Option<DialectType> {
    match name.to_lowercase().as_str() {
        "bigquery" => Some(DialectType::BigQuery),
        "snowflake" => Some(DialectType::Snowflake),
        "postgres" | "postgresql" => Some(DialectType::PostgreSQL),
        "mysql" => Some(DialectType::MySQL),
        "redshift" => Some(DialectType::Redshift),
        "athena" => Some(DialectType::Athena),
        "clickhouse" => Some(DialectType::ClickHouse),
        "databricks" => Some(DialectType::Databricks),
        "tsql" | "mssql" => Some(DialectType::TSQL),
        "duckdb" => Some(DialectType::DuckDB),
        "sqlite" => Some(DialectType::SQLite),
        "hive" => Some(DialectType::Hive),
        "spark" => Some(DialectType::Spark),
        "trino" => Some(DialectType::Trino),
        "presto" => Some(DialectType::Presto),
        "oracle" => Some(DialectType::Oracle),
        "teradata" => Some(DialectType::Teradata),
        _ => None,
    }
}

/// Extract table name from a source Expression by generating it back to SQL.
fn extract_table_name_from_expr(expr: &Expression, dialect: DialectType) -> Option<String> {
    match pgsql::generate(expr, dialect) {
        Ok(s) => {
            let trimmed = s.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                // Strip any alias (e.g. "table1 AS t1" -> "table1")
                // Also strip surrounding backticks for consistency
                let parts: Vec<&str> = trimmed.split_ascii_whitespace().collect();
                let name = parts
                    .first()
                    .unwrap_or(&&trimmed.as_str())
                    .trim_matches('`')
                    .to_string();
                if name.is_empty() {
                    None
                } else {
                    Some(name)
                }
            }
        }
        Err(_) => None,
    }
}

/// Extract table names from a parsed expression using scope analysis.
/// Uses build_scope() to find all sources (tables/subqueries) and then
/// recursively collects table names from all scopes, excluding CTE references.
fn get_tables_from_expr(expr: &Expression, dialect: DialectType) -> Vec<String> {
    let scope = pgsql::scope::build_scope(expr);
    // Collect CTE names so we can exclude them from the table list
    let mut cte_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    collect_cte_names(&scope, &mut cte_names);

    let mut names: Vec<String> = Vec::new();
    collect_tables_from_scope(&scope, dialect, &cte_names, &mut names);
    names.sort();
    names.dedup();
    names
}

fn collect_cte_names(scope: &pgsql::scope::Scope, out: &mut std::collections::HashSet<String>) {
    for (name, _) in &scope.cte_sources {
        out.insert(name.clone());
    }
    for child in &scope.cte_scopes {
        collect_cte_names(child, out);
    }
    for child in &scope.derived_table_scopes {
        collect_cte_names(child, out);
    }
    for child in &scope.subquery_scopes {
        collect_cte_names(child, out);
    }
    for child in &scope.union_scopes {
        collect_cte_names(child, out);
    }
}

fn collect_tables_from_scope(
    scope: &pgsql::scope::Scope,
    dialect: DialectType,
    cte_names: &std::collections::HashSet<String>,
    out: &mut Vec<String>,
) {
    // Collect tables from sources in this scope
    for (name, source_info) in &scope.sources {
        if !source_info.is_scope {
            // Skip CTE references
            if cte_names.contains(name) {
                continue;
            }
            if let Some(table_name) = extract_table_name_from_expr(&source_info.expression, dialect) {
                // Also check if the extracted name itself is a CTE
                if !cte_names.contains(&table_name) {
                    out.push(table_name);
                }
            }
        }
    }

    // Recurse into child scopes
    for child in &scope.derived_table_scopes {
        collect_tables_from_scope(child, dialect, cte_names, out);
    }
    for child in &scope.subquery_scopes {
        collect_tables_from_scope(child, dialect, cte_names, out);
    }
    for child in &scope.cte_scopes {
        collect_tables_from_scope(child, dialect, cte_names, out);
    }
    for child in &scope.udtf_scopes {
        collect_tables_from_scope(child, dialect, cte_names, out);
    }
    for child in &scope.union_scopes {
        collect_tables_from_scope(child, dialect, cte_names, out);
    }
}

// ── response types ──────────────────────────────────────────────────
#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct TablesResponse {
    tables: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct QueryResponse {
    query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct SingleSelectResponse {
    is_single_select: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct UpstreamColumn {
    column: String,
    table: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ColumnLineageItem {
    name: String,
    upstream: Vec<UpstreamColumn>,
    #[serde(default)]
    r#type: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct LineageResponse {
    columns: Vec<ColumnLineageItem>,
    non_selected_columns: Vec<ColumnLineageItem>,
    errors: Vec<String>,
}

// ── request types ───────────────────────────────────────────────────
#[derive(Deserialize)]
struct RenameTablesRequest {
    query: String,
    dialect: String,
    table_mapping: HashMap<String, String>,
}

#[derive(Deserialize)]
struct AddLimitRequest {
    query: String,
    limit: usize,
    dialect: String,
}

#[derive(Deserialize)]
struct LineageRequest {
    query: String,
    dialect: String,
    #[serde(default)]
    #[allow(dead_code)]
    schema: HashMap<String, HashMap<String, String>>,
}

// ── FFI: free ───────────────────────────────────────────────────────
/// Free a string previously returned by any polyglot_* function.
#[unsafe(no_mangle)]
pub extern "C" fn polyglot_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            drop(CString::from_raw(ptr));
        }
    }
}

// ── FFI: get_tables ─────────────────────────────────────────────────
#[unsafe(no_mangle)]
pub extern "C" fn polyglot_get_tables(
    sql: *const c_char,
    dialect: *const c_char,
) -> *mut c_char {
    let sql_str = match to_rust_str(sql) {
        Some(s) => s,
        None => return json_error("null sql pointer"),
    };
    let dialect_str = match to_rust_str(dialect) {
        Some(s) => s,
        None => return json_error("null dialect pointer"),
    };
    let dialect_type = match map_dialect(&dialect_str) {
        Some(d) => d,
        None => {
            return json_error(&format!("unsupported dialect: {dialect_str}"));
        }
    };

    let expressions = match pgsql::parse(&sql_str, dialect_type) {
        Ok(e) => e,
        Err(e) => {
            return json_result(&TablesResponse {
                tables: vec![],
                error: Some(format!("{e}")),
            });
        }
    };

    let mut all_tables: Vec<String> = Vec::new();
    for expr in &expressions {
        all_tables.extend(get_tables_from_expr(expr, dialect_type));
    }

    all_tables.sort();
    all_tables.dedup();

    json_result(&TablesResponse {
        tables: all_tables,
        error: None,
    })
}

// ── FFI: rename_tables ──────────────────────────────────────────────
#[unsafe(no_mangle)]
pub extern "C" fn polyglot_rename_tables(request_json: *const c_char) -> *mut c_char {
    let req_str = match to_rust_str(request_json) {
        Some(s) => s,
        None => return json_error("null request pointer"),
    };
    let req: RenameTablesRequest = match serde_json::from_str(&req_str) {
        Ok(r) => r,
        Err(e) => return json_error(&format!("invalid request json: {e}")),
    };
    let dialect_type = match map_dialect(&req.dialect) {
        Some(d) => d,
        None => return json_error(&format!("unsupported dialect: {}", req.dialect)),
    };

    let mut expressions = match pgsql::parse(&req.query, dialect_type) {
        Ok(e) => e,
        Err(e) => return json_error(&format!("parse error: {e}")),
    };

    if expressions.is_empty() {
        return json_error("no statements parsed");
    }

    let expr = expressions.remove(0);
    let renamed = pgsql::ast_transforms::rename_tables(expr, &req.table_mapping);

    match pgsql::generate(&renamed, dialect_type) {
        Ok(sql) => json_result(&QueryResponse {
            query: sql,
            error: None,
        }),
        Err(e) => json_error(&format!("generate error: {e}")),
    }
}

// ── FFI: add_limit ──────────────────────────────────────────────────
#[unsafe(no_mangle)]
pub extern "C" fn polyglot_add_limit(request_json: *const c_char) -> *mut c_char {
    let req_str = match to_rust_str(request_json) {
        Some(s) => s,
        None => return json_error("null request pointer"),
    };
    let req: AddLimitRequest = match serde_json::from_str(&req_str) {
        Ok(r) => r,
        Err(e) => return json_error(&format!("invalid request json: {e}")),
    };
    let dialect_type = match map_dialect(&req.dialect) {
        Some(d) => d,
        None => return json_error(&format!("unsupported dialect: {}", req.dialect)),
    };

    let mut expressions = match pgsql::parse(&req.query, dialect_type) {
        Ok(e) => e,
        Err(e) => return json_error(&format!("parse error: {e}")),
    };

    if expressions.is_empty() {
        return json_error("no statements parsed");
    }

    let expr = expressions.remove(0);
    let limited = pgsql::ast_transforms::set_limit(expr, req.limit);

    match pgsql::generate(&limited, dialect_type) {
        Ok(sql) => json_result(&QueryResponse {
            query: sql,
            error: None,
        }),
        Err(e) => json_error(&format!("generate error: {e}")),
    }
}

// ── FFI: is_single_select ───────────────────────────────────────────
#[unsafe(no_mangle)]
pub extern "C" fn polyglot_is_single_select(
    sql: *const c_char,
    dialect: *const c_char,
) -> *mut c_char {
    let sql_str = match to_rust_str(sql) {
        Some(s) => s,
        None => return json_error("null sql pointer"),
    };
    let dialect_str = match to_rust_str(dialect) {
        Some(s) => s,
        None => return json_error("null dialect pointer"),
    };

    if sql_str.trim().is_empty() {
        return json_result(&SingleSelectResponse {
            is_single_select: false,
            error: Some("cannot parse query".to_string()),
        });
    }

    let dialect_type = match map_dialect(&dialect_str) {
        Some(d) => d,
        None => {
            return json_result(&SingleSelectResponse {
                is_single_select: false,
                error: Some(format!("unsupported dialect: {dialect_str}")),
            });
        }
    };

    let expressions = match pgsql::parse(&sql_str, dialect_type) {
        Ok(e) => e,
        Err(e) => {
            return json_result(&SingleSelectResponse {
                is_single_select: false,
                error: Some(format!("{e}")),
            });
        }
    };

    if expressions.len() != 1 {
        return json_result(&SingleSelectResponse {
            is_single_select: false,
            error: None,
        });
    }

    // Check if the single expression is a SELECT statement by generating
    // it back to SQL and checking the prefix.
    let is_select = match pgsql::generate(&expressions[0], dialect_type) {
        Ok(sql) => {
            let trimmed = sql.trim().to_uppercase();
            trimmed.starts_with("SELECT") || trimmed.starts_with("WITH")
        }
        Err(_) => false,
    };

    json_result(&SingleSelectResponse {
        is_single_select: is_select,
        error: None,
    })
}

// ── FFI: column_lineage ─────────────────────────────────────────────
#[unsafe(no_mangle)]
pub extern "C" fn polyglot_column_lineage(request_json: *const c_char) -> *mut c_char {
    let req_str = match to_rust_str(request_json) {
        Some(s) => s,
        None => return json_error("null request pointer"),
    };
    let req: LineageRequest = match serde_json::from_str(&req_str) {
        Ok(r) => r,
        Err(e) => return json_error(&format!("invalid request json: {e}")),
    };
    let dialect_type = match map_dialect(&req.dialect) {
        Some(d) => d,
        None => {
            return json_result(&LineageResponse {
                columns: vec![],
                non_selected_columns: vec![],
                errors: vec![format!("unsupported dialect: {}", req.dialect)],
            });
        }
    };

    let expressions = match pgsql::parse(&req.query, dialect_type) {
        Ok(e) => e,
        Err(e) => {
            return json_result(&LineageResponse {
                columns: vec![],
                non_selected_columns: vec![],
                errors: vec![format!("parse error: {e}")],
            });
        }
    };

    if expressions.is_empty() {
        return json_result(&LineageResponse {
            columns: vec![],
            non_selected_columns: vec![],
            errors: vec!["no statements parsed".to_string()],
        });
    }

    let expr = &expressions[0];

    // Extract column names from the SELECT clause
    let col_names = pgsql::ast_transforms::get_column_names(expr);

    let mut columns: Vec<ColumnLineageItem> = Vec::new();
    let mut errors: Vec<String> = Vec::new();

    for col_name in &col_names {
        match pgsql::lineage::lineage(col_name, expr, Some(dialect_type), false) {
            Ok(node) => {
                let mut upstream = collect_leaf_upstream(&node);
                upstream.sort_by(|a, b| a.column.to_lowercase().cmp(&b.column.to_lowercase()));
                upstream.dedup_by(|a, b| a.column == b.column && a.table == b.table);
                columns.push(ColumnLineageItem {
                    name: col_name.clone(),
                    upstream,
                    r#type: String::new(),
                });
            }
            Err(e) => {
                errors.push(format!("lineage error for column {col_name}: {e}"));
            }
        }
    }

    columns.sort_by(|a, b| a.name.cmp(&b.name));

    json_result(&LineageResponse {
        columns,
        non_selected_columns: vec![],
        errors,
    })
}

fn collect_leaf_upstream(node: &pgsql::lineage::LineageNode) -> Vec<UpstreamColumn> {
    if node.downstream.is_empty() {
        // Leaf node – extract table and column
        let source_name = if !node.source_name.is_empty() {
            node.source_name.clone()
        } else {
            // Try to get table name from the name field (format: "table.column")
            let parts: Vec<&str> = node.name.split('.').collect();
            if parts.len() >= 2 {
                parts[..parts.len() - 1].join(".")
            } else {
                String::new()
            }
        };

        let column_name = {
            let parts: Vec<&str> = node.name.split('.').collect();
            parts.last().unwrap_or(&"").to_string()
        };

        if source_name.is_empty() && column_name.is_empty() {
            return vec![];
        }

        vec![UpstreamColumn {
            column: column_name,
            table: source_name,
        }]
    } else {
        let mut result = Vec::new();
        for child in &node.downstream {
            result.extend(collect_leaf_upstream(child));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    fn c(s: &str) -> *const c_char {
        CString::new(s).unwrap().into_raw() as *const c_char
    }

    fn read_and_free(ptr: *mut c_char) -> String {
        assert!(!ptr.is_null());
        let s = unsafe { CStr::from_ptr(ptr) }.to_str().unwrap().to_string();
        polyglot_free_string(ptr);
        s
    }

    #[test]
    fn test_get_tables_simple() {
        let exprs =
            pgsql::parse("SELECT a, b FROM table1 JOIN table2 ON a = b", DialectType::BigQuery)
                .unwrap();
        for expr in &exprs {
            let tables = pgsql::traversal::get_tables(expr);
            eprintln!("traversal tables count: {}", tables.len());
            for t in &tables {
                let gen = pgsql::generate(t, DialectType::BigQuery).unwrap();
                eprintln!("  table expr: {}", gen);
            }
        }
    }

    #[test]
    fn test_get_tables_ffi() {
        let result = read_and_free(polyglot_get_tables(
            c("SELECT a, b FROM table1 JOIN table2 ON a = b"),
            c("bigquery"),
        ));
        eprintln!("result: {result}");
        let resp: TablesResponse = serde_json::from_str(&result).unwrap();
        assert!(resp.error.is_none(), "error: {:?}", resp.error);
        assert_eq!(resp.tables.len(), 2, "tables: {:?}", resp.tables);
    }

    #[test]
    fn test_get_tables_bigquery_backticks() {
        let result = read_and_free(polyglot_get_tables(
            c("SELECT a, b FROM `project.dataset.table1` JOIN `project.dataset.table2` ON a = b"),
            c("bigquery"),
        ));
        eprintln!("bq result: {result}");
        let resp: TablesResponse = serde_json::from_str(&result).unwrap();
        assert!(resp.error.is_none(), "error: {:?}", resp.error);
        assert_eq!(resp.tables.len(), 2, "tables: {:?}", resp.tables);
    }

    #[test]
    fn test_is_single_select() {
        let result = read_and_free(polyglot_is_single_select(c("SELECT 1"), c("bigquery")));
        let resp: SingleSelectResponse = serde_json::from_str(&result).unwrap();
        assert!(resp.is_single_select);

        // Multi-statement
        let result2 = read_and_free(polyglot_is_single_select(
            c("SELECT 1; SELECT 2"),
            c("bigquery"),
        ));
        let resp2: SingleSelectResponse = serde_json::from_str(&result2).unwrap();
        assert!(!resp2.is_single_select);
    }

    #[test]
    fn test_add_limit() {
        let req = serde_json::to_string(&serde_json::json!({
            "query": "SELECT a FROM table1",
            "limit": 10,
            "dialect": "bigquery"
        }))
        .unwrap();
        let result = read_and_free(polyglot_add_limit(c(&req)));
        eprintln!("add_limit result: {result}");
        let resp: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(resp.get("error").is_none() || resp["error"].is_null());
        let query = resp["query"].as_str().unwrap().to_uppercase();
        assert!(query.contains("LIMIT"), "query should contain LIMIT: {query}");
    }

    #[test]
    fn test_rename_tables() {
        let req = serde_json::to_string(&serde_json::json!({
            "query": "SELECT a FROM old_table",
            "dialect": "bigquery",
            "table_mapping": {"old_table": "new_table"}
        }))
        .unwrap();
        let result = read_and_free(polyglot_rename_tables(c(&req)));
        eprintln!("rename result: {result}");
        let resp: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(resp.get("error").is_none() || resp["error"].is_null());
        let query = resp["query"].as_str().unwrap();
        assert!(
            query.contains("new_table"),
            "query should contain new_table: {query}"
        );
    }

    #[test]
    fn test_column_lineage() {
        let req = serde_json::to_string(&serde_json::json!({
            "query": "SELECT a, b FROM table1",
            "dialect": "bigquery",
            "schema": {}
        }))
        .unwrap();
        let result = read_and_free(polyglot_column_lineage(c(&req)));
        eprintln!("lineage result: {result}");
        let resp: LineageResponse = serde_json::from_str(&result).unwrap();
        eprintln!("columns: {:?}", resp.columns);
        eprintln!("errors: {:?}", resp.errors);
        // Should have at least parsed without panicking
        assert!(resp.columns.len() >= 1 || !resp.errors.is_empty());
    }
}

#[cfg(test)]
mod bench_tests {
    use super::*;
    use std::time::Instant;

    const BENCH_QUERY: &str = r#"
WITH daily_revenue AS (
    SELECT
        DATE(o.created_at) AS order_date,
        p.category_id,
        c.category_name,
        SUM(oi.quantity * oi.unit_price) AS revenue,
        COUNT(DISTINCT o.order_id) AS num_orders,
        COUNT(DISTINCT o.customer_id) AS num_customers,
        AVG(oi.quantity * oi.unit_price) AS avg_order_value
    FROM `project.dataset.orders` o
    JOIN `project.dataset.order_items` oi ON o.order_id = oi.order_id
    JOIN `project.dataset.products` p ON oi.product_id = p.product_id
    JOIN `project.dataset.categories` c ON p.category_id = c.category_id
    WHERE o.created_at >= '2024-01-01'
      AND o.status NOT IN ('cancelled', 'refunded')
    GROUP BY 1, 2, 3
),
customer_segments AS (
    SELECT
        cs.customer_id,
        cs.segment_name,
        cs.lifetime_value,
        cs.first_purchase_date,
        cs.last_purchase_date
    FROM `project.dataset.customer_segments` cs
    WHERE cs.is_active = TRUE
),
product_performance AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category_id,
        COUNT(*) AS times_ordered,
        SUM(oi.quantity) AS total_quantity,
        SUM(oi.quantity * oi.unit_price) AS total_revenue,
        AVG(r.rating) AS avg_rating,
        COUNT(r.review_id) AS num_reviews
    FROM `project.dataset.products` p
    JOIN `project.dataset.order_items` oi ON p.product_id = oi.product_id
    LEFT JOIN `project.dataset.reviews` r ON p.product_id = r.product_id
    GROUP BY 1, 2, 3
)
SELECT
    dr.order_date,
    dr.category_name,
    dr.revenue,
    dr.num_orders,
    dr.num_customers,
    dr.avg_order_value,
    pp.product_name AS top_product,
    pp.total_revenue AS product_revenue,
    pp.avg_rating,
    pp.num_reviews,
    COALESCE(seg_stats.segment_orders, 0) AS premium_segment_orders,
    LAG(dr.revenue) OVER (PARTITION BY dr.category_id ORDER BY dr.order_date) AS prev_day_revenue,
    SAFE_DIVIDE(dr.revenue - LAG(dr.revenue) OVER (PARTITION BY dr.category_id ORDER BY dr.order_date),
                LAG(dr.revenue) OVER (PARTITION BY dr.category_id ORDER BY dr.order_date)) AS revenue_growth
FROM daily_revenue dr
LEFT JOIN product_performance pp ON dr.category_id = pp.category_id
LEFT JOIN (
    SELECT
        DATE(o.created_at) AS order_date,
        COUNT(DISTINCT o.order_id) AS segment_orders
    FROM `project.dataset.orders` o
    JOIN customer_segments cs ON o.customer_id = cs.customer_id
    WHERE cs.segment_name = 'Premium'
    GROUP BY 1
) seg_stats ON dr.order_date = seg_stats.order_date
ORDER BY dr.order_date DESC, dr.revenue DESC
    "#;

    #[test]
    fn bench_get_tables() {
        let iterations = 1000;
        let start = Instant::now();
        for _ in 0..iterations {
            let exprs = pgsql::parse(BENCH_QUERY, DialectType::BigQuery).unwrap();
            for expr in &exprs {
                let scope = pgsql::scope::build_scope(expr);
                let _ = scope.source_names();
            }
        }
        let elapsed = start.elapsed();
        let per_op = elapsed / iterations;
        eprintln!("Pure Rust get_tables: {iterations} iterations in {:?} ({:?}/op)", elapsed, per_op);
    }

    #[test]
    fn bench_column_lineage() {
        let iterations = 500;
        let start = Instant::now();
        for _ in 0..iterations {
            let exprs = pgsql::parse(BENCH_QUERY, DialectType::BigQuery).unwrap();
            if let Some(expr) = exprs.first() {
                let col_names = pgsql::ast_transforms::get_column_names(expr);
                for col_name in &col_names {
                    let _ = pgsql::lineage::lineage(col_name, expr, Some(DialectType::BigQuery), false);
                }
            }
        }
        let elapsed = start.elapsed();
        let per_op = elapsed / iterations;
        eprintln!("Pure Rust column_lineage: {iterations} iterations in {:?} ({:?}/op)", elapsed, per_op);
    }

    #[test]
    fn bench_parse_generate() {
        let iterations = 1000;
        let start = Instant::now();
        for _ in 0..iterations {
            let exprs = pgsql::parse(BENCH_QUERY, DialectType::BigQuery).unwrap();
            for expr in &exprs {
                let _ = pgsql::generate(expr, DialectType::BigQuery).unwrap();
            }
        }
        let elapsed = start.elapsed();
        let per_op = elapsed / iterations;
        eprintln!("Pure Rust parse+generate: {iterations} iterations in {:?} ({:?}/op)", elapsed, per_op);
    }
}
