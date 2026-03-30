use polyglot_sql::lineage::{lineage, lineage_with_schema};
use polyglot_sql::validation::{
    mapping_schema_from_validation_schema, SchemaColumn, SchemaTable, ValidationSchema,
};
use polyglot_sql::{generate, parse, parse_one, DialectType, Expression};
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

type SchemaLookup = HashMap<String, HashMap<String, String>>;

#[derive(Debug, Clone, Serialize)]
struct UpstreamColumn {
    column: String,
    table: String,
}

#[derive(Debug, Clone, Serialize)]
struct ColumnLineage {
    name: String,
    upstream: Vec<UpstreamColumn>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    data_type: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct LineageResponse {
    columns: Vec<ColumnLineage>,
    non_selected_columns: Vec<ColumnLineage>,
    errors: Vec<String>,
}

#[derive(Debug, Clone)]
struct SelectedOutput {
    name: String,
    expr: Option<Value>,
}

// Core functions called directly from FFI entry points in lib.rs.

fn expression_to_value(expression: &Expression) -> Value {
    serde_json::to_value(expression).expect("expression should serialize")
}

fn value_to_expression(value: &Value) -> Result<Expression, String> {
    serde_json::from_value(value.clone()).map_err(|err| err.to_string())
}

fn empty_object() -> Value {
    Value::Object(Map::new())
}

fn node_kind(node: &Value) -> Option<String> {
    node.as_object().and_then(|object| {
        if object.len() == 1 {
            object.keys().next().cloned()
        } else {
            None
        }
    })
}

fn identifier_name(node: Option<&Value>) -> Option<String> {
    node.and_then(Value::as_object)
        .and_then(|object| object.get("name"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .filter(|value| !value.is_empty())
}

fn has_truthy_value(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Null) | None => false,
        Some(Value::Bool(value)) => *value,
        Some(Value::Array(values)) => !values.is_empty(),
        Some(Value::Object(values)) => !values.is_empty(),
        Some(Value::String(value)) => !value.is_empty(),
        Some(_) => true,
    }
}

fn collect_wrappers(node: &Value, key: &str, out: &mut Vec<Value>) {
    match node {
        Value::Object(object) => {
            if object.len() == 1 {
                if let Some(value) = object.get(key) {
                    out.push(value.clone());
                }
            }
            for value in object.values() {
                collect_wrappers(value, key, out);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_wrappers(value, key, out);
            }
        }
        _ => {}
    }
}

fn collect_values(node: &Value, key: &str, out: &mut Vec<Value>) {
    match node {
        Value::Object(object) => {
            if let Some(value) = object.get(key) {
                out.push(value.clone());
            }
            for value in object.values() {
                collect_values(value, key, out);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_values(value, key, out);
            }
        }
        _ => {}
    }
}

fn first_wrapper(node: &Value, key: &str) -> Option<Value> {
    let mut values = Vec::new();
    collect_wrappers(node, key, &mut values);
    values.into_iter().next()
}

fn get_table_name_obj(table: &Value) -> String {
    let mut parts = Vec::new();
    if let Some(catalog) = identifier_name(table.get("catalog")) {
        parts.push(catalog);
    }
    if let Some(schema) = identifier_name(table.get("schema")) {
        parts.push(schema);
    }
    if let Some(name) = identifier_name(table.get("name")) {
        parts.push(name);
    }
    parts.join(".")
}

fn get_table_name_with_context_obj(table: &Value, current_database: &str) -> String {
    let catalog = identifier_name(table.get("catalog"));
    let schema = identifier_name(table.get("schema"));
    let name = identifier_name(table.get("name"));

    let mut parts = Vec::new();
    if let Some(catalog) = catalog {
        parts.push(catalog);
        parts.push(schema.unwrap_or_else(|| "dbo".to_string()));
    } else {
        parts.push(current_database.to_string());
        parts.push(schema.unwrap_or_else(|| "dbo".to_string()));
    }
    if let Some(name) = name {
        parts.push(name);
    }
    parts.join(".")
}

fn flatten_simple_schema(
    schema: &Value,
    prefix: &[String],
    out: &mut HashMap<String, HashMap<String, String>>,
) {
    let Some(object) = schema.as_object() else {
        return;
    };

    if !object.is_empty() && object.values().all(Value::is_string) {
        let mut columns = HashMap::new();
        for (column, data_type) in object {
            if let Some(data_type) = data_type.as_str() {
                columns.insert(column.clone(), data_type.to_string());
            }
        }
        out.insert(prefix.join("."), columns);
        return;
    }

    for (key, value) in object {
        if value.is_object() {
            let mut next_prefix = prefix.to_vec();
            next_prefix.push(key.clone());
            flatten_simple_schema(value, &next_prefix, out);
        }
    }
}

fn flatten_schema(schema: &Value) -> HashMap<String, HashMap<String, String>> {
    let mut flattened = HashMap::new();
    flatten_simple_schema(schema, &[], &mut flattened);
    flattened
}

fn simple_schema_lookup(schema: &Value) -> SchemaLookup {
    flatten_schema(schema)
        .into_iter()
        .map(|(table, columns)| {
            (
                table.to_lowercase(),
                columns
                    .into_iter()
                    .map(|(column, data_type)| (column.to_lowercase(), data_type))
                    .collect(),
            )
        })
        .collect()
}

fn simple_schema_to_validation(schema: &Value) -> ValidationSchema {
    let tables = flatten_schema(schema)
        .into_iter()
        .map(|(table_path, columns)| {
            let parts: Vec<_> = table_path.split('.').collect();
            let name = parts.last().copied().unwrap_or_default().to_string();
            let schema_name = if parts.len() > 1 {
                Some(parts[..parts.len() - 1].join("."))
            } else {
                None
            };

            SchemaTable {
                name,
                schema: schema_name,
                columns: columns
                    .into_iter()
                    .map(|(column_name, data_type)| SchemaColumn {
                        name: column_name,
                        data_type,
                        nullable: None,
                        primary_key: false,
                        unique: false,
                        references: None,
                    })
                    .collect(),
                aliases: Vec::new(),
                primary_key: Vec::new(),
                unique_keys: Vec::new(),
                foreign_keys: Vec::new(),
            }
        })
        .collect();

    ValidationSchema {
        tables,
        strict: None,
    }
}

fn is_case_insensitive_column_dialect(dialect: DialectType) -> bool {
    matches!(dialect, DialectType::BigQuery | DialectType::Snowflake)
}

fn align_schema_casing(schema: &Value, parsed: &Value) -> Value {
    let mut query_table_names = BTreeSet::new();
    let mut tables = Vec::new();
    collect_wrappers(parsed, "table", &mut tables);
    for table in tables {
        let name = get_table_name_obj(&table);
        if !name.is_empty() {
            query_table_names.insert(name);
        }
    }

    let lower_to_query: HashMap<String, String> = query_table_names
        .into_iter()
        .map(|name| (name.to_lowercase(), name))
        .collect();

    let mut aligned = schema.clone();
    if let (Some(schema_object), Some(aligned_object)) =
        (schema.as_object(), aligned.as_object_mut())
    {
        for (key, value) in schema_object {
            if let Some(query_key) = lower_to_query.get(&key.to_lowercase()) {
                if !aligned_object.contains_key(query_key) {
                    aligned_object.insert(query_key.clone(), value.clone());
                }
            }
        }
    }

    aligned
}

fn normalize_schema_columns(schema: &Value, dialect: DialectType) -> Value {
    let Some(object) = schema.as_object() else {
        return schema.clone();
    };

    let mut result = Map::new();
    for (key, value) in object {
        if let Some(columns) = value.as_object() {
            if !columns.is_empty() && columns.values().all(Value::is_string) {
                if is_case_insensitive_column_dialect(dialect) {
                    let lowered = columns
                        .iter()
                        .map(|(column, value)| (column.to_lowercase(), value.clone()))
                        .collect();
                    result.insert(key.clone(), Value::Object(lowered));
                } else {
                    result.insert(key.clone(), value.clone());
                }
                continue;
            }
        }

        if value.is_object() {
            result.insert(key.clone(), normalize_schema_columns(value, dialect));
        } else {
            result.insert(key.clone(), value.clone());
        }
    }

    Value::Object(result)
}

fn transparent_select_source(select: &Value) -> Option<Value> {
    let select_object = select.as_object()?;
    let expressions = select_object
        .get("expressions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if expressions.is_empty() {
        return None;
    }

    for expr in &expressions {
        if node_kind(expr).as_deref() != Some("star") {
            return None;
        }
        let star = expr.get("star").and_then(Value::as_object)?;
        if star.get("table").is_some_and(|value| !value.is_null())
            || star.get("except").is_some_and(|value| !value.is_null())
            || star.get("replace").is_some_and(|value| !value.is_null())
            || star.get("rename").is_some_and(|value| !value.is_null())
        {
            return None;
        }
    }

    if select_object
        .get("joins")
        .is_some_and(|value| !value.as_array().unwrap_or(&Vec::new()).is_empty())
        || select_object
            .get("lateral_views")
            .is_some_and(|value| !value.as_array().unwrap_or(&Vec::new()).is_empty())
    {
        return None;
    }

    for key in [
        "where_clause",
        "group_by",
        "having",
        "qualify",
        "order_by",
        "distribute_by",
        "cluster_by",
        "sort_by",
        "limit",
        "offset",
        "fetch",
        "with",
        "sample",
        "windows",
        "hint",
        "connect",
        "into",
    ] {
        if has_truthy_value(select_object.get(key)) {
            return None;
        }
    }

    for key in ["distinct", "distinct_on", "top", "locks"] {
        if has_truthy_value(select_object.get(key)) {
            return None;
        }
    }

    let from_clause = select_object.get("from")?.as_object()?;
    let sources = from_clause
        .get("expressions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if sources.len() != 1 {
        return None;
    }

    sources.into_iter().next()
}

fn simplify_transparent_subqueries(node: &Value) -> Value {
    match node {
        Value::Array(values) => {
            Value::Array(values.iter().map(simplify_transparent_subqueries).collect())
        }
        Value::Object(object) => {
            if let Some(subquery_value) = object.get("subquery") {
                if object.len() == 1 {
                    let subquery = simplify_transparent_subqueries(subquery_value);
                    let subquery_object = subquery.as_object().cloned().unwrap_or_default();
                    if let Some(inner) = subquery_object.get("this") {
                        if node_kind(inner).as_deref() == Some("select") {
                            if let Some(select_object) = inner.get("select") {
                                if let Some(mut source) = transparent_select_source(select_object) {
                                    source = simplify_transparent_subqueries(&source);
                                    let source_kind = node_kind(&source);
                                    if matches!(source_kind.as_deref(), Some("table" | "subquery"))
                                    {
                                        if let Some(alias) = subquery_object.get("alias").cloned() {
                                            if !alias.is_null() {
                                                if let Some(source_object) = source
                                                    .as_object_mut()
                                                    .and_then(|object| {
                                                        source_kind
                                                            .as_deref()
                                                            .and_then(|kind| object.get_mut(kind))
                                                    })
                                                    .and_then(Value::as_object_mut)
                                                {
                                                    source_object
                                                        .insert("alias".to_string(), alias);
                                                }
                                            }
                                        }
                                    }
                                    return source;
                                }
                            }
                        }
                    }
                    return json!({ "subquery": subquery });
                }
            }

            let mapped = object
                .iter()
                .map(|(key, value)| (key.clone(), simplify_transparent_subqueries(value)))
                .collect();
            Value::Object(mapped)
        }
        _ => node.clone(),
    }
}

fn transform_columns_for_lineage(node: &Value, dialect: DialectType) -> Value {
    match node {
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|value| transform_columns_for_lineage(value, dialect))
                .collect(),
        ),
        Value::Object(object) => {
            let kind = node_kind(node);
            if kind.as_deref() == Some("column") {
                let mut column = object
                    .get("column")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                for value in column.values_mut() {
                    *value = transform_columns_for_lineage(value, dialect);
                }
                if is_case_insensitive_column_dialect(dialect) {
                    if let Some(name) = column.get_mut("name").and_then(Value::as_object_mut) {
                        if !name.get("quoted").and_then(Value::as_bool).unwrap_or(false) {
                            if let Some(current) = name.get("name").and_then(Value::as_str) {
                                name.insert(
                                    "name".to_string(),
                                    Value::String(current.to_lowercase()),
                                );
                            }
                        }
                    }
                }
                return json!({ "column": column });
            }

            if kind.as_deref() == Some("function") {
                let mut function = object
                    .get("function")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                for value in function.values_mut() {
                    *value = transform_columns_for_lineage(value, dialect);
                }

                if dialect == DialectType::Snowflake
                    && function
                        .get("name")
                        .and_then(Value::as_str)
                        .is_some_and(|name| name.eq_ignore_ascii_case("datediff"))
                {
                    if let Some(args) = function.get_mut("args").and_then(Value::as_array_mut) {
                        if let Some(first_arg) = args.first_mut() {
                            if node_kind(first_arg).as_deref() == Some("column") {
                                let datepart = first_arg
                                    .get("column")
                                    .and_then(Value::as_object)
                                    .and_then(|column| {
                                        if column.get("table").is_some_and(|value| !value.is_null())
                                        {
                                            return None;
                                        }
                                        identifier_name(column.get("name"))
                                    });
                                if let Some(datepart) = datepart {
                                    *first_arg = json!({
                                        "literal": {
                                            "literal_type": "string",
                                            "value": datepart.to_lowercase(),
                                        }
                                    });
                                }
                            }
                        }
                    }
                }
                return json!({ "function": function });
            }

            let mapped = object
                .iter()
                .map(|(key, value)| (key.clone(), transform_columns_for_lineage(value, dialect)))
                .collect();
            Value::Object(mapped)
        }
        _ => node.clone(),
    }
}

fn prepare_lineage_inputs(
    schema: &Value,
    dialect: DialectType,
    parsed: &Value,
) -> Result<(String, Value, Value), String> {
    let aligned_schema = align_schema_casing(schema, parsed);
    let prepared_schema = normalize_schema_columns(&aligned_schema, dialect);

    let prepared_ast =
        transform_columns_for_lineage(&simplify_transparent_subqueries(parsed), dialect);
    let prepared_expression = value_to_expression(&prepared_ast)?;
    let prepared_query = generate(&prepared_expression, dialect).map_err(|err| err.to_string())?;

    Ok((prepared_query, prepared_schema, prepared_ast))
}

fn select_expressions(ast_node: &Value) -> Vec<Value> {
    first_wrapper(ast_node, "select")
        .and_then(|select| {
            select
                .as_object()
                .and_then(|object| object.get("expressions"))
                .and_then(Value::as_array)
                .cloned()
        })
        .unwrap_or_default()
}

fn output_name(expr: &Value) -> Option<String> {
    match node_kind(expr).as_deref() {
        Some("alias") => expr
            .get("alias")
            .and_then(Value::as_object)
            .and_then(|alias| identifier_name(alias.get("alias"))),
        Some("column") => expr
            .get("column")
            .and_then(Value::as_object)
            .and_then(|column| identifier_name(column.get("name"))),
        Some("star") => Some("*".to_string()),
        Some("identifier") => expr
            .get("identifier")
            .and_then(|identifier| identifier_name(Some(identifier))),
        _ => None,
    }
}

fn expanded_select_outputs(ast_node: &Value, schema: &Value) -> Vec<SelectedOutput> {
    let tables = collect_real_tables(ast_node);
    let schema_lookup = simple_schema_lookup(schema);

    let mut source_order = Vec::new();
    let mut alias_to_table = HashMap::new();
    for table in tables {
        let actual = get_table_name_obj(&table);
        if !actual.is_empty() {
            source_order.push(actual.clone());
        }

        if let Some(alias) = identifier_name(table.get("alias")) {
            alias_to_table.insert(alias.to_lowercase(), actual.clone());
        }
        if let Some(short_name) = identifier_name(table.get("name")) {
            alias_to_table.insert(short_name.to_lowercase(), actual.clone());
        }
    }

    let mut results = Vec::new();
    let mut seen_star_columns = HashSet::new();
    for expr in select_expressions(ast_node) {
        if node_kind(&expr).as_deref() != Some("star") {
            if let Some(name) = output_name(&expr) {
                results.push(SelectedOutput {
                    name,
                    expr: Some(expr),
                });
            }
            continue;
        }

        let qualifier = expr
            .get("star")
            .and_then(Value::as_object)
            .and_then(|star| identifier_name(star.get("table")));

        let table_names = if let Some(qualifier) = qualifier {
            alias_to_table
                .get(&qualifier.to_lowercase())
                .cloned()
                .map(|value| vec![value])
                .unwrap_or_default()
        } else {
            source_order.clone()
        };

        for table_name in table_names {
            if let Some(columns) = schema_lookup.get(&table_name.to_lowercase()) {
                for column_name in columns.keys() {
                    if !seen_star_columns.insert(column_name.to_lowercase()) {
                        continue;
                    }
                    results.push(SelectedOutput {
                        name: column_name.clone(),
                        expr: None,
                    });
                }
            }
        }
    }

    results
}

fn inferred_type_from_expr(expr: &Value) -> Option<Value> {
    match node_kind(expr).as_deref() {
        Some("alias") => {
            let alias = expr.get("alias")?.as_object()?;
            if let Some(inferred) = alias.get("inferred_type") {
                if !inferred.is_null() {
                    return Some(inferred.clone());
                }
            }
            let inner = alias.get("this")?;
            let inner_kind = node_kind(inner)?;
            inner
                .get(&inner_kind)
                .and_then(Value::as_object)
                .and_then(|object| object.get("inferred_type"))
                .cloned()
        }
        Some(kind) => expr
            .get(kind)
            .and_then(Value::as_object)
            .and_then(|object| object.get("inferred_type"))
            .cloned(),
        None => None,
    }
}

fn inner_expression_kind(expr: &Value) -> (Option<String>, Option<String>) {
    match node_kind(expr).as_deref() {
        Some("alias") => {
            let inner = expr
                .get("alias")
                .and_then(Value::as_object)
                .and_then(|alias| alias.get("this"));
            let inner_kind = inner.and_then(node_kind);
            let function_name = if inner_kind.as_deref() == Some("function") {
                inner.and_then(|inner| {
                    inner
                        .get("function")
                        .and_then(Value::as_object)
                        .and_then(|function| function.get("name"))
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                })
            } else {
                None
            };
            (inner_kind, function_name)
        }
        Some("function") => (
            Some("function".to_string()),
            expr.get("function")
                .and_then(Value::as_object)
                .and_then(|function| function.get("name"))
                .and_then(Value::as_str)
                .map(ToString::to_string),
        ),
        kind => (kind.map(ToString::to_string), None),
    }
}

fn normalize_identifier(name: &str, dialect: DialectType, kind: &str) -> String {
    match dialect {
        DialectType::Snowflake => name.to_uppercase(),
        DialectType::BigQuery if matches!(kind, "column" | "output") => name.to_lowercase(),
        _ if matches!(kind, "column" | "output") => name.to_lowercase(),
        _ => name.to_string(),
    }
}

fn schema_type_to_sqlglot(type_name: Option<&str>) -> Option<String> {
    let normalized = type_name?
        .trim()
        .trim_matches(|c| matches!(c, '\'' | '"' | '`'))
        .to_lowercase();

    match normalized.as_str() {
        "str" | "string" | "text" | "varchar" | "char" => Some("TEXT".to_string()),
        "int64" | "bigint" | "bigserial" => Some("BIGINT".to_string()),
        "int" | "integer" | "int32" | "smallint" | "tinyint" => Some("INT".to_string()),
        "float32" | "float" => Some("FLOAT".to_string()),
        "float64" | "double" | "double precision" | "numeric" | "decimal" | "number" => {
            Some("DOUBLE".to_string())
        }
        "bool" | "boolean" => Some("BOOLEAN".to_string()),
        "date" => Some("DATE".to_string()),
        "timestamp" | "datetime" => Some("TIMESTAMP".to_string()),
        _ => None,
    }
}

fn schema_fallback_type(upstream: &[UpstreamColumn], aligned_schema: &Value) -> Option<String> {
    if upstream.is_empty() {
        return None;
    }

    let schema_lookup = simple_schema_lookup(aligned_schema);
    let mut mapped = Vec::new();
    for item in upstream {
        let schema_type = schema_lookup
            .get(&item.table.to_lowercase())
            .and_then(|columns| columns.get(&item.column.to_lowercase()))
            .and_then(|value| schema_type_to_sqlglot(Some(value)));
        let schema_type = schema_type?;
        mapped.push(schema_type);
    }

    if mapped.iter().all(|value| value == &mapped[0]) {
        mapped.into_iter().next()
    } else {
        None
    }
}

fn format_type(inferred_type: Option<&Value>, expr: &Value, has_upstream: bool) -> String {
    let Some(inferred_type) = inferred_type.and_then(Value::as_object) else {
        return "UNKNOWN".to_string();
    };
    let Some(data_type) = inferred_type.get("data_type").and_then(Value::as_str) else {
        return "UNKNOWN".to_string();
    };

    match data_type {
        "unknown" => "UNKNOWN".to_string(),
        "big_int" => "BIGINT".to_string(),
        "int" | "integer" | "small_int" | "tiny_int" => "INT".to_string(),
        "double" => "DOUBLE".to_string(),
        "float" => "FLOAT".to_string(),
        "date" => "DATE".to_string(),
        "timestamp" => "TIMESTAMP".to_string(),
        "boolean" | "bool" => "BOOLEAN".to_string(),
        "decimal" => "DECIMAL".to_string(),
        "var_char" | "char" | "string" | "text" => {
            let mut inner_kind = node_kind(expr);
            if inner_kind.as_deref() == Some("alias") {
                inner_kind = expr
                    .get("alias")
                    .and_then(Value::as_object)
                    .and_then(|alias| alias.get("this"))
                    .and_then(node_kind);
            }
            if inner_kind.as_deref() == Some("column") && has_upstream {
                "TEXT".to_string()
            } else {
                "VARCHAR".to_string()
            }
        }
        value => value.to_uppercase(),
    }
}

fn literal_branch_type(expr: Option<&Value>) -> Option<String> {
    let expr = expr?;
    if node_kind(expr).as_deref() != Some("literal") {
        return None;
    }
    let literal = expr.get("literal")?.as_object()?;
    if literal.get("literal_type").and_then(Value::as_str) != Some("number") {
        return None;
    }
    let value = literal.get("value")?.as_str()?;
    if value.contains('.') {
        Some("FLOAT".to_string())
    } else {
        Some("INT".to_string())
    }
}

fn compat_expression_type(expr: Option<&Value>) -> Option<String> {
    let expr = expr?;
    let mut target = expr;
    let mut kind = node_kind(expr);
    if kind.as_deref() == Some("alias") {
        target = expr
            .get("alias")
            .and_then(Value::as_object)
            .and_then(|alias| alias.get("this"))?;
        kind = node_kind(target);
    }

    match kind.as_deref() {
        Some("if_func") => {
            let if_func = target.get("if_func")?.as_object()?;
            let true_type = literal_branch_type(if_func.get("true_value"));
            let false_type = literal_branch_type(if_func.get("false_value"));
            if true_type.is_some() && true_type == false_type {
                true_type
            } else {
                None
            }
        }
        Some("literal") => {
            let literal = target.get("literal")?.as_object()?;
            if literal.get("literal_type").and_then(Value::as_str) == Some("string") {
                Some("TEXT".to_string())
            } else {
                literal_branch_type(Some(target))
            }
        }
        Some("coalesce") => {
            let expressions = target
                .get("coalesce")
                .and_then(Value::as_object)
                .and_then(|coalesce| coalesce.get("expressions"))
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let expression_types: Vec<_> = expressions
                .iter()
                .filter_map(|value| compat_expression_type(Some(value)))
                .collect();
            if !expression_types.is_empty()
                && expression_types
                    .iter()
                    .all(|value| value == &expression_types[0])
            {
                Some(expression_types[0].clone())
            } else {
                None
            }
        }
        Some("case") => {
            let case = target.get("case")?.as_object()?;
            let mut branch_types = Vec::new();
            for when in case
                .get("whens")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
            {
                if let Some(values) = when.as_array() {
                    if values.len() == 2 {
                        if let Some(branch_type) = compat_expression_type(values.get(1)) {
                            branch_types.push(branch_type);
                        }
                    }
                }
            }
            if let Some(else_type) = compat_expression_type(case.get("else_")) {
                branch_types.push(else_type);
            }
            if !branch_types.is_empty()
                && branch_types.iter().all(|value| value == &branch_types[0])
            {
                Some(branch_types[0].clone())
            } else {
                None
            }
        }
        Some("max") | Some("min") => compat_expression_type(
            target
                .get(kind.as_deref().unwrap())
                .and_then(Value::as_object)
                .and_then(|node| node.get("this")),
        ),
        Some("cast") => {
            let cast_to = target
                .get("cast")
                .and_then(Value::as_object)
                .and_then(|cast| cast.get("to"))
                .and_then(Value::as_object)?;
            let cast_name = cast_to
                .get("name")
                .and_then(Value::as_str)
                .or_else(|| cast_to.get("data_type").and_then(Value::as_str))
                .unwrap_or_default()
                .to_lowercase();
            match cast_name.as_str() {
                "int" | "integer" => Some("INT".to_string()),
                "int64" | "bigint" | "big_int" => Some("BIGINT".to_string()),
                "string" | "text" | "varchar" | "char" => Some("TEXT".to_string()),
                _ => None,
            }
        }
        _ => None,
    }
}

fn output_name_matches(left: Option<&str>, right: Option<&str>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.eq_ignore_ascii_case(right),
        _ => false,
    }
}

fn query_source_wrappers(ast_node: &Value) -> Vec<Value> {
    let Some(select) = first_wrapper(ast_node, "select") else {
        return Vec::new();
    };
    let Some(select_object) = select.as_object() else {
        return Vec::new();
    };

    let mut sources = Vec::new();
    if let Some(from_clause) = select_object.get("from").and_then(Value::as_object) {
        if let Some(expressions) = from_clause.get("expressions").and_then(Value::as_array) {
            sources.extend(expressions.iter().cloned());
        }
    }

    if let Some(joins) = select_object.get("joins").and_then(Value::as_array) {
        for join in joins {
            if let Some(this) = join.as_object().and_then(|join| join.get("this")) {
                sources.push(this.clone());
            }
        }
    }

    sources
}

fn source_lookup_keys(source: &Value) -> Vec<String> {
    match node_kind(source).as_deref() {
        Some("table") => {
            let table = source.get("table").and_then(Value::as_object);
            let alias = table.and_then(|table| identifier_name(table.get("alias")));
            let actual = table.map(|table| get_table_name_obj(&Value::Object(table.clone())));
            let short_name = table.and_then(|table| identifier_name(table.get("name")));
            [alias, actual, short_name].into_iter().flatten().collect()
        }
        Some("subquery") => source
            .get("subquery")
            .and_then(Value::as_object)
            .and_then(|subquery| identifier_name(subquery.get("alias")))
            .map(|alias| vec![alias])
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn find_source_wrapper(ast_node: &Value, name: &str) -> Option<Value> {
    let target = name.to_lowercase();
    for source in query_source_wrappers(ast_node) {
        if source_lookup_keys(&source)
            .into_iter()
            .any(|key| key.to_lowercase() == target)
        {
            return Some(source);
        }
    }
    None
}

fn find_cte_query(ast_node: &Value, name: &str) -> Option<Value> {
    let target = name.to_lowercase();
    let mut with_clauses = Vec::new();
    collect_values(ast_node, "with", &mut with_clauses);
    for with_clause in with_clauses {
        let Some(with_object) = with_clause.as_object() else {
            continue;
        };
        let Some(ctes) = with_object.get("ctes").and_then(Value::as_array) else {
            continue;
        };
        for cte in ctes {
            let Some(alias) = cte
                .as_object()
                .and_then(|cte| identifier_name(cte.get("alias")))
            else {
                continue;
            };
            if alias.to_lowercase() == target {
                if let Some(query) = cte.as_object().and_then(|cte| cte.get("this")) {
                    return Some(query.clone());
                }
            }
        }
    }
    None
}

fn find_output_expression(ast_node: &Value, name: &str) -> Option<Value> {
    for expr in select_expressions(ast_node) {
        if output_name_matches(output_name(&expr).as_deref(), Some(name)) {
            return Some(expr);
        }
    }
    None
}

fn direct_table_upstream(
    table_name: &str,
    column_name: &str,
    schema_lookup: &SchemaLookup,
    dialect: DialectType,
) -> (Vec<UpstreamColumn>, Option<String>) {
    let Some(columns) = schema_lookup.get(&table_name.to_lowercase()) else {
        return (Vec::new(), None);
    };
    if !columns.contains_key(&column_name.to_lowercase()) {
        return (Vec::new(), None);
    }
    (
        vec![UpstreamColumn {
            column: normalize_identifier(column_name, dialect, "column"),
            table: normalize_identifier(table_name, dialect, "table"),
        }],
        None,
    )
}

fn is_query_kind(kind: Option<&str>) -> bool {
    matches!(
        kind,
        Some("select" | "union" | "intersect" | "except" | "cte" | "subquery")
    )
}

fn collect_columns_in_scope(node: &Value, out: &mut Vec<Value>) {
    match node {
        Value::Object(_) => {
            let kind = node_kind(node);
            if kind.as_deref() == Some("column") {
                if let Some(column) = node.get("column") {
                    out.push(column.clone());
                }
                return;
            }
            if is_query_kind(kind.as_deref()) {
                return;
            }
            if let Some(object) = node.as_object() {
                for value in object.values() {
                    collect_columns_in_scope(value, out);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_columns_in_scope(value, out);
            }
        }
        _ => {}
    }
}

fn resolve_expression_upstreams(
    expr: Option<&Value>,
    query_ast: &Value,
    schema: &Value,
    dialect: DialectType,
    seen: &HashSet<(String, String)>,
) -> (Vec<UpstreamColumn>, Option<String>) {
    let Some(expr) = expr else {
        return (Vec::new(), None);
    };

    let schema_lookup = simple_schema_lookup(schema);
    let mut columns = Vec::new();
    collect_columns_in_scope(expr, &mut columns);

    let mut upstream = Vec::new();
    let mut virtual_types = Vec::new();
    for column in columns {
        let Some(column_object) = column.as_object() else {
            continue;
        };
        let name = identifier_name(column_object.get("name"));
        let table = identifier_name(column_object.get("table"));
        let Some(name) = name else {
            continue;
        };

        let (resolved, virtual_type) = if let Some(table) = table {
            resolve_source_column(
                &table,
                &name,
                query_ast,
                &schema_lookup,
                schema,
                dialect,
                seen,
            )
        } else {
            resolve_unqualified_source_column(
                &name,
                query_ast,
                &schema_lookup,
                schema,
                dialect,
                seen,
            )
        };

        upstream.extend(resolved);
        if let Some(virtual_type) = virtual_type {
            virtual_types.push(virtual_type);
        }
    }

    let mut seen_upstream = HashSet::new();
    upstream.retain(|item| seen_upstream.insert((item.table.clone(), item.column.clone())));
    upstream.sort_by(|left, right| {
        left.table
            .cmp(&right.table)
            .then_with(|| left.column.cmp(&right.column))
    });

    let virtual_type = if !virtual_types.is_empty()
        && virtual_types.iter().all(|value| value == &virtual_types[0])
    {
        Some(virtual_types[0].clone())
    } else {
        None
    };

    (upstream, virtual_type)
}

fn resolve_source_column(
    table_ref: &str,
    column_name: &str,
    query_ast: &Value,
    schema_lookup: &SchemaLookup,
    schema: &Value,
    dialect: DialectType,
    seen: &HashSet<(String, String)>,
) -> (Vec<UpstreamColumn>, Option<String>) {
    let Some(source) =
        find_source_wrapper(query_ast, table_ref).or_else(|| find_cte_query(query_ast, table_ref))
    else {
        return (Vec::new(), None);
    };

    match node_kind(&source).as_deref() {
        Some("table") => {
            let table = source.get("table").cloned().unwrap_or_default();
            let actual = get_table_name_obj(&table);
            let (direct, _) = direct_table_upstream(&actual, column_name, schema_lookup, dialect);
            if !direct.is_empty() {
                return (direct, None);
            }

            let actual_name = table
                .as_object()
                .and_then(|table| identifier_name(table.get("name")));
            if let Some(actual_name) = actual_name {
                if let Some(cte_query) = find_cte_query(query_ast, &actual_name) {
                    return resolve_virtual_source_column(
                        &actual_name,
                        column_name,
                        Some(&cte_query),
                        schema,
                        dialect,
                        seen,
                    );
                }
            }
            (Vec::new(), None)
        }
        Some("subquery") => resolve_virtual_source_column(
            table_ref,
            column_name,
            source
                .get("subquery")
                .and_then(Value::as_object)
                .and_then(|subquery| subquery.get("this")),
            schema,
            dialect,
            seen,
        ),
        _ => (Vec::new(), None),
    }
}

fn resolve_virtual_source_column(
    source_name: &str,
    column_name: &str,
    source_query: Option<&Value>,
    schema: &Value,
    dialect: DialectType,
    seen: &HashSet<(String, String)>,
) -> (Vec<UpstreamColumn>, Option<String>) {
    let Some(source_query) = source_query else {
        return (Vec::new(), None);
    };

    let cycle_key = (source_name.to_lowercase(), column_name.to_lowercase());
    if seen.contains(&cycle_key) {
        return (Vec::new(), None);
    }

    let Some(output_expr) = find_output_expression(source_query, column_name) else {
        return (Vec::new(), None);
    };

    let mut next_seen = seen.clone();
    next_seen.insert(cycle_key);
    let (upstream, _) = resolve_expression_upstreams(
        Some(&output_expr),
        source_query,
        schema,
        dialect,
        &next_seen,
    );

    let inferred = inferred_type_from_expr(&output_expr);
    let virtual_type = inferred
        .as_ref()
        .map(|inferred| format_type(Some(inferred), &output_expr, !upstream.is_empty()))
        .or_else(|| compat_expression_type(Some(&output_expr)));

    (upstream, virtual_type)
}

fn resolve_unqualified_source_column(
    column_name: &str,
    query_ast: &Value,
    schema_lookup: &SchemaLookup,
    schema: &Value,
    dialect: DialectType,
    seen: &HashSet<(String, String)>,
) -> (Vec<UpstreamColumn>, Option<String>) {
    let mut candidates = Vec::new();
    for source in query_source_wrappers(query_ast) {
        match node_kind(&source).as_deref() {
            Some("table") => {
                let table = source.get("table").cloned().unwrap_or_default();
                let actual = get_table_name_obj(&table);
                let (direct, _) =
                    direct_table_upstream(&actual, column_name, schema_lookup, dialect);
                if !direct.is_empty() {
                    candidates.push((direct, None));
                    continue;
                }

                if let Some(actual_name) = table
                    .as_object()
                    .and_then(|table| identifier_name(table.get("name")))
                {
                    if let Some(cte_query) = find_cte_query(query_ast, &actual_name) {
                        let resolved = resolve_virtual_source_column(
                            &actual_name,
                            column_name,
                            Some(&cte_query),
                            schema,
                            dialect,
                            seen,
                        );
                        if !resolved.0.is_empty() {
                            candidates.push(resolved);
                        }
                    }
                }
            }
            Some("subquery") => {
                let alias = source
                    .get("subquery")
                    .and_then(Value::as_object)
                    .and_then(|subquery| identifier_name(subquery.get("alias")));
                if let Some(alias) = alias {
                    let resolved = resolve_virtual_source_column(
                        &alias,
                        column_name,
                        source
                            .get("subquery")
                            .and_then(Value::as_object)
                            .and_then(|subquery| subquery.get("this")),
                        schema,
                        dialect,
                        seen,
                    );
                    if !resolved.0.is_empty() {
                        candidates.push(resolved);
                    }
                }
            }
            _ => {}
        }
    }

    if candidates.len() == 1 {
        candidates.into_iter().next().unwrap()
    } else {
        (Vec::new(), None)
    }
}

fn collect_lineage_leaves(node: &Value, leaves: &mut Vec<Value>) {
    let downstream = node
        .as_object()
        .and_then(|object| object.get("downstream"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if downstream.is_empty() {
        leaves.push(node.clone());
        return;
    }
    for child in downstream {
        collect_lineage_leaves(&child, leaves);
    }
}

fn lineage_for_column(
    column_name: &str,
    query: &str,
    schema: &Value,
    dialect: DialectType,
    parsed: Option<&Value>,
) -> (
    Option<Value>,
    Vec<UpstreamColumn>,
    Option<String>,
    Option<String>,
) {
    let expr = match parse_one(query, dialect) {
        Ok(expr) => expr,
        Err(err) => return (None, Vec::new(), Some(err.to_string()), None),
    };

    let validation_schema = simple_schema_to_validation(schema);
    let node = if validation_schema.tables.is_empty() {
        lineage(column_name, &expr, Some(dialect), false)
    } else {
        let mapping_schema = mapping_schema_from_validation_schema(&validation_schema);
        lineage_with_schema(
            column_name,
            &expr,
            Some(&mapping_schema),
            Some(dialect),
            false,
        )
    };

    let node = match node {
        Ok(node) => node,
        Err(err) => return (None, Vec::new(), Some(err.to_string()), None),
    };

    let node_value = serde_json::to_value(node).expect("lineage node should serialize");
    let mut leaves = Vec::new();
    collect_lineage_leaves(&node_value, &mut leaves);

    let schema_lookup = simple_schema_lookup(schema);
    let mut upstream = Vec::new();
    let mut seen = HashSet::new();
    let mut saw_virtual_leaf = false;
    for leaf in leaves {
        let source = leaf.as_object().and_then(|object| object.get("source"));
        if node_kind(source.unwrap_or(&Value::Null)).as_deref() != Some("table") {
            continue;
        }

        let column = leaf
            .as_object()
            .and_then(|object| object.get("name"))
            .and_then(Value::as_str)
            .map(|name| {
                name.split('.')
                    .last()
                    .unwrap_or(name)
                    .trim_matches('"')
                    .to_string()
            });
        let Some(column) = column else {
            continue;
        };

        let table = leaf
            .as_object()
            .and_then(|object| object.get("source_name"))
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .filter(|value| !value.is_empty())
            .or_else(|| {
                source
                    .and_then(|source| source.get("table"))
                    .map(get_table_name_obj)
                    .filter(|value| !value.is_empty())
            });
        let Some(table) = table else {
            continue;
        };

        let column = normalize_identifier(&column, dialect, "column");
        let table = normalize_identifier(&table, dialect, "table");
        if !schema_lookup.contains_key(&table.to_lowercase()) {
            saw_virtual_leaf = true;
            continue;
        }
        if seen.insert((column.clone(), table.clone())) {
            upstream.push(UpstreamColumn { column, table });
        }
    }

    let mut virtual_type = None;
    if saw_virtual_leaf || upstream.is_empty() {
        let query_ast = node_value
            .as_object()
            .and_then(|object| object.get("source"))
            .or(parsed)
            .cloned()
            .unwrap_or_else(empty_object);
        let (resolved_upstream, resolved_type) = resolve_expression_upstreams(
            node_value
                .as_object()
                .and_then(|object| object.get("expression")),
            &query_ast,
            schema,
            dialect,
            &HashSet::new(),
        );
        if !resolved_upstream.is_empty() {
            upstream = resolved_upstream;
        }
        virtual_type = resolved_type;
    }

    upstream.sort_by(|left, right| left.table.cmp(&right.table));
    (Some(node_value), upstream, None, virtual_type)
}

fn resolve_unqualified_column(
    column_name: &str,
    table_names: &[String],
    schema_lookup: &SchemaLookup,
) -> Option<String> {
    let mut candidates = Vec::new();
    let lower_column = column_name.to_lowercase();
    for table_name in table_names {
        if schema_lookup
            .get(&table_name.to_lowercase())
            .is_some_and(|columns| columns.contains_key(&lower_column))
        {
            candidates.push(table_name.clone());
        }
    }

    if candidates.len() == 1 {
        candidates.into_iter().next()
    } else if table_names.len() == 1 {
        table_names.first().cloned()
    } else {
        None
    }
}

fn extract_non_selected_columns(ast_node: &Value, schema: &Value) -> Vec<ColumnLineage> {
    let real_tables = collect_real_tables(ast_node);
    let table_names: Vec<_> = real_tables
        .iter()
        .map(get_table_name_obj)
        .filter(|name| !name.is_empty())
        .collect();

    let mut alias_map = HashMap::new();
    for table in real_tables {
        let actual = get_table_name_obj(&table);
        if actual.is_empty() {
            continue;
        }
        if let Some(alias) = identifier_name(table.get("alias")) {
            alias_map.insert(alias, actual.clone());
        }
        if let Some(short_name) = identifier_name(table.get("name")) {
            alias_map.insert(short_name, actual.clone());
        }
    }

    let schema_lookup = simple_schema_lookup(schema);
    let mut found = BTreeSet::new();

    let mut selects = Vec::new();
    collect_wrappers(ast_node, "select", &mut selects);
    for select in selects {
        let Some(select_object) = select.as_object() else {
            continue;
        };
        let mut clauses = Vec::new();
        if let Some(where_clause) = select_object.get("where_clause") {
            clauses.push(where_clause.clone());
        }
        if let Some(joins) = select_object.get("joins").and_then(Value::as_array) {
            for join in joins {
                if let Some(on) = join.as_object().and_then(|join| join.get("on")) {
                    clauses.push(on.clone());
                }
            }
        }
        if let Some(group_by) = select_object.get("group_by") {
            clauses.push(group_by.clone());
        }

        for clause in clauses {
            let mut columns = Vec::new();
            if node_kind(&clause).as_deref() == Some("group_by") {
                let expressions = clause
                    .get("expressions")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                for expression in expressions {
                    if node_kind(&expression).as_deref() == Some("literal") {
                        let literal = expression.get("literal").and_then(Value::as_object);
                        if literal
                            .and_then(|literal| literal.get("literal_type"))
                            .and_then(Value::as_str)
                            == Some("number")
                        {
                            let index = literal
                                .and_then(|literal| literal.get("value"))
                                .and_then(Value::as_str)
                                .and_then(|value| value.parse::<usize>().ok())
                                .and_then(|value| value.checked_sub(1));
                            if let Some(index) = index {
                                let select_expressions = select_object
                                    .get("expressions")
                                    .and_then(Value::as_array)
                                    .cloned()
                                    .unwrap_or_default();
                                if let Some(select_expr) = select_expressions.get(index) {
                                    collect_columns_in_scope(select_expr, &mut columns);
                                    continue;
                                }
                            }
                        }
                    }
                    collect_columns_in_scope(&expression, &mut columns);
                }
            } else {
                collect_columns_in_scope(&clause, &mut columns);
            }

            for column in columns {
                let column_object = column.as_object();
                let name = column_object.and_then(|column| identifier_name(column.get("name")));
                let mut table =
                    column_object.and_then(|column| identifier_name(column.get("table")));
                let Some(name) = name else {
                    continue;
                };

                if let Some(resolved) = table
                    .as_ref()
                    .and_then(|table_name| alias_map.get(table_name))
                {
                    table = Some(resolved.clone());
                } else if table.is_none() {
                    table = resolve_unqualified_column(&name, &table_names, &schema_lookup);
                }

                if let Some(table) = table {
                    if table_names.contains(&table) {
                        found.insert((name, table));
                    }
                }
            }
        }
    }

    let mut grouped: BTreeMap<String, Vec<UpstreamColumn>> = BTreeMap::new();
    for (name, table) in found {
        grouped
            .entry(name.clone())
            .or_default()
            .push(UpstreamColumn {
                column: name,
                table,
            });
    }

    grouped
        .into_iter()
        .map(|(name, mut upstream)| {
            upstream.sort_by(|left, right| {
                left.column.to_lowercase().cmp(&right.column.to_lowercase())
            });
            ColumnLineage {
                name,
                upstream,
                data_type: None,
            }
        })
        .collect()
}

fn collect_cte_aliases(ast_node: &Value) -> HashSet<String> {
    let mut aliases = HashSet::new();
    let mut with_clauses = Vec::new();
    collect_values(ast_node, "with", &mut with_clauses);
    for with_clause in with_clauses {
        let Some(with_object) = with_clause.as_object() else {
            continue;
        };
        let Some(ctes) = with_object.get("ctes").and_then(Value::as_array) else {
            continue;
        };
        for cte in ctes {
            if let Some(alias_name) =
                identifier_name(cte.as_object().and_then(|object| object.get("alias")))
            {
                aliases.insert(alias_name);
            }
        }
    }
    aliases
}

fn collect_real_tables(ast_node: &Value) -> Vec<Value> {
    let cte_aliases = collect_cte_aliases(ast_node);
    let mut tables = Vec::new();
    collect_wrappers(ast_node, "table", &mut tables);
    tables
        .into_iter()
        .filter(|table| {
            let name = identifier_name(table.get("name"));
            let schema = identifier_name(table.get("schema"));
            let catalog = identifier_name(table.get("catalog"));
            if let Some(name) = name {
                !(cte_aliases.contains(&name) && schema.is_none() && catalog.is_none())
            } else {
                true
            }
        })
        .collect()
}

fn tsql_function_table_names(ast_node: &Value) -> Vec<String> {
    let mut functions = Vec::new();
    collect_wrappers(ast_node, "function", &mut functions);

    let mut names = Vec::new();
    for function in functions {
        let Some(function_object) = function.as_object() else {
            continue;
        };
        let Some(name) = function_object.get("name").and_then(Value::as_str) else {
            continue;
        };
        let args = function_object
            .get("args")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if args.is_empty() {
            continue;
        }
        if args.len() <= 2
            && args
                .iter()
                .all(|arg| node_kind(arg).as_deref() == Some("column"))
        {
            names.push(name.to_string());
        }
    }

    names
}

pub fn get_tables(query: &str, dialect: DialectType) -> Value {
    let parsed = match parse(query, dialect) {
        Ok(parsed) => parsed,
        Err(err) => return json!({ "tables": [], "error": err.to_string() }),
    };

    let mut tables = BTreeSet::new();
    let mut current_database: Option<String> = None;
    for statement in parsed {
        let statement_value = expression_to_value(&statement);
        let kind = node_kind(&statement_value);
        if kind.as_deref() == Some("use") {
            if let Some(name) = statement_value
                .as_object()
                .and_then(|object| object.get("use"))
                .and_then(Value::as_object)
                .and_then(|object| object.get("this"))
            {
                current_database = identifier_name(Some(name));
            }
            continue;
        }

        if kind.as_deref() == Some("create_table") {
            if let Some(target) = statement_value
                .as_object()
                .and_then(|object| object.get("create_table"))
                .and_then(Value::as_object)
                .and_then(|object| object.get("name"))
            {
                let target_name = get_table_name_obj(target);
                if !target_name.is_empty() {
                    tables.insert(target_name);
                }
            }
        }

        for table in collect_real_tables(&statement_value) {
            if dialect == DialectType::TSQL {
                if let Some(current_database) = current_database.as_deref() {
                    tables.insert(get_table_name_with_context_obj(&table, current_database));
                    continue;
                }
            }
            tables.insert(get_table_name_obj(&table));
        }

        if dialect == DialectType::TSQL {
            for name in tsql_function_table_names(&statement_value) {
                tables.insert(name);
            }
        }
    }

    json!({ "tables": tables.into_iter().collect::<Vec<_>>() })
}

pub fn replace_table_references(
    query: &str,
    dialect: DialectType,
    table_references: &HashMap<String, String>,
) -> Value {
    let parsed_queries = match parse(query, dialect) {
        Ok(parsed) => parsed,
        Err(err) => return json!({ "query": "", "error": err.to_string() }),
    };

    let mut rewritten = Vec::new();
    for parsed_query in parsed_queries {
        let mut parsed_value = expression_to_value(&parsed_query);
        rewrite_tables_in_place(&mut parsed_value, table_references);
        if dialect == DialectType::BigQuery {
            canonicalize_bigquery_functions(&mut parsed_value);
        }

        match value_to_expression(&parsed_value)
            .and_then(|expression| generate(&expression, dialect).map_err(|err| err.to_string()))
        {
            Ok(sql) => rewritten.push(sql),
            Err(err) => return json!({ "query": "", "error": err }),
        }
    }

    json!({ "query": rewritten.join("; "), "error": null })
}

fn canonicalize_bigquery_functions(node: &mut Value) {
    match node {
        Value::Object(object) => {
            if object.len() == 1 && object.contains_key("function") {
                if let Some(function) = object.get_mut("function").and_then(Value::as_object_mut) {
                    if function
                        .get("name")
                        .and_then(Value::as_str)
                        .is_some_and(|name| name.eq_ignore_ascii_case("datediff"))
                    {
                        function.insert("name".to_string(), Value::String("DATE_DIFF".to_string()));
                        if let Some(args) = function.get_mut("args").and_then(Value::as_array_mut) {
                            if args.len() == 3
                                && args.get(2).is_some_and(is_zero_arg_current_date_function)
                            {
                                args.pop();
                            }
                        }
                    }
                }
            }

            for value in object.values_mut() {
                canonicalize_bigquery_functions(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                canonicalize_bigquery_functions(value);
            }
        }
        _ => {}
    }
}

fn is_zero_arg_current_date_function(value: &Value) -> bool {
    node_kind(value).as_deref() == Some("function")
        && value
            .get("function")
            .and_then(Value::as_object)
            .and_then(|function| function.get("name"))
            .and_then(Value::as_str)
            .is_some_and(|name| name.eq_ignore_ascii_case("current_date"))
        && value
            .get("function")
            .and_then(Value::as_object)
            .and_then(|function| function.get("args"))
            .and_then(Value::as_array)
            .is_some_and(|args| args.is_empty())
}

fn rewrite_tables_in_place(node: &mut Value, table_references: &HashMap<String, String>) {
    match node {
        Value::Object(object) => {
            if let Some(table_value) = object.get_mut("table") {
                rewrite_table_object(table_value, table_references);
            }
            for value in object.values_mut() {
                rewrite_tables_in_place(value, table_references);
            }
        }
        Value::Array(values) => {
            for value in values {
                rewrite_tables_in_place(value, table_references);
            }
        }
        _ => {}
    }
}

fn rewrite_table_object(table_value: &mut Value, table_references: &HashMap<String, String>) {
    let table_name = table_value
        .as_object()
        .and_then(|object| identifier_name(object.get("name")));
    let table_schema = table_value
        .as_object()
        .and_then(|object| identifier_name(object.get("schema")));
    let table_catalog = table_value
        .as_object()
        .and_then(|object| identifier_name(object.get("catalog")));
    let Some(table_name) = table_name else {
        return;
    };

    for (source, destination) in table_references {
        let source_parts: Vec<_> = source.split('.').collect();
        let (source_catalog, source_schema, source_table) = match source_parts.len() {
            3 => (
                Some(source_parts[0]),
                Some(source_parts[1]),
                source_parts[2],
            ),
            2 => (None, Some(source_parts[0]), source_parts[1]),
            _ => (None, None, source_parts[0]),
        };

        if table_name != source_table {
            continue;
        }
        if source_schema.is_some() && source_schema != table_schema.as_deref() {
            continue;
        }
        if source_catalog.is_some() && source_catalog != table_catalog.as_deref() {
            continue;
        }

        let destination_parts: Vec<_> = destination.split('.').collect();
        let (destination_catalog, destination_schema, destination_table) =
            match destination_parts.len() {
                3 => (
                    Some(destination_parts[0].to_string()),
                    Some(destination_parts[1].to_string()),
                    destination_parts[2],
                ),
                2 => (
                    None,
                    Some(destination_parts[0].to_string()),
                    destination_parts[1],
                ),
                _ => (None, None, destination_parts[0]),
            };

        update_table_object(
            table_value,
            source_table,
            destination_table,
            destination_schema,
            destination_catalog,
        );
        return;
    }
}

pub fn add_limit(query: &str, limit_value: usize, dialect: DialectType) -> Value {
    let parsed = match parse_one(query, dialect) {
        Ok(parsed) => parsed,
        Err(_) => return json!({ "error": "cannot parse query" }),
    };

    let mut parsed_value = expression_to_value(&parsed);
    match node_kind(&parsed_value).as_deref() {
        Some("select") => {
            if let Some(select) = parsed_value
                .as_object_mut()
                .and_then(|object| object.get_mut("select"))
                .and_then(Value::as_object_mut)
            {
                select.insert("limit".to_string(), make_limit_node(limit_value, dialect));
            }
        }
        Some("union") | Some("intersect") | Some("except") => {
            if let Some(kind) = node_kind(&parsed_value) {
                if let Some(node) = parsed_value
                    .as_object_mut()
                    .and_then(|object| object.get_mut(&kind))
                    .and_then(Value::as_object_mut)
                {
                    node.insert("limit".to_string(), make_limit_node(limit_value, dialect));
                }
            }
        }
        _ => return json!({ "error": "cannot parse query" }),
    }

    match value_to_expression(&parsed_value)
        .and_then(|expression| generate(&expression, dialect).map_err(|err| err.to_string()))
    {
        Ok(query) => json!({ "query": query }),
        Err(_) => json!({ "error": "cannot parse query" }),
    }
}

pub fn is_single_select_query(query: &str, dialect: DialectType) -> Value {
    if query.trim().is_empty() {
        return json!({ "is_single_select": false, "error": "cannot parse query" });
    }

    let statements = match parse(query, dialect) {
        Ok(statements) => statements,
        Err(err) => {
            return json!({
                "is_single_select": false,
                "error": err.to_string(),
            })
        }
    };

    if statements.is_empty() {
        return json!({ "is_single_select": false, "error": "cannot parse query" });
    }
    if statements.len() != 1 {
        return json!({ "is_single_select": false, "error": "" });
    }

    let kind = node_kind(&expression_to_value(&statements[0]));
    json!({
        "is_single_select": matches!(kind.as_deref(), Some("select" | "union" | "intersect" | "except")),
        "error": "",
    })
}

fn make_limit_node(limit: usize, dialect: DialectType) -> Value {
    let ast =
        parse_one(&format!("SELECT 1 LIMIT {limit}"), dialect).expect("limit query should parse");
    let ast_value = expression_to_value(&ast);
    first_wrapper(&ast_value, "select")
        .and_then(|select| {
            select
                .as_object()
                .and_then(|object| object.get("limit"))
                .cloned()
        })
        .expect("select limit should exist")
}

fn update_table_object(
    table_value: &mut Value,
    source_table: &str,
    destination_table: &str,
    destination_schema: Option<String>,
    destination_catalog: Option<String>,
) {
    let Some(table_object) = table_value.as_object_mut() else {
        return;
    };

    table_object.insert(
        "name".to_string(),
        json!({"name": destination_table, "quoted": false, "trailing_comments": []}),
    );
    table_object.insert(
        "schema".to_string(),
        destination_schema
            .map(|schema| json!({"name": schema, "quoted": false, "trailing_comments": []}))
            .unwrap_or(Value::Null),
    );
    table_object.insert(
        "catalog".to_string(),
        destination_catalog
            .map(|catalog| json!({"name": catalog, "quoted": false, "trailing_comments": []}))
            .unwrap_or(Value::Null),
    );

    if (table_object.get("alias").is_none()
        || table_object.get("alias").is_some_and(Value::is_null))
        && source_table != destination_table
    {
        table_object.insert(
            "alias".to_string(),
            json!({"name": source_table, "quoted": false, "trailing_comments": []}),
        );
    }
}

pub fn get_column_lineage(query: &str, schema: &Value, dialect: DialectType) -> LineageResponse {
    let parsed = match parse_one(query, dialect) {
        Ok(parsed) => parsed,
        Err(err) => {
            return LineageResponse {
                columns: Vec::new(),
                non_selected_columns: Vec::new(),
                errors: vec![format!("Parse error: {err}")],
            }
        }
    };

    let parsed_value = expression_to_value(&parsed);
    if !matches!(
        node_kind(&parsed_value).as_deref(),
        Some("select" | "union" | "intersect" | "except")
    ) {
        return LineageResponse {
            columns: Vec::new(),
            non_selected_columns: Vec::new(),
            errors: vec!["Failed to parse query".to_string()],
        };
    }

    let (prepared_query, prepared_schema, prepared_ast) =
        match prepare_lineage_inputs(schema, dialect, &parsed_value) {
            Ok(prepared) => prepared,
            Err(err) => {
                return LineageResponse {
                    columns: Vec::new(),
                    non_selected_columns: Vec::new(),
                    errors: vec![err],
                }
            }
        };

    let selected = if select_expressions(&parsed_value)
        .iter()
        .any(|expr| node_kind(expr).as_deref() == Some("star"))
    {
        expanded_select_outputs(&prepared_ast, &prepared_schema)
    } else {
        select_expressions(&parsed_value)
            .into_iter()
            .filter_map(|expr| {
                output_name(&expr).map(|name| SelectedOutput {
                    name,
                    expr: Some(expr),
                })
            })
            .collect()
    };

    let mut qualified_source: Option<Value> = None;
    let mut results = Vec::new();
    for item in selected {
        let expr = item.expr.as_ref();
        let mut lookup_name = item.name.clone();
        if expr.is_some()
            && node_kind(expr.unwrap()).as_deref() == Some("column")
            && is_case_insensitive_column_dialect(dialect)
        {
            lookup_name = lookup_name.to_lowercase();
        }

        let (node, mut upstream, _error, virtual_type) = lineage_for_column(
            &lookup_name,
            &prepared_query,
            &prepared_schema,
            dialect,
            Some(&prepared_ast),
        );
        let Some(node) = node else {
            continue;
        };

        if qualified_source.is_none() {
            qualified_source = node
                .as_object()
                .and_then(|object| object.get("source"))
                .cloned();
        }

        let expr_for_type = expr
            .cloned()
            .or_else(|| {
                node.as_object()
                    .and_then(|object| object.get("expression"))
                    .cloned()
            })
            .unwrap_or_else(empty_object);

        let inferred_type = node
            .as_object()
            .and_then(|object| object.get("expression"))
            .and_then(inferred_type_from_expr);
        let fallback = schema_fallback_type(&upstream, &prepared_schema);

        let mut formatted_type =
            format_type(inferred_type.as_ref(), &expr_for_type, !upstream.is_empty());
        if matches!(formatted_type.as_str(), "UNKNOWN" | "CUSTOM") {
            if let Some(compat_type) = compat_expression_type(Some(&expr_for_type)) {
                formatted_type = compat_type;
            }
        }
        if formatted_type == "UNKNOWN" {
            if let Some(virtual_type) = virtual_type.clone() {
                formatted_type = virtual_type;
            }
        }
        if formatted_type == "UNKNOWN" {
            if let Some(fallback) = fallback.clone() {
                formatted_type = fallback;
            }
        }

        let (inner_kind, function_name) = inner_expression_kind(&expr_for_type);
        let function_name_upper = function_name.as_ref().map(|name| name.to_uppercase());
        if inner_kind.as_deref() == Some("column")
            && matches!(fallback.as_deref(), Some("INT" | "BIGINT"))
        {
            formatted_type = fallback.clone().unwrap_or(formatted_type);
        }
        if inner_kind.as_deref() == Some("sum")
            && matches!(fallback.as_deref(), Some("INT" | "BIGINT"))
        {
            formatted_type = "BIGINT".to_string();
        } else if formatted_type == "DECIMAL" && inner_kind.as_deref() == Some("sum") {
            if let Some(fallback) = fallback.clone() {
                formatted_type = fallback;
            }
        }
        if formatted_type == "VARCHAR" {
            if matches!(
                inner_kind.as_deref(),
                Some("upper" | "lower" | "trim" | "substring")
            ) && fallback.as_deref() == Some("TEXT")
            {
                formatted_type = "TEXT".to_string();
            }
            if inner_kind.as_deref() == Some("date_trunc")
                || function_name_upper.as_deref() == Some("DATE_TRUNC")
            {
                formatted_type = "UNKNOWN".to_string();
            }
        }
        if formatted_type == "DOUBLE"
            && (matches!(inner_kind.as_deref(), Some("round" | "floor" | "ceil"))
                || matches!(
                    function_name_upper.as_deref(),
                    Some("ROUND" | "FLOOR" | "CEIL")
                ))
        {
            formatted_type = "FLOAT".to_string();
        }
        if formatted_type == "INT" && inner_kind.as_deref() == Some("length") {
            formatted_type = "BIGINT".to_string();
        }
        if matches!(function_name_upper.as_deref(), Some("TO_TIMESTAMP" | "NOW")) {
            formatted_type = "UNKNOWN".to_string();
        }

        upstream
            .sort_by(|left, right| left.column.to_lowercase().cmp(&right.column.to_lowercase()));
        results.push(ColumnLineage {
            name: normalize_identifier(&item.name, dialect, "output"),
            upstream,
            data_type: Some(formatted_type),
        });
    }

    results.sort_by(|left, right| left.name.cmp(&right.name));
    let source_for_non_selected = qualified_source.as_ref().unwrap_or(&prepared_ast);
    let mut non_selected = extract_non_selected_columns(source_for_non_selected, &prepared_schema);
    for item in &mut non_selected {
        item.name = normalize_identifier(&item.name, dialect, "column");
        for upstream in &mut item.upstream {
            upstream.column = normalize_identifier(&upstream.column, dialect, "column");
            upstream.table = normalize_identifier(&upstream.table, dialect, "table");
        }
    }

    LineageResponse {
        columns: results,
        non_selected_columns: non_selected,
        errors: Vec::new(),
    }
}
