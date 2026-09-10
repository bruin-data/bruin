use polyglot_sql::dialects::Dialect;
use polyglot_sql::tokens::{Token, TokenType};
use polyglot_sql::{parse, DialectType, Expression};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::LazyLock;

static SNOWFLAKE_BLOCKED_FUNCTIONS: LazyLock<HashSet<&str>> = LazyLock::new(|| {
    include_str!("../../../../pythonsrc/parser/snowflake_blocked_functions.txt")
        .split_whitespace()
        .collect()
});

pub fn is_read_only_query(query: &str, dialect: DialectType) -> Value {
    match validate_query(query, dialect) {
        Ok(read_only) => json!({"is_read_only": read_only, "error": ""}),
        Err(error) => json!({"is_read_only": false, "error": error}),
    }
}

fn validate_query(query: &str, dialect: DialectType) -> Result<bool, String> {
    let mut tokens = Dialect::get(dialect)
        .tokenize(query)
        .map_err(|error| error.to_string())?;
    tokens.retain(|token| {
        !matches!(
            token.token_type,
            TokenType::BlockComment | TokenType::LineComment
        )
    });
    if dialect == DialectType::Snowflake
        && tokens
            .iter()
            .any(|token| token.token_type == TokenType::DArrow)
    {
        return Ok(false);
    }
    let statements = parse(query, dialect).map_err(|error| error.to_string())?;
    if statements.is_empty() {
        return Err("cannot parse empty query".into());
    }
    let mut aliases = HashSet::new();
    for statement in statements {
        if !is_read_root(&statement) {
            return Ok(false);
        }
        let value = serde_json::to_value(&statement).map_err(|error| error.to_string())?;
        if !is_read_tree(&value, dialect) {
            return Ok(false);
        }
        collect_alias_names(&value, &mut aliases);
    }
    Ok(allowed_function_tokens(query, dialect, &tokens, &aliases))
}

fn is_read_root(statement: &Expression) -> bool {
    match statement {
        Expression::Select(_)
        | Expression::Union(_)
        | Expression::Intersect(_)
        | Expression::Except(_)
        | Expression::Subquery(_)
        | Expression::Show(_)
        | Expression::Describe(_) => true,
        Expression::Annotated(node) => is_read_root(&node.this),
        _ => false,
    }
}

fn is_read_tree(value: &Value, dialect: DialectType) -> bool {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                if matches!(
                    key.as_str(),
                    "insert"
                        | "update"
                        | "delete"
                        | "merge"
                        | "copy"
                        | "put"
                        | "command"
                        | "execute"
                        | "execute_statement"
                        | "transaction"
                        | "commit"
                        | "rollback"
                        | "use"
                        | "pragma"
                        | "grant"
                        | "revoke"
                        | "next_value_for"
                        | "nextval"
                        | "pipe_operator"
                ) || key.starts_with("create")
                    || key.starts_with("alter")
                    || key.starts_with("drop")
                    || key.starts_with("truncate")
                {
                    if !child.is_null() {
                        return false;
                    }
                }
                if (key == "into" && !child.is_null())
                    || (key == "locks" && child.as_array().is_some_and(|v| !v.is_empty()))
                {
                    return false;
                }
                if key == "column"
                    && child
                        .pointer("/name/name")
                        .and_then(Value::as_str)
                        .is_some_and(|name| name.eq_ignore_ascii_case("NEXTVAL"))
                {
                    return false;
                }
                if matches!(
                    key.as_str(),
                    "function" | "anonymous" | "anonymous_agg_func"
                ) {
                    if dialect != DialectType::Snowflake
                        || child
                            .get("name")
                            .and_then(Value::as_str)
                            .is_some_and(|name| name.eq_ignore_ascii_case("IDENTIFIER"))
                    {
                        return false;
                    }
                }
                if !is_read_tree(child, dialect) {
                    return false;
                }
            }
            true
        }
        Value::Array(values) => values.iter().all(|value| is_read_tree(value, dialect)),
        _ => true,
    }
}

fn collect_alias_names(value: &Value, aliases: &mut HashSet<String>) {
    match value {
        Value::Object(object) => {
            if let Some(name) = value.pointer("/alias/name").and_then(Value::as_str) {
                aliases.insert(name.to_ascii_uppercase());
            }
            for child in object.values() {
                collect_alias_names(child, aliases);
            }
        }
        Value::Array(values) => {
            for child in values {
                collect_alias_names(child, aliases);
            }
        }
        _ => {}
    }
}

fn is_alias_token(query: &str, dialect: DialectType, token: &Token) -> bool {
    let mut marker = "BRUIN_READ_ONLY_ALIAS".to_string();
    let upper_query = query.to_ascii_uppercase();
    while upper_query.contains(&marker) {
        marker.push('X');
    }
    let before: String = query.chars().take(token.span.start).collect();
    let after: String = query.chars().skip(token.span.end).collect();
    let marked_query = format!("{before}{marker}{after}");
    let Ok(statements) = parse(&marked_query, dialect) else {
        return false;
    };
    let mut aliases = HashSet::new();
    for statement in statements {
        let Ok(value) = serde_json::to_value(&statement) else {
            return false;
        };
        collect_alias_names(&value, &mut aliases);
    }
    aliases.contains(&marker)
}

fn allowed_function_tokens(
    query: &str,
    dialect: DialectType,
    tokens: &[Token],
    aliases: &HashSet<String>,
) -> bool {
    for pair in tokens.windows(2) {
        if pair[1].token_type != TokenType::LParen {
            continue;
        }
        let token = &pair[0];
        let name = token.text.to_ascii_uppercase();
        if aliases.contains(&name) && is_alias_token(query, dialect, token) {
            continue;
        }
        if SNOWFLAKE_BLOCKED_FUNCTIONS.iter().any(|blocked| {
            blocked
                .strip_suffix('*')
                .map_or(name == *blocked, |prefix| name.starts_with(prefix))
        }) {
            return false;
        }
    }
    true
}
