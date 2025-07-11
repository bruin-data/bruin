import logging
from dataclasses import dataclass
from sqlglot import parse_one, parse, exp, lineage
from sqlglot.lineage import Node
from sqlglot.optimizer import optimize
from sqlglot.optimizer.scope import find_all_in_scope, build_scope


@dataclass(frozen=True)
class Column:
    name: str
    table: str


def extract_non_selected_columns(parsed: exp.Select) -> list[Column]:
    where = parsed.find_all(exp.Where)
    join = parsed.find_all(exp.Join)
    group = parsed.find_all(exp.Group)

    cols = []
    tables = extract_tables(parsed)
    table_alias = {}
    for table in tables:
        if table.alias:
            table_alias[table.alias] = merge_parts(table)

    table_names = {}
    for table in tables:
        table_key = merge_parts(table)
        table_names[table_key] = table

    for scopes in [where, join, group]:
        for scope in scopes:
            if scope is None:
                continue
            for expr in find_all_in_scope(scope, exp.Column):
                table_name = expr.table

                if expr.table in table_alias:
                    table_name = table_alias[expr.table]
                if table_name in table_names:
                    cols.append(Column(name=expr.name, table=table_name))

    result = list(set(cols))
    result.sort(key=lambda x: x.name + x.table)
    return result


def extract_tables(parsed):
    if parsed is None:
        return []

    def get_cte_names(parsed_stmt):
        """Get all CTE names from the parsed statement"""
        cte_names = set()

        # Handle different statement types
        if isinstance(parsed_stmt, exp.Create):
            # For CREATE TABLE statements, look in the expression part
            if parsed_stmt.expression:
                for cte in parsed_stmt.expression.find_all(exp.CTE):
                    cte_names.add(cte.alias_or_name)
        else:
            # For regular SELECT statements
            for cte in parsed_stmt.find_all(exp.CTE):
                cte_names.add(cte.alias_or_name)

        return cte_names

    def extract_table_references(stmt, cte_names):
        """Extract table references, excluding CTEs"""
        table_refs = []

        # Find all table references
        for table in stmt.find_all(exp.Table):
            # Get the actual table name (not the alias)
            actual_table_name = table.name

            # Check if this is a CTE reference
            # A table reference is a CTE if:
            # 1. The table name matches a CTE name, AND
            # 2. It doesn't have a schema/database prefix (CTEs are referenced without schema)
            is_cte_reference = (
                actual_table_name in cte_names
                and not table.db  # No schema/database prefix
                and not table.catalog  # No catalog prefix
            )

            # Skip if it's a CTE reference
            if is_cte_reference:
                continue

            # Keep all table references, including different aliases for the same table
            # This is important for self-joins and extract_non_selected_columns
            table_refs.append(table)

        return table_refs

    # Get all CTE names first
    cte_names = get_cte_names(parsed)

    # Extract table references
    table_refs = extract_table_references(parsed, cte_names)

    return table_refs


def extract_columns(parsed):
    cols = []
    found = parsed.find(exp.Select)
    if found is None:
        return cols
    for expression in found.expressions:
        if isinstance(expression, exp.CTE):
            continue
        cols.append(
            {
                "name": expression.alias_or_name,
                "type": str(expression.type),
            }
        )

    return cols


def get_table_name(table: exp.Table):
    db_name = ""
    if hasattr(table, "catalog") and len(table.catalog) > 0:
        db_name = table.catalog + "."
    schema_name = ""
    if hasattr(table, "db") and len(table.db) > 0:
        schema_name = table.db + "."
    return db_name + schema_name + table.name


def get_tables(query: str, dialect: str):
    try:
        parsed = parse(query, dialect=dialect)
        if parsed is None:
            return {"tables": [], "error": "unable to parse query"}
    except Exception as e:
        return {"tables": [], "error": str(e)}

    tables = []
    for parsedSingle in parsed:
        if parsedSingle is None:
            continue
        try:
            extracted = extract_tables(parsedSingle)
            tables.extend(extracted)
        except Exception as e:
            return {"tables": [], "error": str(e)}

    return {
        "tables": list(set([get_table_name(table) for table in tables])),
    }


def get_column_lineage(query: str, schema: dict, dialect: str):
    try:
        parsed = parse_one(query, dialect=dialect)
        if not isinstance(parsed, exp.Query):
            return {
                "columns": [],
                "non_selected_columns": [],
                "errors": ["Failed to parse query"],
            }
    except Exception as e:
        return {
            "columns": [],
            "non_selected_columns": [],
            "errors": [f"Parse error: {str(e)}"],
        }

    result = []
    errors = []

    from sqlglot.optimizer.annotate_types import annotate_types
    from sqlglot.optimizer.merge_subqueries import merge_subqueries
    from sqlglot.optimizer.qualify import qualify
    from sqlglot.optimizer.unnest_subqueries import unnest_subqueries

    nested_schema = schema_dict_to_schema_object(schema)
    try:
        try:
            optimized = optimize(
                parsed,
                nested_schema,
                dialect=dialect,
                rules=(
                    qualify,
                    # normalize,
                    unnest_subqueries,
                    # pushdown_predicates,
                    # optimize_joins,
                    # eliminate_subqueries,
                    merge_subqueries,
                    # eliminate_joins,
                    # eliminate_ctes,
                    annotate_types,
                    # canonicalize,
                    # simplify,
                ),
            )
        except Exception:
            # try again without dialect, this solves some issues
            try:
                optimized = optimize(parsed, nested_schema)
            except Exception as e:
                return {
                    "columns": [],
                    "non_selected_columns": [],
                    "errors": [f"Schema Error: {str(e)}"],
                }
    except Exception as e:
        logging.error(f"Schema error: {str(e)}")
        return {
            "columns": [],
            "non_selected_columns": [],
            "errors": [],
        }

    try:
        cols = extract_columns(optimized)
    except Exception as e:
        logging.error(f"Error extracting columns: {str(e)}")
        return {
            "columns": [],
            "non_selected_columns": [],
            "errors": [],
        }

    scope = build_scope(optimized)
    for col in cols:
        try:
            ll = lineage.lineage(
                col["name"],
                optimized,
                schema,
                dialect=dialect,
                scope=scope,
            )
            cl = []
            leaves: list[Node] = []

            try:
                find_leaf_nodes(ll, leaves)
            except Exception:
                continue

            for ds in leaves:
                try:
                    if isinstance(ds.expression.this, exp.Literal) or isinstance(
                        ds.expression.this, exp.Anonymous
                    ):
                        continue

                    if isinstance(ds.expression, exp.Table):
                        cl.append(
                            {
                                "column": ds.name.split(".")[-1].strip('"'),
                                "table": merge_parts(ds.expression),
                            }
                        )
                except Exception:
                    continue

            # Deduplicate based on column-table combination
            cl = [dict(t) for t in {tuple(d.items()) for d in cl}]
            cl.sort(key=lambda x: x["table"])

            result.append(
                {"name": col["name"], "upstream": cl, "type": col.get("type", "")}
            )
        except Exception as e:
            logging.error(f"Lineage error for column {col['name']}: {str(e)}")
            continue

    result.sort(key=lambda x: x["name"])

    non_selected_columns = []
    try:
        non_selected_columns_dict = {}
        for column in extract_non_selected_columns(optimized):
            try:
                if column.name not in non_selected_columns_dict:
                    non_selected_columns_dict[column.name] = {
                        "name": column.name,
                        "upstream": [],
                    }
                non_selected_columns_dict[column.name]["upstream"].append(
                    {"column": column.name, "table": column.table}
                )
            except Exception:
                continue
        non_selected_columns = list(non_selected_columns_dict.values())
    except Exception as e:
        logging.error(f"Error extracting non-selected columns: {str(e)}")

    # Sort upstreams even if there are errors
    try:
        for col in result:
            col["upstream"] = sorted(col["upstream"], key=lambda x: x["column"].lower())
        for col in non_selected_columns:
            col["upstream"] = sorted(col["upstream"], key=lambda x: x["column"].lower())
    except Exception as e:
        logging.error(f"Error: {str(e)}")

    return {
        "columns": result,
        "non_selected_columns": non_selected_columns,
        "errors": errors,
    }


def find_leaf_nodes(node: Node, leaf_nodes):
    if not node.downstream:
        leaf_nodes.append(node)
    else:
        for child in node.downstream:
            find_leaf_nodes(child, leaf_nodes)


def merge_parts(table: exp.Table) -> str:
    return ".".join(
        part.name for part in table.parts if isinstance(part, exp.Identifier)
    )


def schema_dict_to_schema_object(schema_dict: dict) -> dict:
    result = {}

    for table_path, value in schema_dict.items():
        current = result
        parts = table_path.split(".")

        # Handle all parts except the last one
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        # Handle the last part
        current[parts[-1]] = value

    return result


def replace_table_references(
    query: str, dialect: str, table_references: dict[str, str]
):
    parsed = parse_one(query, dialect=dialect)
    if parsed is None:
        return {"error": "unable to parse query"}

    for table_name, new_table_name in table_references.items():
        for table in parsed.find_all(exp.Table):
            if table.name == table_name:
                table.name = new_table_name

    return parsed.sql()


def add_limit(query: str, limit_value: int, dialect: str = None) -> dict:
    try:
        parsed = parse_one(query, dialect=dialect)
        if parsed is None:
            return {"error": "cannot parse query"}
    except Exception:
        return {"error": "cannot parse query"}

    limited_query = parsed.limit(limit_value).sql(dialect=dialect)
    return {"query": limited_query}


def is_single_select_query(query: str, dialect: str = None) -> dict:
    """
    Check if a query is a single SELECT statement.
    Returns {"is_single_select": bool, "error": str}
    """
    # Handle empty or whitespace-only queries
    if not query or not query.strip():
        return {"is_single_select": False, "error": "cannot parse query"}

    try:
        # Parse all statements in the query
        parsed_statements = parse(query, dialect=dialect)
        if not parsed_statements:
            return {"is_single_select": False, "error": "cannot parse query"}

        # Check if there's exactly one statement and it's a SELECT
        if len(parsed_statements) == 1:
            stmt = parsed_statements[0]
            # Check if it's a SELECT statement (including CTEs with SELECT)
            is_select = isinstance(stmt, (exp.Select, exp.Query))
            return {"is_single_select": is_select, "error": ""}
        else:
            # Multiple statements
            return {"is_single_select": False, "error": ""}

    except Exception as e:
        return {"is_single_select": False, "error": str(e)}
