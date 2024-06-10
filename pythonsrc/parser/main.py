from sqlglot import parse_one, exp, lineage
from sqlglot.optimizer.scope import find_all_in_scope, build_scope
from sqlglot.optimizer import optimize, qualify
from sqlglot.lineage import Node


def extract_tables(parsed):
    root = build_scope(parsed)
    tables = []
    for scope in root.traverse():
        for alias, (node, source) in scope.selected_sources.items():
            if isinstance(source, exp.Table):
                tables.append(source)

    return tables

def extract_columns(parsed):
    cols = []
    for expression in parsed.find(exp.Select).expressions:
        if isinstance(expression, exp.CTE):
            continue

        cols.append(expression.alias_or_name)

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
    parsed = parse_one(query, dialect=dialect)
    tables = extract_tables(parsed)

    return {
        "tables": list(set([get_table_name(table) for table in tables])),
    }

def get_column_lineage(query: str, schema: dict, dialect: str):
    parsed = parse_one(query, dialect=dialect)
    optimized = optimize(parsed, schema, dialect=dialect)

    result = []

    cols = extract_columns(optimized)
    for col in cols:
        ll = lineage.lineage(col, optimized, schema, dialect=dialect)

        cl = []
        leaves = []
        find_leaf_nodes(ll, leaves)

        for ds in leaves:
            cl.append(
                {"column": ds.name.split(".")[-1], "table": ds.expression.this.name}
            )

        cl.sort(key=lambda x: x["table"])
        result.append({"name": col, "upstream": cl})

    result.sort(key=lambda x: x["name"])
    return {
        "columns": result,
    }


def find_leaf_nodes(node: Node, leaf_nodes):
    if not node.downstream:
        leaf_nodes.append(node)
    else:
        for child in node.downstream:
            find_leaf_nodes(child, leaf_nodes)