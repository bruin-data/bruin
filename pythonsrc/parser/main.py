from sqlglot import parse_one, exp, lineage
from sqlglot.optimizer.scope import find_all_in_scope, build_scope
from sqlglot.optimizer import optimize, qualify
from sqlglot.lineage import Node



def extract_columns(parsed):
    cols = []
    for expression in parsed.find(exp.Select).expressions:
        if isinstance(expression, exp.CTE):
            continue

        cols.append(expression.alias_or_name)

    return cols


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