from dataclasses import dataclass, asdict

from sqlglot import parse_one, exp, lineage
from sqlglot.lineage import Node
from sqlglot.optimizer import optimize
from sqlglot.optimizer.scope import find_all_in_scope, build_scope


@dataclass(frozen=True)
class Column:
    name: str
    table: str

    def to_json(self) -> str:
        return asdict(self)


def extract_non_selected_columns(parsed: exp.Select) -> list[Column]:
    where = parsed.find_all(exp.Where)
    join = parsed.find_all(exp.Join)
    group = parsed.find_all(exp.Group)

    cols = []
    for scopes in [where, join, group]:
        for scope in scopes:
            if scope is None:
                continue
            cols += [
                Column(name=expr.name, table=expr.table)
                for expr in find_all_in_scope(scope, exp.Column)
            ]
    result = list(set(cols))
    result.sort(key=lambda x: x.name + x.table)
    return result


def extract_tables(parsed):
    root = build_scope(parsed)
    if root is None:
        raise Exception("unable to build scope")

    tables = []
    for scope in root.traverse():
        for alias, (node, source) in scope.selected_sources.items():
            if isinstance(source, exp.Table):
                tables.append(source)

    return tables


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
        parsed = parse_one(query, dialect=dialect)
        if parsed is None:
            return {"tables": [], "error": "unable to parse query"}
    except Exception as e:
        return {"tables": [], "error": str(e)}

    try:
        tables = extract_tables(parsed)
    except Exception as e:
        return {"tables": [], "error": str(e)}

    return {
        "tables": list(set([get_table_name(table) for table in tables])),
    }


def get_column_lineage(query: str, schema: dict, dialect: str):
    parsed = parse_one(query, dialect=dialect)
    if not isinstance(parsed, exp.Query):
        return {"columns": []}
    try:
        optimized = optimize(parsed, schema, dialect=dialect)
    except:
        return {"columns": []}

    result = []

    cols = extract_columns(optimized)
    for col in cols:
        try:
            ll = lineage.lineage(col["name"], optimized, schema, dialect=dialect)
        except:
            continue

        cl = []
        leaves: list[Node] = []
        find_leaf_nodes(ll, leaves)

        for ds in leaves:
            if isinstance(ds.expression.this, exp.Literal) or isinstance(
                ds.expression.this, exp.Anonymous
            ):
                continue

            cl.append(
                {"column": ds.name.split(".")[-1], "table": ds.expression.this.name}
            )

        # Deduplicate based on column-table combination
        cl = [dict(t) for t in {tuple(d.items()) for d in cl}]
        cl.sort(key=lambda x: x["table"])

        result.append({"name": col["name"], "upstream": cl, "type": col["type"]})

    result.sort(key=lambda x: x["name"])

    non_selected_columns_dict = {}
    for column in extract_non_selected_columns(optimized):
        if column.name not in non_selected_columns_dict:
            non_selected_columns_dict[column.name] = {"name": column.name, "upstream": []}
        non_selected_columns_dict[column.name]["upstream"].append(
            {"column": column.name, "table": column.table}
        )
    non_selected_columns = list(non_selected_columns_dict.values())

    return {
        "columns": result,
        "non_selected_columns": non_selected_columns,
    }


def find_leaf_nodes(node: Node, leaf_nodes):
    if not node.downstream:
        leaf_nodes.append(node)
    else:
        for child in node.downstream:
            find_leaf_nodes(child, leaf_nodes)
