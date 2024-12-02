from sqlglot import parse_one, exp, lineage
from sqlglot.optimizer.scope import find_all_in_scope, build_scope
from sqlglot.optimizer import optimize, qualify
from sqlglot.lineage import Node

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
	select_columns = []
	found = parsed.find(exp.Select)
	if found is not None:
		# Extract select columns
		for expression in found.expressions:
			if not isinstance(expression, exp.CTE):  # Skip CTEs
				select_columns.append({
					"name": expression.alias_or_name,
					"type": str(expression.type),
				})

	# Extract all columns from the parsed expression
	cols = [{
		"name": str(expr),
		"type": str(expr.type),
	} for expr in find_all_in_scope(parsed, exp.Column)]

	# Remove duplicates while preserving order
	unique_cols = {col['name']: col for col in cols}.values()
	cols = []

	for col in unique_cols:
		# Split the column name into table and column parts
		table_name, column_name = col["name"].split(".") if "." in col["name"] else (None, col["name"])
		is_select = any(c["name"] == column_name.strip('"') and c["type"] == col["type"] for c in select_columns)  # Check if already selected
		if not is_select:
			cols.append({
				"name": column_name.strip('"'),
				"upstream": [{"column": column_name.strip('"'), "table": table_name.strip('"')}],
				"type": col["type"]
			})

	cols.sort(key=lambda x: x["name"])
	select_columns.sort(key=lambda x: x["name"])
	return {
		"select_columns": select_columns,
		"cols": cols,
	}

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
    
    for col in cols["select_columns"]:
        try:
            ll = lineage.lineage(col["name"], optimized, schema, dialect=dialect)
        except:
            continue

        cl = []
        leaves: list[Node] = []
        find_leaf_nodes(ll, leaves)

        for ds in leaves:
            if isinstance(ds.expression.this, exp.Literal) or isinstance(ds.expression.this, exp.Anonymous):
                continue

            cl.append(
                {"column": ds.name.split(".")[-1], "table": ds.expression.this.name}
            )

        # Deduplicate based on column-table combination
        cl = [dict(t) for t in {tuple(d.items()) for d in cl}]
        cl.sort(key=lambda x: x["table"])

        result.append({"name": col["name"], "upstream": cl,  "type": col["type"]})

    return {
        "columns": result,
        "lineage": cols["cols"]
    }


def find_leaf_nodes(node: Node, leaf_nodes):
    if not node.downstream:
        leaf_nodes.append(node)
    else:
        for child in node.downstream:
            find_leaf_nodes(child, leaf_nodes)



