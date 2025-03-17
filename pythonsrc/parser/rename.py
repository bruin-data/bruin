from sqlglot import parse_one, exp


# todo: this implementation does not support columns in select where they specify the schema as well, e.g. select raw.table1.col1, col2 from raw.table1
def replace_table_references(
    query: str, dialect: str, table_references: dict[str, str]
):
    parsed = parse_one(query, dialect=dialect)
    if parsed is None:
        return {"error": "unable to parse query"}

    for table_node in parsed.find_all(exp.Table):
        for table_name, new_table_name in table_references.items():
            parts = table_name.split(".")
            source_schema = None
            source_table = table_name
            if len(parts) > 1:
                source_schema = parts[0]
                source_table = parts[1]

            if table_node.name != source_table:
                continue

            if source_schema is not None and source_schema != table_node.db:
                continue

            parts = new_table_name.split(".")
            dest_schema = None
            dest_table = new_table_name
            if len(parts) > 1:
                dest_schema = parts[0]
                dest_table = parts[1]

            table_node.this.set("this", dest_table)
            table_node.set("db", dest_schema)
            if not table_node.alias and source_table != dest_table:
                table_node.set("alias", source_table)

    return {
        "query": parsed.sql(),
        "error": None,
    }
