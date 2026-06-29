from sqlglot import parse, exp


def replace_table_references(
    query: str, dialect: str, table_references: dict[str, str]
):
    parsed_queries = parse(query, dialect=dialect)
    if parsed_queries is None:
        return {"error": "unable to parse query"}

    for parsed_query in parsed_queries:
        for table_node in parsed_query.find_all(exp.Table):
            for table_name, new_table_name in table_references.items():
                parts = table_name.split(".")
                source_catalog = None
                source_schema = None
                source_table = table_name
                if len(parts) == 3:
                    source_catalog = parts[0]
                    source_schema = parts[1]
                    source_table = parts[2]
                elif len(parts) == 2:
                    source_schema = parts[0]
                    source_table = parts[1]

                if table_node.name != source_table:
                    continue

                if source_schema is not None and source_schema != table_node.db:
                    continue

                if source_catalog is not None and source_catalog != table_node.catalog:
                    continue

                parts = new_table_name.split(".")
                dest_catalog = None
                dest_schema = None
                dest_table = new_table_name
                if len(parts) == 3:
                    dest_catalog = parts[0]
                    dest_schema = parts[1]
                    dest_table = parts[2]
                elif len(parts) == 2:
                    dest_schema = parts[0]
                    dest_table = parts[1]

                table_node.this.set("this", dest_table)
                table_node.set("db", dest_schema)
                if dest_catalog is not None:
                    table_node.set("catalog", dest_catalog)
                elif dest_schema is None:
                    # The destination is a single identifier (e.g. a fixture CTE
                    # name), so clear the stale source catalog too and let a
                    # 3-part source collapse to one name. A 2-part destination
                    # intentionally keeps the source catalog (only schema.table
                    # was remapped).
                    table_node.set("catalog", None)
                if not table_node.alias and source_table != dest_table:
                    table_node.set("alias", source_table)

        # A renamed table loses its original schema/catalog (a fixture CTE is a
        # single identifier, a dev rename keeps only the leaf), so a column still
        # qualified by the old schema — e.g. analytics.orders.amount — would no
        # longer resolve. Drop that qualifier to the leaf table, which the renamed
        # table is always reachable by, via its explicit or implicit alias.
        for column_node in parsed_query.find_all(exp.Column):
            col_table = column_node.text("table")
            if not col_table:
                continue
            col_schema = column_node.text("db")
            col_catalog = column_node.text("catalog")
            if not col_schema and not col_catalog:
                continue  # already leaf-qualified; resolves to the alias
            for table_name in table_references:
                parts = table_name.split(".")
                source_catalog = None
                source_schema = None
                source_table = table_name
                if len(parts) == 3:
                    source_catalog = parts[0]
                    source_schema = parts[1]
                    source_table = parts[2]
                elif len(parts) == 2:
                    source_schema = parts[0]
                    source_table = parts[1]

                if col_table != source_table:
                    continue
                if (
                    source_schema is not None
                    and col_schema
                    and col_schema != source_schema
                ):
                    continue
                if (
                    source_catalog is not None
                    and col_catalog
                    and col_catalog != source_catalog
                ):
                    continue

                column_node.set("db", None)
                column_node.set("catalog", None)
                break

    return {
        "query": "; ".join([q.sql(dialect=dialect) for q in parsed_queries]),
        "error": None,
    }
