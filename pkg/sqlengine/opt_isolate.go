package sqlengine

// IsolateTableSelects rewrites each table with known columns that shares its scope with other
// sources into `(SELECT * FROM table AS alias) AS alias`. Tables must already carry an alias.
func IsolateTableSelects(expression *Expr, schema *MappingSchema, d *Dialect) *Expr {
	if schema == nil {
		schema = NewMappingSchema(nil, nil, d, true, nil)
	}

	for _, scope := range TraverseScope(expression) {
		if scope.SelectedSources().Len() == 1 {
			continue
		}

		for _, selected := range scope.SelectedSources().Values() {
			source := selected.Source

			// Every source must be attached to a parent node.
			if (source.Scope != nil && source.Scope.Parent == nil) || (source.Table != nil && source.Table.Parent() == nil) {
				panic(&ValueError{Msg: ""})
			}

			if source.Table == nil {
				continue
			}
			table := source.Table
			if len(schema.ColumnNames(table, false, nil, nil)) == 0 ||
				table.Parent().IsA(KSubquery) ||
				table.Parent().Parent().IsA(KTable) {
				continue
			}

			if table.Alias() == "" {
				panic(&OptimizeError{Msg: "Tables require an alias. Run qualify_tables optimization."})
			}

			table.Replace(
				SelectExpr(qtStar()).
					SelectFrom(AliasTableExpr(table, table.AliasOrName(), nil, nil, true), false).
					QuerySubquery(table.Alias(), false),
			)
		}
	}

	return expression
}
