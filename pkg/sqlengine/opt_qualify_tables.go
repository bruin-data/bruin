package sqlengine

import "strings"

// Port of sqlglot/optimizer/qualify_tables.py.

// QualifyTables mirrors qualify_tables(expression, db, catalog, on_qualify, dialect, canonicalize_table_aliases).
//
// It rewrites the AST to have fully qualified tables. Join constructs such as (t1 JOIN t2) AS t
// are expanded into (SELECT * FROM t1 AS t1, t2 AS t2) AS t. Empty db / catalog mean None;
// onQualify may be nil.
func QualifyTables(expression *Expr, db, catalog string, onQualify func(table *Expr), d *Dialect, canonicalizeTableAliases bool) *Expr {
	if d == nil {
		d = prototype("")
	}
	dialect := d
	nextAliasName := nameSequence("_")

	var dbID, catalogID *Expr
	if db != "" {
		dbID = parseIdentifier(db, dialect)
		dbID.Meta()["is_table"] = true
		dbID = NormalizeIdentifiers(dbID, dialect)
	}
	if catalog != "" {
		catalogID = parseIdentifier(catalog, dialect)
		catalogID.Meta()["is_table"] = true
		catalogID = NormalizeIdentifiers(catalogID, dialect)
	}

	qualify := func(table *Expr) {
		if table.This().IsA(KIdentifier) {
			if dbID != nil && !table.ArgB("db") {
				table.Set("db", dbID.Copy())
			}
			if catalogID != nil && !table.ArgB("catalog") && table.ArgB("db") {
				table.Set("catalog", catalogID.Copy())
			}
		}
	}

	if (dbID != nil || catalogID != nil) && !expression.IsA(KQuery) {
		with := expression.ArgE("with_")
		if with == nil {
			with = New(KWith)
		}
		cteNames := newStrSet()
		for _, cte := range with.Expressions() {
			cteNames.Add(cte.AliasOrName())
		}

		for node := range expression.Walk(true, func(n *Expr) bool { return n.IsA(KQuery) }) {
			if node.IsA(KTable) && !cteNames.Has(node.Name()) {
				qualify(node)
			}
		}
	}

	// setAlias mirrors the nested _set_alias. targetAlias "" means None; columns entries are
	// strings or expressions (Identifier / ColumnDef).
	setAlias := func(expression *Expr, canonicalAliases map[string]string, targetAlias string, scope *Scope, normalize bool, columns []any) {
		alias := expression.ArgE("alias")
		if alias == nil {
			alias = New(KTableAlias)
		}

		var newAliasName string
		if canonicalizeTableAliases {
			newAliasName = nextAliasName()
			key := alias.Name()
			if key == "" {
				key = targetAlias
			}
			canonicalAliases[key] = newAliasName
		} else if alias.Name() == "" {
			if targetAlias != "" {
				newAliasName = targetAlias
			} else {
				newAliasName = nextAliasName()
			}
			if normalize && targetAlias != "" {
				newAliasName = NormalizeIdentifiersStr(newAliasName, dialect).Name()
			}
		} else {
			return
		}

		alias.Set("this", ToIdentifier(newAliasName, nil))

		if len(columns) > 0 {
			cols := make([]*Expr, 0, len(columns))
			for _, c := range columns {
				switch x := c.(type) {
				case string:
					cols = append(cols, ToIdentifier(x, nil))
				case *Expr:
					cols = append(cols, x.Copy())
				}
			}
			alias.Set("columns", cols)
		}

		expression.Set("alias", alias)

		if scope != nil {
			scope.RenameSource("", newAliasName)
		}
	}

	for _, scope := range TraverseScope(expression) {
		localColumns := scope.LocalColumns()
		canonicalAliases := map[string]string{}

		for _, query := range scope.Subqueries() {
			subquery := query.Parent()
			if subquery.IsA(KSubquery) {
				unwrapped := subquery.Unwrap()
				if unwrapped.Parent().IsA(KCreate) && unwrapped != subquery {
					// Function bodies may require wrapping parentheses, e.g. in BigQuery
					// `... AS ((SELECT 1))` the outer parens delimit the body itself
					unwrapped.Set("this", subquery)
				} else {
					unwrapped.Replace(subquery)
				}
			}
		}

		for _, derivedTable := range scope.DerivedTables() {
			unnested := derivedTable.UnnestSubqueryOrParen()
			if unnested.IsA(KTable) {
				joins := unnested.Arg("joins")
				unnested.Set("joins", nil)
				derivedTable.This().Replace(SelectExpr(qtStar()).SelectFrom(unnested.Copy(), false))
				derivedTable.This().Set("joins", joins)
			}

			setAlias(derivedTable, canonicalAliases, "", scope, false, nil)
			if pivot := seqGet(derivedTable.ArgL("pivots"), 0); pivot != nil {
				setAlias(pivot, canonicalAliases, "", nil, false, nil)
			}
		}

		tableAliases := map[string]*Expr{}

		for _, name := range append([]string{}, scope.Sources.Keys()...) {
			source, _ := scope.Sources.Get(name)
			if source.Table != nil {
				source := source.Table
				// When the name is empty, it means that we have a non-table source, e.g. a pivoted cte
				isRealTableSource := name != ""

				pivot := seqGet(source.ArgL("pivots"), 0)
				if pivot != nil {
					name = source.Name()
				}

				tableThis := source.This()
				tableAlias := source.ArgE("alias")
				var functionColumns []any
				if tableThis.IsA(KFunc) {
					if tableAlias == nil {
						if c, ok := dialect.S.DEFAULT_FUNCTIONS_COLUMN_NAMES[tableThis.Kind()]; ok {
							functionColumns = []any{c}
						}
					} else if columns := tableAlias.ArgL("columns"); len(columns) > 0 {
						for _, c := range columns {
							functionColumns = append(functionColumns, c)
						}
					} else if _, ok := dialect.S.DEFAULT_FUNCTIONS_COLUMN_NAMES[tableThis.Kind()]; ok {
						functionColumns = []any{source.AliasOrName()}
						source.Set("alias", nil)
						name = ""
					}
				}

				targetAlias := name
				if targetAlias == "" {
					targetAlias = source.Name()
				}
				setAlias(source, canonicalAliases, targetAlias, nil, true, functionColumns)

				partNames := []string{}
				for _, p := range source.Parts() {
					partNames = append(partNames, p.Name())
				}
				sourceFQN := strings.Join(partNames, ".")
				hadExplicitAlias := tableAlias != nil && tableAlias.Name() != ""
				if _, seen := tableAliases[sourceFQN]; !hadExplicitAlias || !seen {
					tableAliases[sourceFQN] = source.ArgE("alias").This().Copy()
				}

				if pivot != nil {
					pivotTarget := ""
					if pivot.ArgB("unpivot") {
						pivotTarget = source.Alias()
					}
					setAlias(pivot, canonicalAliases, pivotTarget, nil, true, nil)

					// This case corresponds to a pivoted CTE, we don't want to qualify that
					if s, ok := scope.Sources.Get(source.AliasOrName()); ok && s.Scope != nil {
						continue
					}
				}

				if isRealTableSource {
					qualify(source)

					if onQualify != nil {
						onQualify(source)
					}
				}
			} else if source.Scope != nil && source.Scope.IsUDTF() {
				udtf := source.Scope.Expression
				setAlias(udtf, canonicalAliases, "", nil, false, nil)

				tableAlias := udtf.ArgE("alias")

				if udtf.IsA(KValues) && len(tableAlias.ArgL("columns")) == 0 {
					var columnAliases []*Expr
					for _, i := range qtGenerateValuesAliases(dialect, udtf) {
						columnAliases = append(columnAliases, NormalizeIdentifiers(i, dialect))
					}
					tableAlias.Set("columns", columnAliases)
				}
			}
		}

		for _, table := range scope.Tables() {
			if table.Alias() == "" && table.Parent().IsA(KFrom, KJoin) {
				setAlias(table, canonicalAliases, table.Name(), nil, false, nil)
			}
		}

		for _, column := range localColumns {
			columnTable := column.TableName()

			if column.DbName() != "" {
				parts := column.Parts()
				names := []string{}
				for _, p := range parts[:len(parts)-1] {
					names = append(names, p.Name())
				}
				tableAlias := tableAliases[strings.Join(names, ".")]

				if tableAlias != nil {
					for _, p := range []string{"table", "db", "catalog"} {
						column.Set(p, nil)
					}

					column.Set("table", tableAlias.Copy())
				}
			} else if len(canonicalAliases) > 0 && columnTable != "" {
				// Amend existing aliases, e.g. t.c -> _0.c if t is aliased to _0
				if canonicalTable := canonicalAliases[columnTable]; canonicalTable != columnTable {
					column.Set("table", ToIdentifier(canonicalTable, nil))
				}
			}
		}
	}

	return expression
}

// qtStar mirrors the Star produced by parsing "*" (as in exp.select("*")).
func qtStar() *Expr {
	return New(KStar, "ilike", nil, "except_", nil, "replace", nil, "rename", nil)
}

// qtGenerateValuesAliases mirrors Dialect.generate_values_aliases(expression).
func qtGenerateValuesAliases(d *Dialect, e *Expr) []*Expr {
	if d.hooks != nil && d.hooks.generateValuesAliases != nil {
		return d.hooks.generateValuesAliases(d, e)
	}
	var out []*Expr
	for i := range e.Expressions()[0].Expressions() {
		out = append(out, ToIdentifier("_col_"+itoa(i), nil))
	}
	return out
}
