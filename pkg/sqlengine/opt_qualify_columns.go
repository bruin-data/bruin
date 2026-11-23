package sqlengine

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// QualifyColumns rewrites the AST to have fully qualified columns.
//
// inferSchema TriNone means "infer the schema iff it is empty". A nil schema is replaced by an
// empty MappingSchema for d; d is otherwise unused (the schema's dialect drives the
// qualification).
//
// Notes:
//   - Currently only handles a single PIVOT or UNPIVOT operator.
func QualifyColumns(expression *Expr, schema *MappingSchema, expandAliasRefs, expandStars bool, inferSchema Tri, allowPartialQualification bool, d *Dialect) *Expr {
	if schema == nil {
		schema = NewMappingSchema(nil, nil, d, true, nil)
	}

	// The TypeAnnotator for schema is created lazily: it is stateless until first used.
	var annotator *TypeAnnotator
	getAnnotator := func() *TypeAnnotator {
		if annotator == nil {
			annotator = newTypeAnnotator(schema, nil, nil, nil, true)
		}
		return annotator
	}

	infer := inferSchema == TriTrue
	if inferSchema == TriNone {
		infer = schema.Empty()
	}
	dialect := schema.Dialect()
	if dialect == nil {
		dialect = prototype("")
	}
	pseudocolumns := dialect.S.PSEUDOCOLUMNS

	for _, scope := range TraverseScope(expression) {
		if dialect.S.PREFER_CTE_ALIAS_COLUMN {
			PushdownCTEAliasColumns(scope)
		}

		scopeExpression := scope.Expression
		isSelect := scopeExpression.IsA(KSelect)

		qcSeparatePseudocolumns(scope, pseudocolumns)

		resolver := NewResolver(scope, schema, infer)
		qcPopTableColumnAliases(scope.CTEs())
		qcPopTableColumnAliases(scope.DerivedTables())
		usingColumnTables, usingColumns := qcExpandUsing(scope, resolver)

		if (schema.Empty() || dialect.S.FORCE_EARLY_ALIAS_REF_EXPANSION) && expandAliasRefs {
			qcExpandAliasRefs(scope, resolver, dialect, dialect.S.EXPAND_ONLY_GROUP_ALIAS_REF)
		}

		qcConvertColumnsToDots(scope, resolver)
		qcQualifyColumns(scope, resolver, allowPartialQualification)

		// Columns just qualified in place may have been classified as external
		scope.ClearColumnCache()

		if !schema.Empty() && expandAliasRefs {
			qcExpandAliasRefs(scope, resolver, dialect, false)
		}

		if isSelect {
			if expandStars {
				qcExpandStars(scope, resolver, usingColumnTables, usingColumns, pseudocolumns, getAnnotator)
			}
			QualifyOutputsScope(scope, dialect)
		}

		qcExpandGroupBy(scope, dialect)

		// DISTINCT ON and ORDER BY follow the same rules (tested in DuckDB, Postgres, ClickHouse)
		// https://www.postgresql.org/docs/current/sql-select.html#SQL-DISTINCT
		qcExpandOrderByAndDistinctOn(scope, resolver)

		if dialect.S.ANNOTATE_ALL_SCOPES {
			getAnnotator().annotateScope(scope)
		}
	}

	return expression
}

// ValidateQualifyColumns panics with an OptimizeError if any columns aren't qualified. When sql is
// not "", the message quotes it with the offending column highlighted.
func ValidateQualifyColumns(expression *Expr, sql string) *Expr {
	var allUnqualifiedColumns []*Expr
	for _, scope := range TraverseScope(expression) {
		if scope.Expression.IsA(KSelect) {
			unqualifiedColumns := scope.UnqualifiedColumns()

			if len(scope.ExternalColumns()) > 0 && !scope.IsCorrelatedSubquery() && len(scope.Pivots()) == 0 {
				column := scope.ExternalColumns()[0]
				forTable := ""
				if column.TableName() != "" {
					forTable = fmt.Sprintf(" for table: '%s'", column.TableName())
				}
				line, col, start, end := qcPositions(column.This())

				errorMsg := fmt.Sprintf("Column '%s' could not be resolved%s.", column.Name(), forTable)
				if truthy(line) && truthy(col) {
					errorMsg += fmt.Sprintf(" Line: %v, Col: %v", line, col)
				}
				if sql != "" && start != nil && end != nil {
					formattedSQL := qcHighlight(sql, start, end)
					errorMsg += "\n  " + formattedSQL
				}

				panic(&OptimizeError{Msg: errorMsg})
			}

			allUnqualifiedColumns = append(allUnqualifiedColumns, unqualifiedColumns...)
		}
	}

	if len(allUnqualifiedColumns) > 0 {
		firstColumn := allUnqualifiedColumns[0]
		line, col, start, end := qcPositions(firstColumn.This())

		errorMsg := fmt.Sprintf("Ambiguous column '%s'", firstColumn.Name())
		if truthy(line) && truthy(col) {
			errorMsg += fmt.Sprintf(" (Line: %v, Col: %v)", line, col)
		}
		if sql != "" && start != nil && end != nil {
			formattedSQL := qcHighlight(sql, start, end)
			errorMsg += "\n  " + formattedSQL
		}

		panic(&OptimizeError{Msg: errorMsg})
	}

	return expression
}

func qcPositions(e *Expr) (line, col, start, end any) {
	if e == nil {
		return nil, nil, nil, nil
	}
	return e.MetaGet("line"), e.MetaGet("col"), e.MetaGet("start"), e.MetaGet("end")
}

func qcHighlight(sql string, start, end any) string {
	s, _ := start.(int)
	e, _ := end.(int)
	formatted, _, _, _ := highlightSQL([]rune(sql), [][2]int{{s, e}}, errorMessageContextDefault)
	return formatted
}

func qcSeparatePseudocolumns(scope *Scope, pseudocolumns StrSet) {
	if len(pseudocolumns) == 0 {
		return
	}

	hasPseudocolumns := false
	scopeExpression := scope.Expression

	for _, column := range scope.Columns() {
		name := pyUpper(column.Name())
		if !pseudocolumns.Has(name) {
			continue
		}

		if name != "LEVEL" || (scopeExpression.IsA(KSelect) && scopeExpression.ArgB("connect")) {
			kv := []any{}
			for _, k := range column.ArgKeys() {
				kv = append(kv, k, column.Arg(k))
			}
			column.Replace(New(KPseudocolumn, kv...))
			hasPseudocolumns = true
		}
	}

	if hasPseudocolumns {
		scope.ClearCache()
	}
}

// qcPopTableColumnAliases removes table column aliases (except on recursive CTEs).
//
// For example, `col1` and `col2` will be dropped in SELECT ... FROM (SELECT ...) AS foo(col1, col2).
func qcPopTableColumnAliases(derivedTables []*Expr) {
	for _, derivedTable := range derivedTables {
		if derivedTable.Parent().IsA(KWith) && derivedTable.Parent().ArgB("recursive") {
			continue
		}
		if tableAlias := derivedTable.ArgE("alias"); tableAlias != nil {
			tableAlias.Set("columns", nil)
		}
	}
}

// usingColumnTables maps each automatically joined (USING / NATURAL) column name to the ordered
// set of source names that provide it.
type usingColumnTables = omap[*omap[struct{}]]

// qcExpandUsing rewrites USING and NATURAL joins as ON joins and replaces unqualified references
// to their columns with COALESCE over the joined sources. It returns the sources of each such
// column and the USING columns that star expansion lists first (each join's columns preceding
// those of earlier joins; empty when they keep their position).
func qcExpandUsing(scope *Scope, resolver *Resolver) (*usingColumnTables, []string) {
	columns := newOMap[string]()

	updateSourceColumns := func(sourceName string) {
		for _, columnName := range resolver.GetSourceColumns(sourceName, false) {
			if !columns.Has(columnName) {
				columns.Set(columnName, sourceName)
			}
		}
	}

	var joins []*Expr
	for j := range scope.FindAll(KJoin) {
		joins = append(joins, j)
	}
	if len(joins) == 0 {
		return newOMap[*omap[struct{}]](), nil
	}

	names := newStrSet()
	var namesOrdered []string
	for _, join := range joins {
		if n := join.AliasOrName(); !names.Has(n) {
			names.Add(n)
			namesOrdered = append(namesOrdered, n)
		}
	}
	var ordered []string
	for _, key := range scope.SelectedSources().Keys() {
		if !names.Has(key) {
			ordered = append(ordered, key)
		}
	}

	if len(names) > 0 && len(ordered) == 0 {
		reprs := make([]string, len(namesOrdered))
		for i, n := range namesOrdered {
			reprs[i] = pyRepr(n)
		}
		panic(&OptimizeError{Msg: fmt.Sprintf("Joins {%s} missing source table %s", strings.Join(reprs, ", "), chunkFPyStr(scope.Expression))})
	}

	columnTables := newOMap[*omap[struct{}]]()

	anyUsing := false
	for _, join := range joins {
		if join.ArgB("using") || join.MethodText() == "NATURAL" {
			anyUsing = true
			break
		}
	}
	var usingColumns []string
	if !anyUsing {
		return columnTables, usingColumns
	}

	for _, sourceName := range ordered {
		updateSourceColumns(sourceName)
	}

	columnOrder := resolver.dialect.S.USING_COLUMN_ORDER
	moveUsingColumns := columnOrder != "IN_PLACE"

	for i, join := range joins {
		sourceTable := ordered[len(ordered)-1]
		if sourceTable != "" {
			updateSourceColumns(sourceTable)
		}

		joinTable := join.AliasOrName()
		ordered = append(ordered, joinTable)

		joinColumns := resolver.GetSourceColumns(joinTable, false)

		usingArg := join.Arg("using")
		using := join.ArgL("using")
		if !(len(using) > 0 || join.ArgB("on") || join.MethodText() != "" || join.SideText() != "" || join.KindText() != "") {
			// `,` binds looser than JOIN, so in `a, b JOIN c USING (k)` k only moves ahead of b's
			// columns, not a's. That isn't modeled, so after a comma join the USING columns stay
			// in place
			moveUsingColumns = false
		}

		// The order in which the left side outputs its columns
		leftColumns := append([]string{}, usingColumns...)
		for _, c := range columns.Keys() {
			if !containsStr(usingColumns, c) {
				leftColumns = append(leftColumns, c)
			}
		}

		if usingArg == nil && join.MethodText() == "NATURAL" {
			// A NATURAL JOIN is a USING join over the columns common to both sides, in the order
			// the left side outputs them; when those can't be determined (unknown schema, no
			// common columns), NATURAL stays
			if columns.Len() > 0 && !columns.Has("*") && len(joinColumns) > 0 && !containsStr(joinColumns, "*") {
				using = nil
				for _, columnName := range leftColumns {
					if containsStr(joinColumns, columnName) {
						using = append(using, ToIdentifier(columnName, nil))
					}
				}
				if len(using) > 0 {
					join.Set("method", nil)
				}
			}
		}
		if len(using) == 0 {
			continue
		}

		var conditions []*Expr
		usingIdentifierCount := len(using)
		isSemiOrAntiJoin := join.IsSemiOrAntiJoin()

		if moveUsingColumns {
			var joinKeys []string
			for _, identifier := range using {
				joinKeys = append(joinKeys, identifier.Name())
			}
			if columnOrder == "LEFT_TABLE" {
				var keys []string
				for _, name := range leftColumns {
					if containsStr(joinKeys, name) {
						keys = append(keys, name)
					}
				}
				joinKeys = keys
			}
			for _, name := range usingColumns {
				if !containsStr(joinKeys, name) {
					joinKeys = append(joinKeys, name)
				}
			}
			usingColumns = joinKeys
		}

		for _, identifierExpr := range using {
			identifier := identifierExpr.Name()
			table, _ := columns.Get(identifier)

			if table == "" || !containsStr(joinColumns, identifier) {
				if (columns.Len() > 0 && !columns.Has("*")) && len(joinColumns) > 0 {
					panic(&OptimizeError{Msg: "Cannot automatically join: " + identifier})
				}
			}

			if table == "" {
				table = sourceTable
			}

			var lhs *Expr
			if tables, ok := columnTables.Get(identifier); ok && tables.Len() > 0 {
				// A key merged by an earlier join compares against all of its sources
				var coalesceColumns []*Expr
				for _, t := range tables.Keys() {
					coalesceColumns = append(coalesceColumns, ColumnExpr(identifier, t, nil, nil, nil, nil, true))
				}
				lhs = qcCoalesce(coalesceColumns)
			} else if i == 0 || usingIdentifierCount == 1 {
				lhs = ColumnExpr(identifier, table, nil, nil, nil, nil, true)
			} else {
				var coalesceColumns []*Expr
				for _, t := range ordered[:len(ordered)-1] {
					if containsStr(resolver.GetSourceColumns(t, false), identifier) {
						coalesceColumns = append(coalesceColumns, ColumnExpr(identifier, t, nil, nil, nil, nil, true))
					}
				}
				if len(coalesceColumns) > 1 {
					lhs = qcCoalesce(coalesceColumns)
				} else {
					lhs = ColumnExpr(identifier, table, nil, nil, nil, nil, true)
				}
			}

			conditions = append(conditions, qcEQ(lhs, ColumnExpr(identifier, joinTable, nil, nil, nil, nil, true)))

			// Do not update the map if this was a SEMI/ANTI join in
			// order to avoid generating COALESCE columns for this join pair
			if !isSemiOrAntiJoin {
				// The set's values are empty, because we only care about the key ordering
				tables, ok := columnTables.Get(identifier)
				if !ok {
					tables = newOMap[struct{}]()
					columnTables.Set(identifier, tables)
				}
				if !tables.Has(table) {
					tables.Set(table, struct{}{})
				}
				if !tables.Has(joinTable) {
					tables.Set(joinTable, struct{}{})
				}
			}
		}

		join.Set("using", nil)
		join.Set("on", AndExprOpts(conditions, false, true))
	}

	if columnTables.Len() > 0 {
		for _, column := range scope.Columns() {
			if column.TableName() == "" && columnTables.Has(column.Name()) {
				tables, _ := columnTables.Get(column.Name())
				var coalesceArgs []*Expr
				for _, table := range tables.Keys() {
					coalesceArgs = append(coalesceArgs, ColumnExpr(column.Name(), table, nil, nil, nil, nil, true))
				}
				replacement := qcCoalesce(coalesceArgs)

				if column.Parent().IsA(KSelect) {
					// Ensure the USING column keeps its name if it's projected
					replacement = AliasExpr(replacement, column.Name(), nil, false)
				} else if column.Parent().IsA(KStruct) {
					// Ensure the USING column keeps its name if it's an anonymous STRUCT field
					replacement = New(KPropertyEQ, "this", ToIdentifier(column.Name(), nil), "expression", replacement)
				}

				scope.Replace(column, replacement)
			}
		}
	}

	return columnTables, usingColumns
}

func qcCoalesce(args []*Expr) *Expr {
	converted := make([]*Expr, 0, len(args))
	for _, a := range args {
		converted = append(converted, a.Copy())
	}

	var function *Expr
	if len(converted) > 0 {
		function = New(KCoalesce, "this", converted[0], "expressions", converted[1:], "is_nvl", nil, "is_null", nil)
	} else {
		function = New(KCoalesce)
	}

	for _, msg := range function.ErrorMessages(converted) {
		panic(&ValueError{Msg: msg})
	}
	return function
}

func qcEQ(this, other *Expr) *Expr {
	this = this.Copy()
	other = other.Copy()
	if !this.IsA(KEQ) && !other.IsA(KEQ) {
		this = wrapIfKind(this, KBinary)
		other = wrapIfKind(other, KBinary)
	}
	return New(KEQ, "this", this, "expression", other)
}

type qcAliasRef struct {
	expr  *Expr
	index int
}

// qcExpandAliasRefs expands references to projection aliases.
//
// Example:
//
//	SELECT y.foo AS bar, bar * 2 AS baz FROM y
//	=> SELECT y.foo AS bar, y.foo * 2 AS baz FROM y
func qcExpandAliasRefs(scope *Scope, resolver *Resolver, dialect *Dialect, expandOnlyGroupby bool) {
	expression := scope.Expression

	if !expression.IsA(KSelect) || dialect.S.DISABLES_ALIAS_REF_EXPANSION {
		return
	}

	aliasToExpression := map[string]qcAliasRef{}
	projections := newStrSet()
	for _, s := range expression.Selects() {
		projections.Add(s.AliasOrName())
	}
	replaced := false

	replaceColumns := func(node *Expr, resolveTable bool, literalIndex bool) {
		isGroupBy := node.IsA(KGroup)
		isHaving := node.IsA(KHaving)
		isQualify := node.IsA(KQualify)
		if node == nil || (expandOnlyGroupby && !isGroupBy) {
			return
		}

		for column := range WalkInScope(node, func(n *Expr) bool { return n.IsStar() }) {
			if !column.IsA(KColumn) {
				continue
			}

			// BigQuery's GROUP BY allows alias expansion only for standalone names, e.g:
			//   SELECT FUNC(col) AS col FROM t GROUP BY col --> Can be expanded
			//   SELECT FUNC(col) AS col FROM t GROUP BY FUNC(col)  --> Shouldn't be expanded, will result to FUNC(FUNC(col))
			// This not required for the HAVING clause as it can evaluate expressions using both the alias & the table columns
			if expandOnlyGroupby && isGroupBy && column.Parent() != node {
				continue
			}

			skipReplace := false
			var table *Expr
			if resolveTable && column.TableName() == "" {
				table = resolver.GetTable(column)
			}
			ref, hasAlias := aliasToExpression[column.Name()]
			aliasExpr, i := ref.expr, ref.index
			if !hasAlias {
				aliasExpr, i = nil, 1
			}

			if aliasExpr != nil {
				// An aggregate alias must not be expanded into a GROUP BY or another (non-window) aggregate.
				skipReplace = FindInScope(aliasExpr, KAggFunc) != nil &&
					(isGroupBy || (column.FindAncestor(KAggFunc) != nil &&
						!column.FindAncestor(KWindow, KSelect).IsA(KWindow)))

				// BigQuery's having clause gets confused if an alias matches a source.
				// SELECT x.a, max(x.b) as x FROM x GROUP BY 1 HAVING x > 1;
				// If "HAVING x" is expanded to "HAVING max(x.b)", BQ would blindly replace the "x" reference with the projection MAX(x.b)
				// i.e HAVING MAX(MAX(x.b).b), resulting in the error: "Aggregations of aggregations are not allowed"
				if (isHaving || isQualify) && dialect.S.PROJECTION_ALIASES_SHADOW_SOURCE_NAMES {
					if !skipReplace {
						for c := range aliasExpr.FindAll(KColumn) {
							if projections.Has(c.Parts()[0].Name()) {
								skipReplace = true
								break
							}
						}
					}
				}
			} else if dialect.S.PROJECTION_ALIASES_SHADOW_SOURCE_NAMES && (isGroupBy || isHaving || isQualify) {
				columnTable := column.TableName()
				if table != nil {
					columnTable = table.Name()
				}
				if projections.Has(columnTable) {
					// BigQuery's GROUP BY and HAVING clauses get confused if the column name
					// matches a source name and a projection. For instance:
					// SELECT id, ARRAY_AGG(col) AS custom_fields FROM custom_fields GROUP BY id HAVING id >= 1
					// We should not qualify "id" with "custom_fields" in either clause, since the aggregation shadows the actual table
					// and we'd get the error: "Column custom_fields contains an aggregation function, which is not allowed in GROUP BY clause"
					column.Replace(ToIdentifier(column.Name(), nil))
					replaced = true
					return
				}
			}

			if table != nil && (aliasExpr == nil || skipReplace) {
				column.Set("table", table)
			} else if column.TableName() == "" && aliasExpr != nil && !skipReplace {
				// In e.g. SELECT x + 1 AS y FROM read_parquet('f') WHERE y > 1, y may also be a
				// column of a source whose columns are unknown (a table function, or a table
				// missing from the schema). Such sources are common, so the alias is assumed.
				if (aliasExpr.IsA(KLiteral) || aliasExpr.IsNumber()) && (literalIndex || resolveTable) {
					if literalIndex {
						column.Replace(LiteralInt(i))
						replaced = true
					}
				} else {
					replaced = true
					column = column.Replace(ParenExpr(aliasExpr, true))
					simplified := SimplifyParens(column, dialect)
					if simplified != column {
						column.Replace(simplified)
						column = simplified
					}

					if resolveTable {
						for inner := range WalkInScope(column, nil) {
							if inner.IsA(KColumn) && inner.TableName() == "" {
								if innerTable := resolver.GetTable(inner); innerTable != nil {
									inner.Set("table", innerTable)
								}
							}
						}
					}
				}
			}
		}
	}

	for i, projection := range expression.Selects() {
		replaceColumns(projection, false, false)
		if projection.IsA(KAlias) {
			aliasToExpression[projection.Alias()] = qcAliasRef{projection.This(), i + 1}
		}
	}

	childScope := scope
	parentScope := scope
	onRightSubTree := false
	for parentScope != nil && !parentScope.IsCTE() {
		childScope = parentScope
		parentScope = parentScope.Parent
		if parentScope != nil {
			if parentScope.Expression.IsA(KUnion) {
				// Access the arg directly instead of the right property, because set operation
				// operands aren't guaranteed to be Query nodes, e.g. SELECT 1 UNION ALL VALUES (2).
				// Unnest to see through parenthesized operands, whose scope is the inner query
				onRightSubTree = unnestMethod(parentScope.Expression.Expression()) == childScope.Expression
			}
		}
	}

	// We shouldn't expand aliases if they match the recursive CTE's columns
	// and we are in the recursive part (right sub tree) of the CTE
	if parentScope != nil && onRightSubTree {
		if cte := parentScope.Expression.Parent(); cte != nil {
			with := cte.FindAncestor(KWith)
			if with != nil && with.ArgB("recursive") {
				recursiveColumns := cte.ArgE("alias").ArgL("columns")
				if len(recursiveColumns) == 0 {
					recursiveColumns = cte.This().Selects()
				}
				for _, recursiveCTEColumn := range recursiveColumns {
					delete(aliasToExpression, recursiveCTEColumn.OutputName())
				}
			}
		}
	}

	replaceColumns(expression.ArgE("where"), false, false)
	replaceColumns(expression.ArgE("group"), false, true)
	replaceColumns(expression.ArgE("having"), true, false)
	replaceColumns(expression.ArgE("qualify"), true, false)

	if dialect.S.SUPPORTS_ALIAS_REFS_IN_JOIN_CONDITIONS {
		for _, join := range expression.ArgL("joins") {
			replaceColumns(join, false, false)
		}
	}

	if replaced {
		scope.ClearCache()
	}
}

func qcExpandGroupBy(scope *Scope, dialect *Dialect) {
	expression := scope.Expression
	group := expression.ArgE("group")
	if group == nil {
		return
	}

	group.Set("expressions", qcExpandPositionalReferences(scope, group.Expressions(), dialect, false))
	expression.Set("group", group)
}

func qcExpandOrderByAndDistinctOn(scope *Scope, resolver *Resolver) {
	expression := scope.Expression

	if !expression.IsA(KSelectable) {
		return
	}

	for _, modifierKey := range []string{"order", "distinct"} {
		modifier := expression.ArgE(modifierKey)
		if modifier.IsA(KDistinct) {
			modifier = modifier.ArgE("on")
		}

		if modifier == nil {
			continue
		}

		modifierExpressions := modifier.Expressions()
		if modifierKey == "order" {
			thisList := make([]*Expr, len(modifierExpressions))
			for i, ordered := range modifierExpressions {
				thisList[i] = ordered.This()
			}
			modifierExpressions = thisList
		}

		expandedList := qcExpandPositionalReferences(scope, modifierExpressions, resolver.dialect, true)
		for i := 0; i < len(modifierExpressions) && i < len(expandedList); i++ {
			original, expanded := modifierExpressions[i], expandedList[i]
			for agg := range original.FindAll(KAggFunc) {
				for col := range agg.FindAll(KColumn) {
					if col.TableName() == "" {
						if table := resolver.GetTable(col); table != nil {
							col.Set("table", table)
							delete(col.Meta(), "dot_parts")
						}
					}
				}
			}

			original.Replace(expanded)
		}

		if expression.ArgB("group") {
			type selectEntry struct{ key, value *Expr }
			var selects []selectEntry
			for _, s := range expression.Selects() {
				selects = append(selects, selectEntry{s.This(), ColumnExpr(s.AliasOrName(), nil, nil, nil, nil, nil, true)})
			}
			lookup := func(node *Expr) *Expr {
				// the last projection whose expression equals node wins
				for j := len(selects) - 1; j >= 0; j-- {
					if selects[j].key != nil && selects[j].key.Equal(node) {
						return selects[j].value
					}
				}
				return node
			}

			for _, node := range modifierExpressions {
				if node.IsInt() {
					node.Replace(ToIdentifier(qcSelectByPos(expression, node).Alias(), nil))
				} else {
					node.Replace(lookup(node))
				}
			}
		}
	}
}

func qcExpandPositionalReferences(scope *Scope, expressions []*Expr, dialect *Dialect, alias bool) []*Expr {
	newNodes := []*Expr{}
	var ambiguousProjections StrSet

	expression := scope.Expression

	if !expression.IsA(KSelectable) {
		return newNodes
	}

	for _, node := range expressions {
		if node.IsInt() && node.IsA(KLiteral) {
			sel := qcSelectByPos(expression, node)

			if alias {
				newNodes = append(newNodes, ColumnExpr(sel.ArgE("alias").Copy(), nil, nil, nil, nil, nil, true))
			} else {
				selectExpr := sel.This()

				ambiguous := false
				if dialect.S.PROJECTION_ALIASES_SHADOW_SOURCE_NAMES {
					if ambiguousProjections == nil {
						// When a projection name is also a source name and it is referenced in the
						// GROUP BY clause, BQ can't understand what the identifier corresponds to
						ambiguousProjections = newStrSet()
						for _, s := range expression.Selects() {
							if scope.SelectedSources().Has(s.AliasOrName()) {
								ambiguousProjections.Add(s.AliasOrName())
							}
						}
					}

					for column := range selectExpr.FindAll(KColumn) {
						if ambiguousProjections.Has(column.Parts()[0].Name()) {
							ambiguous = true
							break
						}
					}
				}

				if selectExpr.IsA(KLiteral, KBoolean, KNull) ||
					selectExpr.IsNumber() ||
					selectExpr.Find(KExplode, KUnnest) != nil ||
					ambiguous {
					newNodes = append(newNodes, node)
				} else {
					newNodes = append(newNodes, selectExpr.Copy())
				}
			}
		} else {
			newNodes = append(newNodes, node)
		}
	}

	return newNodes
}

// qcSelectByPos returns the projection at the 1-based position held by node, which must be an
// Alias. Positions below 1 count back from the end (0 is the last projection).
func qcSelectByPos(expression *Expr, node *Expr) *Expr {
	selects := expression.Selects()
	n, err := strconv.Atoi(strings.ReplaceAll(pyStrip(node.ThisS()), "_", ""))
	idx := n - 1
	if idx < 0 {
		idx += len(selects)
	}
	if err != nil || idx < 0 || idx >= len(selects) {
		panic(&OptimizeError{Msg: "Unknown output column: " + node.Name()})
	}
	return chunkFAssertIs(selects[idx], KAlias)
}

// qcConvertColumnsToDots converts `Column` instances that represent STRUCT or JSON field lookups
// into chained `Dot`s.
//
// These lookups may be parsed as columns (e.g. "col"."field"."field2"), but they need to be
// normalized to `Dot(Dot(...(<table>.<column>, field1), field2, ...))` to be qualified properly.
func qcConvertColumnsToDots(scope *Scope, resolver *Resolver) {
	converted := false
	scopeSources := scope.SelectedSources()
	semiOrAntiJoinTables := scope.SemiOrAntiJoinTables()
	joinColumns := map[string]StrSet{}
	all := append(append([]*Expr{}, scope.Columns()...), scope.Stars()...)
	for _, column := range all {
		if column.IsA(KDot) {
			continue
		}

		columnTableName := column.TableName()
		isStar := column.This().IsA(KStar)
		meta := column.Meta()
		dotParts, _ := meta["dot_parts"].([]string)
		delete(meta, "dot_parts")

		var rootResolver *Resolver
		if len(semiOrAntiJoinTables) > 0 && columnTableName != "" {
			root := column.ArgE("catalog")
			if root == nil {
				root = column.ArgE("db")
			}
			rootName := columnTableName
			if root != nil {
				rootName = root.Name()
			}
			if join := column.FindAncestor(KJoin, KSelect); semiOrAntiJoinTables.Has(rootName) &&
				join.IsA(KJoin) && join.IsSemiOrAntiJoin() && join.AliasOrName() == rootName {
				sourceColumns, ok := joinColumns[rootName]
				if !ok {
					sourceColumns = newStrSet(resolver.GetSourceColumns(rootName, false)...)
					joinColumns[rootName] = sourceColumns
				}
				field := column.This()
				if root != nil {
					field = column.Parts()[1]
				}
				if field.IsStar() || len(sourceColumns) == 0 || sourceColumns.Has(field.Name()) || sourceColumns.Has("*") {
					// The join's own columns win, a missing one falling back to a struct field
					if column.DbName() == "" {
						continue
					}
					rootResolver = resolver
				}
			}
		}

		if rootResolver == nil && !isStar && column.DbName() != "" && scopeSources.Has(columnTableName) {
			parts := column.Parts()
			root, field := parts[0], parts[1]
			for _, candidate := range append([]*Resolver{resolver}, resolver.OuterResolvers()...) {
				if !candidate.scope.SelectedSources().Has(root.Name()) {
					continue
				}
				if containsStr(candidate.GetSourceColumns(root.Name(), false), field.Name()) {
					rootResolver = candidate
				}
				break
			}
		}

		if columnTableName != "" &&
			(!scopeSources.Has(columnTableName) || (isStar && column.DbName() != "") || rootResolver != nil) &&
			(rootResolver != nil || isStar || scope.Parent == nil ||
				!scope.Parent.Sources.Has(columnTableName) || !scope.IsCorrelatedSubquery()) {
			parts := column.Parts()
			root, parts := parts[0], parts[1:]
			wasQualified := false

			// Correlated stars can't be deferred; qualified paths use their root source's resolver
			resolvers := []*Resolver{resolver}
			if isStar {
				resolvers = append(resolvers, resolver.OuterResolvers()...)
			} else if rootResolver != nil {
				resolvers = []*Resolver{rootResolver}
			}

			var columnTable *Expr
			for _, sourceResolver := range resolvers {
				selectedSources := sourceResolver.scope.SelectedSources()
				if column.DbName() == "" && selectedSources.Has(column.TableName()) {
					// The star is over a table, so it's expanded as is
					columnTable = nil
					break
				}

				if root.IsA(KIdentifier) && (rootResolver != nil || selectedSources.Has(root.Name())) {
					// The struct is already qualified, but we still need to change the AST
					columnTable = root
					if len(parts) == 0 {
						panic(&ValueError{Msg: "not enough values to unpack (expected at least 1, got 0)"})
					}
					root, parts = parts[0], parts[1:]
					wasQualified = true
					break
				}

				if isStar && !sourceResolver.AllColumns().Has(root.Name()) && sourceResolver.HasUnknownSources() {
					// An unknown source may own the star, so it's preserved instead of inferred
					columnTable = nil
					break
				}

				columnTable = sourceResolver.GetTable(chunkFAssertIs(root, KIdentifier))
				if columnTable != nil {
					break
				}
			}

			if columnTable != nil {
				converted = true

				// The column becomes the root of the Dots, so other scopes' references to it, e.g.
				// a correlated subquery's, stay in the tree
				placeholder := New(KNull)
				column.Replace(DotBuild(append([]*Expr{placeholder}, parts...)))
				column.Set("catalog", nil)
				column.Set("db", nil)
				column.Set("this", root)
				column.Set("table", ToIdentifierAny(columnTable, nil, true))

				if len(dotParts) > 0 {
					// Remove the actual column parts from the rest of dot parts
					skip := 1
					if wasQualified {
						skip = 2
					}
					rest := []string{}
					if skip < len(dotParts) {
						rest = append(rest, dotParts[skip:]...)
					}
					column.Meta()["dot_parts"] = rest
				}

				placeholder.Replace(column)
			}
		}
	}

	if converted {
		// We want to re-aggregate the converted columns, otherwise they'd be skipped in
		// an iteration over scope.Columns(), even though they shouldn't be
		scope.ClearCache()
	}
}

func qcQualifyColumns(scope *Scope, resolver *Resolver, allowPartialQualification bool) {
	for _, column := range scope.Columns() {
		columnTable := column.TableName()
		columnName := column.Name()

		if columnTable != "" && scope.Sources.Has(columnTable) {
			columnSource, _ := scope.Sources.Get(columnTable)
			sourceColumns := resolver.GetSourceColumns(columnTable, false)
			// For pivoted sources, sourceColumns are pre-pivot; validate against the post-pivot set.
			var pivots []*Expr
			if columnSource.Table != nil {
				pivots = columnSource.Table.ArgL("pivots")
			}
			if len(pivots) > 0 && len(sourceColumns) > 0 && !containsStr(sourceColumns, "*") {
				// Each operator's input is the previous one's output
				for _, pivot := range pivots {
					sourceColumns = PivotOutputColumns(pivot, sourceColumns).Keys()
				}
			}
			if qcQualifyPositionalColumn(scope, resolver, column, columnTable, columnSource, sourceColumns, pivots, allowPartialQualification) {
				continue
			}
			columnName = column.Name()
			if !allowPartialQualification &&
				len(sourceColumns) > 0 &&
				!containsStr(sourceColumns, columnName) &&
				!containsStr(sourceColumns, "*") {
				panic(&OptimizeError{Msg: "Unknown column: " + columnName})
			}
		}

		if columnTable == "" {
			if pivots := scope.Pivots(); len(pivots) > 0 && column.FindAncestor(KPivot) == nil {
				if resolver.dialect.S.TABLE_ALIASES_SHADOW_COLUMN_NAMES {
					shadowed := false
					for _, pivot := range pivots {
						if pivot.Alias() == columnName {
							shadowed = true
							break
						}
					}
					if shadowed {
						// A whole-row reference to the pivot's alias wins over a same-named column
						scope.Replace(column, New(KTableColumn, "this", column.This()))
						continue
					}
				}

				// If the column is under the Pivot expression, we need to qualify it
				// using the name of the pivoted source instead of the pivot's alias
				column.Set("table", ToIdentifier(pivots[len(pivots)-1].Alias(), nil))
				continue
			}

			// columnTable can be "" because BigQuery UNNEST has no table alias
			table := resolver.GetTable(column)

			if table != nil {
				if source, ok := scope.Sources.Get(table.Name()); ok && source.Scope != nil {
					if _, inIndex := source.Scope.ColumnIndex()[column]; inIndex {
						continue
					}
				}
			}

			// With sources of unknown columns, a source's name may also be one of their columns.
			// The name of a function's output or of a source with unknown columns still refers to
			// that source (see qcIsOpaqueSource), even where outer columns are visible, since the
			// local source shadows them.
			if table != nil {
				column.Set("table", table)
			} else if resolver.dialect.S.TABLES_REFERENCEABLE_AS_COLUMNS &&
				len(column.Parts()) == 1 &&
				scope.SelectedSources().Has(columnName) &&
				(!resolver.HasUnknownSources() || qcIsOpaqueSource(scope, resolver, columnName)) {
				var outerTable *Expr
				if resolver.dialect.S.OUTER_COLUMNS_SHADOW_TABLE_ALIASES {
					outerTable = qcGetOuterColumnTable(column, resolver)
				}
				if outerTable != nil {
					column.Set("table", outerTable)
				} else {
					// BigQuery and Postgres allow tables to be referenced as columns, treating them as structs/records
					scope.Replace(column, New(KTableColumn, "this", column.This()))
				}
			} else if scope.CanBeCorrelated && resolver.HasUnknownTVFSources() {
				// The column may belong to the table function or to an outer source
				panic(&OptimizeError{Msg: fmt.Sprintf("Cannot resolve '%s' due to missing source schemas", columnName)})
			}
		}
	}

	pivots := scope.Pivots()

	// A chained operator's IN-list may name columns produced by earlier operators, which no
	// source exposes; the chain's accumulated output resolves them. Attribution is only
	// unambiguous when all pivots share one parent, i.e. there's a single pivoted source.
	singleChain := len(pivots) > 0 && pivots[0].Parent() == pivots[len(pivots)-1].Parent()
	produced := newStrSet()
	pivotedSource := ""
	if singleChain {
		pivotedSource = pivots[len(pivots)-1].Alias()
	}
	var available []string
	if scope.Sources.Has(pivotedSource) {
		available = resolver.GetSourceColumns(pivotedSource, false)
	}

	for _, pivot := range pivots {
		for column := range pivot.FindAll(KColumn) {
			if column.TableName() != "" {
				continue
			}
			if resolver.AllColumns().Has(column.Name()) {
				if table := resolver.GetTable(column); table != nil {
					column.Set("table", table)
				}
			} else if singleChain && produced.Has(column.Name()) {
				column.Set("table", ToIdentifier(pivotedSource, nil))
			}
		}

		if singleChain {
			available = PivotOutputColumns(pivot, available).Keys()
			produced.Add(available...)
		}
	}
}

// qcIsOpaqueSource reports whether the source called name is the output of a function (a table
// function, UNNEST or LATERAL) or has unknown columns. A reference to such a source's name reads
// that source, as a whole row or as its column of the same name (e.g. Postgres names a scalar
// function's only column after the source).
func qcIsOpaqueSource(scope *Scope, resolver *Resolver, name string) bool {
	source, ok := scope.Sources.Get(name)
	if !ok {
		return false
	}
	if (source.Scope != nil && source.Scope.IsUDTF()) || (source.Table.IsA(KTable) && source.Table.This().IsA(KFunc)) {
		return true
	}
	columns := resolver.GetSourceColumns(name, false)
	return len(columns) == 0 || containsStr(columns, "*")
}

// qcQualifyPositionalColumn resolves a qualified positional reference (`t.$N`, the Nth column of
// source t) to the column's name, in dialects that support them. It reports whether the column
// must be left as is, skipping any further qualification.
func qcQualifyPositionalColumn(scope *Scope, resolver *Resolver, column *Expr, columnTable string, columnSource Source,
	sourceColumns []string, pivots []*Expr, allowPartialQualification bool,
) bool {
	if !resolver.dialect.S.SUPPORTS_POSITIONAL_COLUMN_REFS || !column.This().IsA(KParameter) {
		return false
	}
	position := column.This().This()
	if !position.IsA(KLiteral) || !position.IsInt() {
		return false
	}

	// Pivots of unrelated sources may share this scope, so an exact output alias match wins; for
	// an aliasless chain, the last operator on the referenced source is used.
	var scopePivot *Expr
	for _, pivot := range scope.Pivots() {
		if pivot.Alias() == columnTable {
			scopePivot = pivot
			break
		}
	}
	if scopePivot == nil {
		scopePivots := scope.Pivots()
		for i := len(scopePivots) - 1; i >= 0; i-- {
			if parent := scopePivots[i].Parent(); parent != nil && parent.AliasOrName() == columnTable {
				scopePivot = scopePivots[i]
				break
			}
		}
	}

	if len(pivots) > 0 || scopePivot != nil || len(sourceColumns) == 0 || containsStr(sourceColumns, "*") {
		if scopePivot != nil {
			column.Set("table", ToIdentifier(scopePivot.Alias(), nil))
		}
		return true
	}

	positionValue, err := strconv.Atoi(strings.ReplaceAll(position.ThisS(), "_", ""))
	if err != nil {
		return true
	}

	var aliasColumns []string
	sourceColumnsIncomplete := false
	if columnSource.Table != nil {
		aliasColumns = columnSource.Table.AliasColumnNames()
		withoutAliases := resolver.schema.ColumnNames(columnSource.Table, true, nil, nil)
		sourceColumnsIncomplete = len(withoutAliases) == 0 || containsStr(withoutAliases, "*")
	}

	if len(aliasColumns) > 0 && positionValue > len(aliasColumns) && sourceColumnsIncomplete {
		return true
	}

	positionalColumns := resolver.GetSourceColumns(columnTable, true)
	if positionValue < 1 || positionValue > len(positionalColumns) {
		if allowPartialQualification {
			return true
		}
		panic(&OptimizeError{Msg: fmt.Sprintf("Positional reference $%d is out of range for source '%s'", positionValue, columnTable)})
	}

	positionalName := positionalColumns[positionValue-1]
	count := 0
	for _, name := range positionalColumns {
		if name == positionalName {
			count++
		}
	}
	if count > 1 {
		return true
	}

	var positionalIdentifier *Expr
	if columnSource.Scope != nil {
		sourceExpression := columnSource.Scope.Expression
		if !sourceExpression.IsA(KQuery) {
			return true
		}

		var selection *Expr
		if sourceSelects := sourceExpression.Selects(); positionValue <= len(sourceSelects) {
			selection = sourceSelects[positionValue-1]
		}
		sourceIdentifier := qcOutputIdentifier(selection)
		if sourceIdentifier == nil || sourceIdentifier.Name() != positionalName {
			return true
		}

		positionalIdentifier = sourceIdentifier.Copy()
	} else {
		positionalIdentifier = ToIdentifier(positionalName, nil)
		resolver.dialect.QuoteIdentifier(positionalIdentifier, false)
	}
	column.Set("this", positionalIdentifier)

	return false
}

// qcGetOuterColumnTable returns the outer source a subquery column refers to, in dialects where
// outer columns shadow the subquery's own table aliases; nil when no outer source has it or a
// source in between with the same name captures it.
func qcGetOuterColumnTable(column *Expr, resolver *Resolver) *Expr {
	columnName := column.Name()
	scope := resolver.scope
	shadowingSources := newStrSet(scope.SelectedSources().Keys()...)

	// Correlated columns can reach this scope from a descendant with shadowing aliases
	if _, ok := scope.ColumnIndex()[column]; !ok {
		var columnScope *Scope
		for _, inner := range scope.Traverse() {
			if _, ok := inner.ColumnIndex()[column]; ok {
				columnScope = inner
				break
			}
		}
		for columnScope != nil && columnScope != scope {
			shadowingSources.Add(columnScope.SelectedSources().Keys()...)
			columnScope = columnScope.Parent
		}
	}

	for _, outerResolver := range resolver.OuterResolvers() {
		sourceColumns := outerResolver.getAllSourceColumns()
		if scope.IsSubquery() {
			// A LATERAL's scope already holds the preceding sources it can reference
			join := scope.Expression.FindAncestor(KJoin, KSelect)
			if join.IsA(KJoin) && join.FindAncestor(KSelect) == outerResolver.scope.Expression {
				sourceColumns = outerResolver.getAvailableSourceColumns(join, true)
			}

			for _, columns := range sourceColumns.Values() {
				if containsStr(columns, columnName) {
					// A source in between with the same name would capture the qualified column
					table := outerResolver.getTable(columnName, column, sourceColumns)
					if table != nil && !shadowingSources.Has(table.Name()) {
						return table
					}
					return nil
				}
			}
		}

		scope = outerResolver.scope
		shadowingSources.Add(sourceColumns.Keys()...)
	}

	return nil
}

func qcIsType(e *Expr, dtype DType) bool {
	t := e.Type()
	return t != nil && DataTypeIsType(t, []any{dtype}, false)
}

// qcExpandStructStarsNoParens expands (flattens) foo.bar.* where bar is a struct column
// (BigQuery). It returns nil when the star can't be expanded.
func qcExpandStructStarsNoParens(expression *Expr) []*Expr {
	dotColumn := expression.Find(KColumn)
	if !dotColumn.IsA(KColumn) || !qcIsType(dotColumn, DT_STRUCT) {
		return nil
	}

	// All nested struct values are ColumnDefs, so normalize the first Column into one
	dotColumn = dotColumn.Copy()
	startingStruct := New(KColumnDef, "this", dotColumn.This(), "kind", dotColumn.Type())

	// First part is the table name and last part is the star so they can be dropped
	allParts := expression.Parts()
	var dotParts []*Expr
	if len(allParts) > 2 {
		dotParts = allParts[1 : len(allParts)-1]
	}

	// If we're expanding a nested struct eg. t.c.f1.f2.* find the last struct (f2 in this case)
	if len(dotParts) > 1 {
		for _, part := range dotParts[1:] {
			found := false
			for _, field := range startingStruct.ArgE("kind").Expressions() {
				// Unable to expand star unless all fields are named
				if !field.This().IsA(KIdentifier) {
					return nil
				}

				if field.Name() == part.Name() && field.ArgE("kind") != nil && DataTypeIsType(field.ArgE("kind"), []any{DT_STRUCT}, false) {
					startingStruct = field
					found = true
					break
				}
			}
			if !found {
				return nil
			}
		}
	}

	takenNames := newStrSet()
	var newSelections []*Expr

	for _, field := range startingStruct.ArgE("kind").Expressions() {
		name := field.Name()

		// Ambiguous or anonymous fields can't be expanded
		if takenNames.Has(name) || !field.This().IsA(KIdentifier) {
			return nil
		}

		takenNames.Add(name)

		this := field.This().Copy()
		var copied []*Expr
		for _, part := range dotParts {
			copied = append(copied, part.Copy())
		}
		copied = append(copied, this.Copy())
		root, rest := copied[0], copied[1:]
		fields := make([]any, len(rest))
		for i, p := range rest {
			fields[i] = p
		}
		var table any
		if t := dotColumn.ArgE("table"); t != nil {
			table = t
		}
		newColumn := ColumnExpr(root, table, nil, nil, fields, nil, true)
		newSelections = append(newSelections, chunkFAssertIs(AliasExpr(newColumn, this, nil, false), KAlias))
	}

	return newSelections
}

// qcExpandStructStarsWithParens expands (flattens) (<exp>.bar).*, where bar is a struct column
// (RisingWave). It returns nil when the star can't be expanded.
func qcExpandStructStarsWithParens(expression *Expr) []*Expr {
	if !expression.This().IsA(KParen) {
		return nil
	}

	dotColumn := expression.Find(KColumn)
	if !dotColumn.IsA(KColumn) || !qcIsType(dotColumn, DT_STRUCT) {
		return nil
	}

	parent := dotColumn.Parent()
	startingStruct := dotColumn.Type()

	// walk up AST and down into struct definition in sync
	for parent != nil {
		if parent.IsA(KParen) {
			parent = parent.Parent()
			continue
		}

		if !parent.IsA(KDot) {
			return nil
		}

		rhs := parent.Right()
		if rhs.IsA(KStar) {
			break
		}

		if !rhs.IsA(KIdentifier) {
			return nil
		}

		matched := false
		for _, structFieldDef := range startingStruct.Expressions() {
			if structFieldDef.Name() == rhs.Name() {
				matched = true
				startingStruct = structFieldDef.ArgE("kind") // update struct
				break
			}
		}

		if !matched {
			return nil
		}

		parent = parent.Parent()
	}

	var newSelections []*Expr

	outerParen := expression.This()

	for _, structFieldDef := range startingStruct.Expressions() {
		newIdentifier := structFieldDef.This().Copy()
		newDot := DotBuild([]*Expr{outerParen.Copy(), newIdentifier})
		newAlias := chunkFAssertIs(AliasExpr(newDot, newIdentifier, nil, false), KAlias)
		newSelections = append(newSelections, newAlias)
	}

	return newSelections
}

func qcExpandStars(scope *Scope, resolver *Resolver, usingColumnTables *usingColumnTables, usingColumns []string, pseudocolumns StrSet, annotator func() *TypeAnnotator) {
	var newSelections []*Expr
	dialect := resolver.dialect

	annotatedAhead := false
	if dialect.S.SUPPORTS_STRUCT_STAR_EXPANSION {
		for _, col := range scope.Stars() {
			if col.IsA(KDot) {
				annotatedAhead = true
				break
			}
		}
	}
	if annotatedAhead {
		// Found struct expansion, annotate scope ahead of time
		annotator().annotateScope(scope)
	}

	scopeExpression := scope.Expression

	if !scopeExpression.IsA(KSelectable) {
		return
	}

	for _, expression := range scopeExpression.Selects() {
		// EXCEPT, REPLACE and RENAME modifiers apply to the star they belong to, keyed by the
		// names of the sources it reads.
		var tables []string
		exceptColumns := map[string]StrSet{}
		replaceColumns := map[string]map[string]*Expr{}
		renameColumns := map[string]map[string]*Expr{}
		ilikePattern := ""
		isUnqualifiedStar := expression.IsA(KStar)
		coalescedColumns := newStrSet()

		if isUnqualifiedStar {
			// Only a string literal ILIKE pattern can filter the expansion at optimization time
			if ilike := expression.ArgE("ilike"); ilike != nil && !ilike.IsString() {
				newSelections = append(newSelections, expression)
				continue
			}

			tables = append(tables, scope.SelectedSources().Keys()...)
			qcAddExceptColumns(expression, tables, exceptColumns)
			qcAddReplaceColumns(expression, tables, replaceColumns)
			qcAddRenameColumns(expression, tables, renameColumns)
			ilikePattern = qcAddIlikeColumns(expression)
		} else if expression.IsStar() {
			if expression.IsA(KColumn) {
				if ilike := expression.This().ArgE("ilike"); ilike != nil && !ilike.IsString() {
					newSelections = append(newSelections, expression)
					continue
				}

				tables = append(tables, expression.TableName())
				qcAddExceptColumns(expression.This(), tables, exceptColumns)
				qcAddReplaceColumns(expression.This(), tables, replaceColumns)
				qcAddRenameColumns(expression.This(), tables, renameColumns)
				ilikePattern = qcAddIlikeColumns(expression.This())
			} else if expression.IsA(KDot) {
				var structFields []*Expr
				if dialect.S.REQUIRES_PARENTHESIZED_STRUCT_ACCESS {
					structFields = qcExpandStructStarsWithParens(expression)
				} else if dialect.S.SUPPORTS_STRUCT_STAR_EXPANSION {
					structFields = qcExpandStructStarsNoParens(expression)
				}

				if len(structFields) > 0 {
					if annotatedAhead {
						annotator().uncache(expression, true)
					}

					newSelections = append(newSelections, structFields...)
					continue
				}
			}
		}

		if len(tables) == 0 {
			newSelections = append(newSelections, expression)
			continue
		}

		start := len(newSelections)
		// The selections of the USING columns, which go first, in order
		usingSelections := newOMap[*Expr]()
		if isUnqualifiedStar {
			for _, name := range usingColumns {
				usingSelections.Set(name, nil)
			}
		}

		var ilikeRe *regexp.Regexp
		for _, table := range tables {
			source, ok := scope.Sources.Get(table)
			sourceResolver := resolver
			var pivots []*Expr
			pivotsKnown := false
			sourceTable := table

			if !ok {
				// The final alias of a (UN)PIVOT chain names the resulting source, but only the
				// underlying source is registered, so it is resolved through the chain's parent.
				// Attribution is only unambiguous for a single chain (all pivots share a parent)
				chain := scope.Pivots()
				var parent *Expr
				if len(chain) > 0 && chain[0].Parent() == chain[len(chain)-1].Parent() && chain[len(chain)-1].Alias() == table {
					parent = chain[len(chain)-1].Parent()
				}
				if parent != nil {
					pivots, pivotsKnown = chain, true
					sourceTable = parent.AliasOrName()
					source, ok = scope.Sources.Get(sourceTable)
				}

				if !ok {
					// Correlated stars, e.g. (SELECT AS STRUCT x.* EXCEPT (a)), expand an outer source
					resolved := false
					for _, r := range append([]*Resolver{resolver}, resolver.OuterResolvers()...) {
						sourceResolver = r
						source, ok = r.scope.Sources.Get(table)
						if ok || r.HasUnknownSources() {
							resolved = true
							break
						}
					}
					if !resolved {
						panic(&OptimizeError{Msg: "Unknown table: " + table})
					}
				}

				if !ok {
					// An unknown source may own the star, so it's preserved instead of expanded
					newSelections = append(newSelections, expression)
					break
				}
			}

			columns := sourceResolver.GetSourceColumns(sourceTable, true)
			if len(columns) == 0 {
				columns = scope.OuterColumns
			}

			if len(pseudocolumns) > 0 && dialect.S.EXCLUDES_PSEUDOCOLUMNS_FROM_STAR {
				var filtered []string
				for _, name := range columns {
					if !pseudocolumns.Has(pyUpper(name)) {
						filtered = append(filtered, name)
					}
				}
				columns = filtered
			}

			// A source exposing duplicate output names (e.g. a derived table re-exposing colliding
			// star-expanded columns) would make the expanded projections ambiguous, so the star
			// is left as is.
			if len(columns) == 0 || containsStr(columns, "*") || len(dedupeStrings(columns)) != len(columns) {
				return
			}

			columnsToExclude := exceptColumns[table]
			renamedColumns := renameColumns[table]
			replacedColumns := replaceColumns[table]

			// Preserve case-sensitivity of quoted source columns when expanding stars,
			// so the generated alias isn't folded by dialect normalization
			quotedColumns := newStrSet()
			if source.Scope != nil && source.Scope.Expression.IsA(KQuery) {
				for _, s := range source.Scope.Expression.Selects() {
					if qcOutputIdentifierQuoted(s) {
						quotedColumns.Add(s.OutputName())
					}
				}
			}

			// The operators belong to a specific source, so a star over a source joined
			// alongside it expands from that source's own columns
			if !pivotsKnown {
				var selectedNode *Expr
				if selected, ok := scope.SelectedSources().Get(table); ok {
					selectedNode = selected.Node
				}
				if selectedNode == nil && source.Table != nil {
					// A pivoted CTE reference is registered under the pivot's alias, which isn't
					// one of the selected sources
					selectedNode = source.Table
				}
				pivots = selectedNode.ArgL("pivots")
			}

			if len(pivots) > 0 {
				// Each operator consumes the previous one's output, so they are folded in order
				pivotColumns := columns
				for _, pivot := range pivots {
					if out := PivotOutputColumns(pivot, pivotColumns).Keys(); len(out) > 0 {
						pivotColumns = out
					} else {
						pivotColumns = pivot.AliasColumnNames()
					}
				}

				if len(pivotColumns) > 0 {
					var pivotTable any
					if a := pivots[len(pivots)-1].Alias(); a != "" {
						pivotTable = a
					}
					for _, name := range pivotColumns {
						if !columnsToExclude.Has(name) {
							newSelections = append(newSelections, AliasExpr(ColumnExpr(name, pivotTable, nil, nil, nil, nil, true), name, nil, false))
						}
					}
					continue
				}
			}

			for _, name := range columns {
				if columnsToExclude.Has(name) || coalescedColumns.Has(name) {
					continue
				}
				if ilikePattern != "" {
					if ilikeRe == nil {
						ilikeRe = regexp.MustCompile("(?i)^(?:" + ilikePattern + ")$")
					}
					if !ilikeRe.MatchString(name) {
						continue
					}
				}

				var selection *Expr
				if usingTables, isUsing := usingColumnTables.Get(name); isUnqualifiedStar && isUsing && usingTables.Has(table) {
					coalescedColumns.Add(name)
					var coalesceArgs []*Expr
					for _, t := range usingTables.Keys() {
						coalesceArgs = append(coalesceArgs, ColumnExpr(name, t, nil, nil, nil, nil, true))
					}

					selection = AliasExpr(qcCoalesce(coalesceArgs), name, nil, false)
				} else {
					// if it has characters that the dialect would have changed, infer that it was quoted.
					quoted := quotedColumns.Has(name) || (source.Table != nil && dialect.CaseSensitive(name))
					selection = ColumnExpr(name, table, nil, nil, nil, boolp(quoted), true)
				}

				if replaced := replacedColumns[name]; replaced != nil {
					selection = replaced
				}
				if alias := renamedColumns[name]; alias != nil {
					selection = AliasExpr(selection, alias, nil, true)
				}

				if pending, ok := usingSelections.Get(name); ok && pending == nil {
					usingSelections.Set(name, selection)
				} else {
					newSelections = append(newSelections, selection)
				}
			}
		}

		var first []*Expr
		for _, selection := range usingSelections.Values() {
			if selection != nil {
				first = append(first, selection)
			}
		}
		if len(first) > 0 {
			newSelections = append(newSelections[:start], append(first, newSelections[start:]...)...)
		}

		if annotatedAhead {
			// The star projection was replaced by the expansions above
			annotator().uncache(expression, true)
		}
	}

	// Ensures we don't overwrite the initial selections with an empty list
	if len(newSelections) > 0 && scopeExpression.IsA(KSelect) {
		if annotatedAhead {
			// The mutation below would otherwise be skipped by the final annotation pass
			annotator().uncache(scopeExpression, false)
		}

		scopeExpression.Set("expressions", newSelections)
		// The parent scope sees this scope's columns, so they must reflect the expansions
		scope.ClearCache()
	}
}

func qcOutputIdentifier(selection *Expr) *Expr {
	var identifier *Expr
	if selection.IsA(KAlias) {
		identifier = selection.ArgE("alias")
	} else if selection.IsA(KColumn) {
		identifier = selection.This()
	}

	if !identifier.IsA(KIdentifier) {
		return nil
	}
	return identifier
}

func qcOutputIdentifierQuoted(selection *Expr) bool {
	return qcOutputIdentifier(selection).ArgB("quoted")
}

// qcAddIlikeColumns converts the ILIKE pattern of a star (% and _ wildcards) to a regular
// expression, or returns "" when the star has none.
func qcAddIlikeColumns(expression *Expr) string {
	ilike := expression.ArgE("ilike")

	if ilike == nil {
		return ""
	}

	var b strings.Builder
	for _, c := range ilike.Name() {
		switch c {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	return b.String()
}

func qcAddExceptColumns(expression *Expr, tables []string, exceptColumns map[string]StrSet) {
	except := expression.ArgL("except_")

	if len(except) == 0 {
		return
	}

	columns := newStrSet()
	qualifiedColumns := map[string]StrSet{}
	for _, e := range except {
		// A qualified exclusion only applies to the source it references
		if e.IsA(KColumn) && e.TableName() != "" {
			if qualifiedColumns[e.TableName()] == nil {
				qualifiedColumns[e.TableName()] = newStrSet()
			}
			qualifiedColumns[e.TableName()].Add(e.Name())
		} else {
			columns.Add(e.Name())
		}
	}

	for _, table := range tables {
		tableColumns := columns
		if qualified := qualifiedColumns[table]; len(qualified) > 0 {
			tableColumns = newStrSet()
			for c := range columns {
				tableColumns.Add(c)
			}
			for c := range qualified {
				tableColumns.Add(c)
			}
		}

		if len(tableColumns) > 0 {
			exceptColumns[table] = tableColumns
		}
	}
}

func qcAddRenameColumns(expression *Expr, tables []string, renameColumns map[string]map[string]*Expr) {
	rename := expression.ArgL("rename")

	if len(rename) == 0 {
		return
	}

	columns := map[string]*Expr{}
	for _, e := range rename {
		columns[e.This().Name()] = e.ArgE("alias")
	}

	for _, table := range tables {
		renameColumns[table] = columns
	}
}

func qcAddReplaceColumns(expression *Expr, tables []string, replaceColumns map[string]map[string]*Expr) {
	replace := expression.ArgL("replace")

	if len(replace) == 0 {
		return
	}

	columns := map[string]*Expr{}
	for _, e := range replace {
		columns[e.Alias()] = e
	}

	for _, table := range tables {
		replaceColumns[table] = columns
	}
}

// QualifyOutputs ensures all output columns of expression are aliased.
func QualifyOutputs(expression *Expr, dialect *Dialect) {
	scope := BuildScope(expression)
	if scope == nil {
		return
	}
	QualifyOutputsScope(scope, dialect)
}

// QualifyOutputsScope ensures all output columns of scope are aliased.
func QualifyOutputsScope(scope *Scope, dialect *Dialect) {
	if dialect == nil {
		dialect = prototype("")
	}
	expression := scope.Expression

	if !expression.IsA(KSelectable) {
		return
	}

	var newSelections []*Expr

	selects := expression.Selects()
	n := len(selects)
	if len(scope.OuterColumns) > n {
		n = len(scope.OuterColumns)
	}
	for i := range n {
		var selection *Expr
		if i < len(selects) {
			selection = selects[i]
		}
		var aliasedColumn string
		if i < len(scope.OuterColumns) {
			aliasedColumn = scope.OuterColumns[i]
		}

		if selection == nil || selection.IsA(KQueryTransform) {
			break
		}

		if selection.IsA(KSubquery) {
			if selection.OutputName() == "" {
				aliasIdentifier := ToIdentifier(fmt.Sprintf("_col_%d", i), nil)
				dialect.NormalizeIdentifier(aliasIdentifier)
				selection.Set("alias", New(KTableAlias, "this", aliasIdentifier))
			}
		} else if !selection.IsA(KAlias, KAliases) && !selection.IsStar() {
			var sourceIdentifier *Expr
			if unwrapped := unnestMethod(selection); unwrapped.IsA(KColumn) {
				sourceIdentifier = unwrapped.This()
			} else if unwrapped.IsA(KDot) {
				sourceIdentifier = unwrapped.Expression()
			}

			aliasName := selection.OutputName()
			if aliasName == "" {
				aliasName = fmt.Sprintf("_col_%d", i)
			}
			selection = AliasExpr(selection, aliasName, nil, false)
			if sourceIdentifier.IsA(KIdentifier) {
				// The alias copies the spelling of an existing identifier, whose casing identifier
				// normalization already settled, so it is only marked quoted like its source
				if sourceIdentifier.ArgB("quoted") {
					selection.ArgE("alias").Set("quoted", true)
				}
			} else {
				dialect.NormalizeIdentifier(selection.ArgE("alias"))
			}
		}
		if aliasedColumn != "" {
			selection.Set("alias", ToIdentifier(aliasedColumn, nil))
		}

		newSelections = append(newSelections, selection)
	}

	if len(newSelections) > 0 && expression.IsA(KSelect) {
		expression.Set("expressions", newSelections)
	}
}

func QuoteIdentifiers(expression *Expr, d *Dialect, identify bool) *Expr {
	if d == nil {
		d = prototype("")
	}

	// QuoteIdentifier only mutates identifiers in place, so we avoid Transform here
	// because its node replacement machinery is wasteful for this case.
	for node := range expression.Walk(true, nil) {
		if node.IsA(KIdentifier) {
			d.QuoteIdentifier(node, identify)
		}
	}

	return expression
}

// PushdownCTEAliasColumns pushes down the CTE alias columns into the projection.
//
// This step is useful in Snowflake where the CTE alias columns can be referenced in the HAVING.
func PushdownCTEAliasColumns(scope *Scope) {
	for _, cte := range scope.CTEs() {
		aliasColumnNames := cte.AliasColumnNames()
		if len(aliasColumnNames) > 0 && cte.This().IsA(KSelect) {
			var newExpressions []*Expr
			projections := cte.This().Expressions()
			for i := 0; i < len(aliasColumnNames) && i < len(projections); i++ {
				alias, projection := aliasColumnNames[i], projections[i]
				if projection.IsA(KAlias) {
					projection.Set("alias", ToIdentifier(alias, nil))
				} else {
					projection = AliasExpr(projection, alias, nil, true)
				}
				newExpressions = append(newExpressions, projection)
			}
			cte.This().Set("expressions", newExpressions)
		}
	}
}

// SimplifyParens returns the inner expression of a Paren when the parentheses don't affect how it
// binds in its parent, and expression itself otherwise.
func SimplifyParens(expression *Expr, d *Dialect) *Expr {
	if !expression.IsA(KParen) {
		return expression
	}
	if d == nil {
		d = prototype("")
	}

	this := expression.This()
	parent := expression.Parent()
	parentIsPredicate := parent.IsA(KPredicate)

	if this.IsA(KSelect) {
		return expression
	}

	if parent.IsA(KSubqueryPredicate, KBracket) {
		return expression
	}

	if d.S.REQUIRES_PARENTHESIZED_STRUCT_ACCESS &&
		parent.IsA(KDot) &&
		parent.Right().IsA(KIdentifier, KStar) {
		return expression
	}

	if this.IsA(KPredicate) &&
		!(parentIsPredicate ||
			parent.IsA(KNeg) ||
			(parent.IsA(KBinary) && !parent.IsA(KConnector))) {
		return this
	}

	if !parent.IsA(KCondition, KBinary) ||
		parent.IsA(KParen) ||
		(!this.IsA(KBinary) && !(this.IsA(KNot, KIs) && parentIsPredicate)) ||
		(this.IsA(KAdd) && parent.IsA(KAdd)) ||
		(this.IsA(KMul) && parent.IsA(KMul)) ||
		(this.IsA(KMul) && parent.IsA(KAdd, KSub)) {
		return this
	}

	return expression
}
