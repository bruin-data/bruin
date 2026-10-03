package sqlengine

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Port of sqlglot/optimizer/qualify_columns.py (plus simplify_parens from simplify.py).

// QualifyColumns mirrors qualify_columns(expression, schema, expand_alias_refs, expand_stars,
// infer_schema, allow_partial_qualification, dialect).
//
// It rewrites the AST to have fully qualified columns. inferSchema TriNone means "infer the schema
// iff it is empty". A nil schema is replaced by an empty MappingSchema for d (ensure_schema); d is
// otherwise unused, like in Python (the schema dialect drives the qualification).
//
// Notes:
//   - Currently only handles a single PIVOT or UNPIVOT operator
func QualifyColumns(expression *Expr, schema *MappingSchema, expandAliasRefs, expandStars bool, inferSchema Tri, allowPartialQualification bool, d *Dialect) *Expr {
	if schema == nil {
		schema = NewMappingSchema(nil, nil, d, true, nil)
	}

	// TypeAnnotator(schema) is created lazily: it is stateless until first used.
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
		usingColumnTables := qcExpandUsing(scope, resolver)

		if (schema.Empty() || dialect.S.FORCE_EARLY_ALIAS_REF_EXPANSION) && expandAliasRefs {
			qcExpandAliasRefs(scope, resolver, dialect, dialect.S.EXPAND_ONLY_GROUP_ALIAS_REF)
		}

		qcConvertColumnsToDots(scope, resolver)
		qcQualifyColumns(scope, resolver, allowPartialQualification)

		if !schema.Empty() && expandAliasRefs {
			qcExpandAliasRefs(scope, resolver, dialect, false)
		}

		if isSelect {
			if expandStars {
				qcExpandStars(scope, resolver, usingColumnTables, pseudocolumns, getAnnotator)
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

// ValidateQualifyColumns mirrors validate_qualify_columns(expression, sql): it raises an
// OptimizeError if any columns aren't qualified. sql "" means None.
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

// qcPositions reads the line/col/start/end position metadata of a node (nil when absent).
func qcPositions(e *Expr) (line, col, start, end any) {
	if e == nil {
		return nil, nil, nil, nil
	}
	return e.MetaGet("line"), e.MetaGet("col"), e.MetaGet("start"), e.MetaGet("end")
}

// qcHighlight mirrors highlight_sql(sql, [(start, end)])[0].
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

// qcPopTableColumnAliases mirrors _pop_table_column_aliases: remove table column aliases.
//
// For example, `col1` and `col2` will be dropped in SELECT ... FROM (SELECT ...) AS foo(col1, col2)
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

// usingColumnTables mirrors the dict[str, dict[str, None]] returned by _expand_using: automatically
// joined column names mapped to an ordered set of source names.
type usingColumnTables = omap[*omap[struct{}]]

func qcExpandUsing(scope *Scope, resolver *Resolver) *usingColumnTables {
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
		return newOMap[*omap[struct{}]]()
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

	// Mapping of automatically joined column names to an ordered set of source names (dict).
	columnTables := newOMap[*omap[struct{}]]()

	anyUsing := false
	for _, join := range joins {
		if join.ArgB("using") || join.MethodText() == "NATURAL" {
			anyUsing = true
			break
		}
	}
	if !anyUsing {
		return columnTables
	}

	for _, sourceName := range ordered {
		updateSourceColumns(sourceName)
	}

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
		if usingArg == nil && join.MethodText() == "NATURAL" {
			// A NATURAL JOIN is a USING join over the columns common to both sides; when
			// those can't be determined (unknown schema, no common columns), NATURAL stays
			if columns.Len() > 0 && !columns.Has("*") && len(joinColumns) > 0 && !containsStr(joinColumns, "*") {
				using = nil
				for _, columnName := range columns.Keys() {
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
			if i == 0 || usingIdentifierCount == 1 {
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

			// Set all values in the dict to None, because we only care about the key ordering
			tables, ok := columnTables.Get(identifier)
			if !ok {
				tables = newOMap[struct{}]()
				columnTables.Set(identifier, tables)
			}

			// Do not update the dict if this was a SEMI/ANTI join in
			// order to avoid generating COALESCE columns for this join pair
			if !isSemiOrAntiJoin {
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

	return columnTables
}

// qcCoalesce mirrors exp.func("coalesce", *args) (base dialect parser: build_coalesce).
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

// qcEQ mirrors this.eq(other) (Expr._binop(EQ, other)).
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

// qcExpandAliasRefs mirrors _expand_alias_refs: expand references to aliases.
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
				table = resolver.GetTableByName(column.Name())
			}
			ref, hasAlias := aliasToExpression[column.Name()]
			aliasExpr, i := ref.expr, ref.index
			if !hasAlias {
				aliasExpr, i = nil, 1
			}

			if aliasExpr != nil {
				skipReplace = aliasExpr.Find(KAggFunc) != nil &&
					column.FindAncestor(KAggFunc) != nil &&
					!column.FindAncestor(KWindow, KSelect).IsA(KWindow)

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
						col.Set("table", resolver.GetTableByName(col.Name()))
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
				// dict semantics: the value of the last entry whose key equals node
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

// qcSelectByPos mirrors _select_by_pos (Python list indexing semantics, including negative indexes).
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

// qcConvertColumnsToDots mirrors _convert_columns_to_dots.
//
// Converts `Column` instances that represent STRUCT or JSON field lookup into chained `Dots`.
//
// These lookups may be parsed as columns (e.g. "col"."field"."field2"), but they need to be
// normalized to `Dot(Dot(...(<table>.<column>, field1), field2, ...))` to be qualified properly.
func qcConvertColumnsToDots(scope *Scope, resolver *Resolver) {
	converted := false
	all := append(append([]*Expr{}, scope.Columns()...), scope.Stars()...)
	for _, column := range all {
		if column.IsA(KDot) {
			continue
		}

		columnTableName := column.TableName()
		meta := column.Meta()
		dotParts, _ := meta["dot_parts"].([]string)
		delete(meta, "dot_parts")
		if columnTableName != "" &&
			!scope.SelectedSources().Has(columnTableName) &&
			(scope.Parent == nil ||
				!scope.Parent.Sources.Has(columnTableName) ||
				!scope.IsCorrelatedSubquery()) {
			parts := column.Parts()
			root, parts := parts[0], parts[1:]

			var columnTable *Expr
			var wasQualified bool
			if root.IsA(KIdentifier) && scope.SelectedSources().Has(root.Name()) {
				// The struct is already qualified, but we still need to change the AST
				columnTable = root
				if len(parts) == 0 {
					panic(&ValueError{Msg: "not enough values to unpack (expected at least 1, got 0)"})
				}
				root, parts = parts[0], parts[1:]
				wasQualified = true
			} else {
				columnTable = resolver.GetTableByName(root.Name())
				wasQualified = false
			}

			if columnTable != nil {
				converted = true
				newColumn := ColumnExpr(root, columnTable, nil, nil, nil, nil, true)

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
					newColumn.Meta()["dot_parts"] = rest
				}

				column.Replace(DotBuild(append([]*Expr{newColumn}, parts...)))
			}
		}
	}

	if converted {
		// We want to re-aggregate the converted columns, otherwise they'd be skipped in
		// a `for column in scope.columns` iteration, even though they shouldn't be
		scope.ClearCache()
	}
}

// qcQualifyColumns mirrors _qualify_columns: disambiguate columns, ensuring each column specifies a source.
func qcQualifyColumns(scope *Scope, resolver *Resolver, allowPartialQualification bool) {
	for _, column := range scope.Columns() {
		columnTable := column.TableName()
		columnName := column.Name()

		if columnTable != "" && scope.Sources.Has(columnTable) {
			columnSource, _ := scope.Sources.Get(columnTable)
			sourceColumns := resolver.GetSourceColumns(columnTable, false)
			// For pivoted sources, source_columns are pre-pivot; validate against the post-pivot set.
			if columnSource.Table != nil {
				if pivots := columnSource.Table.ArgL("pivots"); len(pivots) > 0 {
					sourceColumns = PivotOutputColumns(pivots[0], sourceColumns).Keys()
				}
			}
			if !allowPartialQualification &&
				len(sourceColumns) > 0 &&
				!containsStr(sourceColumns, columnName) &&
				!containsStr(sourceColumns, "*") {
				panic(&OptimizeError{Msg: "Unknown column: " + columnName})
			}
		}

		if columnTable == "" {
			if pivots := scope.Pivots(); len(pivots) > 0 && column.FindAncestor(KPivot) == nil {
				// If the column is under the Pivot expression, we need to qualify it
				// using the name of the pivoted source instead of the pivot's alias
				column.Set("table", ToIdentifier(pivots[0].Alias(), nil))
				continue
			}

			// column_table can be a '' because bigquery unnest has no table alias
			table := resolver.GetTable(column)

			if table != nil {
				if source, ok := scope.Sources.Get(table.Name()); ok && source.Scope != nil {
					if _, inIndex := source.Scope.ColumnIndex()[column]; inIndex {
						continue
					}
				}
			}

			if table != nil {
				column.Set("table", table)
			} else if resolver.dialect.S.TABLES_REFERENCEABLE_AS_COLUMNS &&
				len(column.Parts()) == 1 &&
				scope.SelectedSources().Has(columnName) {
				// BigQuery and Postgres allow tables to be referenced as columns, treating them as structs/records
				scope.Replace(column, New(KTableColumn, "this", column.This()))
			}
		}
	}

	for _, pivot := range scope.Pivots() {
		for column := range pivot.FindAll(KColumn) {
			if column.TableName() == "" && resolver.AllColumns().Has(column.Name()) {
				if table := resolver.GetTableByName(column.Name()); table != nil {
					column.Set("table", table)
				}
			}
		}
	}
}

// qcIsType mirrors Expr.is_type(dtype) (the annotated type) for a single DType.
func qcIsType(e *Expr, dtype DType) bool {
	t := e.Type()
	return t != nil && DataTypeIsType(t, []any{dtype}, false)
}

// qcExpandStructStarsNoParens mirrors _expand_struct_stars_no_parens:
// [BigQuery] Expand/Flatten foo.bar.* where bar is a struct column.
func qcExpandStructStarsNoParens(expression *Expr) []*Expr {
	dotColumn := expression.Find(KColumn)
	if !dotColumn.IsA(KColumn) || !qcIsType(dotColumn, DT_STRUCT) {
		return nil
	}

	// All nested struct values are ColumnDefs, so normalize the first exp.Column in one
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
				// There is no matching field in the struct
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

// qcExpandStructStarsWithParens mirrors _expand_struct_stars_with_parens:
// [RisingWave] Expand/Flatten (<exp>.bar).*, where bar is a struct column.
func qcExpandStructStarsWithParens(expression *Expr) []*Expr {
	// it is not (<sub_exp>).* pattern, which means we can't expand
	if !expression.This().IsA(KParen) {
		return nil
	}

	// find column definition to get data-type
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

		// if parent is not a dot, then something is wrong
		if !parent.IsA(KDot) {
			return nil
		}

		// if the rhs of the dot is star we are done
		rhs := parent.Right()
		if rhs.IsA(KStar) {
			break
		}

		// if it is not identifier, then something is wrong
		if !rhs.IsA(KIdentifier) {
			return nil
		}

		// Check if current rhs identifier is in struct
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

	// build new aliases to expand star
	var newSelections []*Expr

	// fetch the outermost parentheses for new aliaes
	outerParen := expression.This()

	for _, structFieldDef := range startingStruct.Expressions() {
		newIdentifier := structFieldDef.This().Copy()
		newDot := DotBuild([]*Expr{outerParen.Copy(), newIdentifier})
		newAlias := chunkFAssertIs(AliasExpr(newDot, newIdentifier, nil, false), KAlias)
		newSelections = append(newSelections, newAlias)
	}

	return newSelections
}

// qcStarTable is a table name referenced by a star projection. key mirrors Python's id(table): the
// names taken from scope.selected_sources are shared by every `*`, while a `tbl.*` column owns its name.
type qcStarTable struct {
	name string
	key  string
}

// qcExpandStars mirrors _expand_stars: expand stars to lists of column selections.
func qcExpandStars(scope *Scope, resolver *Resolver, usingColumnTables *usingColumnTables, pseudocolumns StrSet, annotator func() *TypeAnnotator) {
	var newSelections []*Expr
	exceptColumns := map[string]StrSet{}
	replaceColumns := map[string]map[string]*Expr{}
	renameColumns := map[string]map[string]string{}
	ilikePattern := ""

	coalescedColumns := newStrSet()
	dialect := resolver.dialect

	pivot := seqGet(scope.Pivots(), 0)

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
		var tables []qcStarTable
		if expression.IsA(KStar) {
			for _, name := range scope.SelectedSources().Keys() {
				tables = append(tables, qcStarTable{name, "S\x00" + name})
			}
			qcAddExceptColumns(expression, tables, exceptColumns)
			qcAddReplaceColumns(expression, tables, replaceColumns)
			qcAddRenameColumns(expression, tables, renameColumns)
			ilikePattern = qcAddIlikeColumns(expression)
		} else if expression.IsStar() {
			if expression.IsA(KColumn) {
				tables = append(tables, qcStarTable{expression.TableName(), fmt.Sprintf("C%p\x00%s", expression, expression.TableName())})
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

		var ilikeRe *regexp.Regexp
		for _, tableRef := range tables {
			table := tableRef.name
			source, ok := scope.Sources.Get(table)
			if !ok {
				panic(&OptimizeError{Msg: "Unknown table: " + table})
			}

			columns := resolver.GetSourceColumns(table, true)
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

			if len(columns) == 0 || containsStr(columns, "*") {
				return
			}

			columnsToExclude := exceptColumns[tableRef.key]
			renamedColumns := renameColumns[tableRef.key]
			replacedColumns := replaceColumns[tableRef.key]

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

			if pivot != nil {
				pivotColumns := PivotOutputColumns(pivot, columns).Keys()
				if len(pivotColumns) == 0 {
					pivotColumns = pivot.AliasColumnNames()
				}

				if len(pivotColumns) > 0 {
					var pivotTable any
					if a := pivot.Alias(); a != "" {
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
				usingTables, isUsing := usingColumnTables.Get(name)
				if isUsing && usingTables.Has(table) {
					coalescedColumns.Add(name)
					var coalesceArgs []*Expr
					for _, t := range usingTables.Keys() {
						coalesceArgs = append(coalesceArgs, ColumnExpr(name, t, nil, nil, nil, nil, true))
					}

					newSelections = append(newSelections, AliasExpr(qcCoalesce(coalesceArgs), name, nil, false))
				} else {
					alias := name
					if r, ok := renamedColumns[name]; ok {
						alias = r
					}
					// if it has characters that the dialect would have changed, infer that it was quoted.
					quoted := quotedColumns.Has(name) || (source.Table != nil && dialect.CaseSensitive(name))
					selectionExpr := replacedColumns[name]
					if selectionExpr == nil {
						selectionExpr = ColumnExpr(name, table, nil, nil, nil, boolp(quoted), true)
					}
					if alias != name {
						newSelections = append(newSelections, AliasExpr(selectionExpr, alias, nil, false))
					} else {
						newSelections = append(newSelections, selectionExpr)
					}
				}
			}
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
	}
}

// qcOutputIdentifierQuoted mirrors _output_identifier_quoted: whether a projection's output column
// name is a quoted (case-sensitive) identifier.
func qcOutputIdentifierQuoted(selection *Expr) bool {
	var identifier *Expr
	if selection.IsA(KAlias) {
		identifier = selection.ArgE("alias")
	} else if selection.IsA(KColumn) {
		identifier = selection.This()
	}

	return identifier.IsA(KIdentifier) && identifier.ArgB("quoted")
}

// qcAddIlikeColumns mirrors _add_ilike_columns; "" means None.
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

func qcAddExceptColumns(expression *Expr, tables []qcStarTable, exceptColumns map[string]StrSet) {
	except := expression.ArgL("except_")

	if len(except) == 0 {
		return
	}

	columns := newStrSet()
	for _, e := range except {
		columns.Add(e.Name())
	}

	for _, table := range tables {
		exceptColumns[table.key] = columns
	}
}

func qcAddRenameColumns(expression *Expr, tables []qcStarTable, renameColumns map[string]map[string]string) {
	rename := expression.ArgL("rename")

	if len(rename) == 0 {
		return
	}

	columns := map[string]string{}
	for _, e := range rename {
		columns[e.This().Name()] = e.Alias()
	}

	for _, table := range tables {
		renameColumns[table.key] = columns
	}
}

func qcAddReplaceColumns(expression *Expr, tables []qcStarTable, replaceColumns map[string]map[string]*Expr) {
	replace := expression.ArgL("replace")

	if len(replace) == 0 {
		return
	}

	columns := map[string]*Expr{}
	for _, e := range replace {
		columns[e.Alias()] = e
	}

	for _, table := range tables {
		replaceColumns[table.key] = columns
	}
}

// QualifyOutputs mirrors qualify_outputs(expression, dialect) for an expression input: ensure all
// output columns are aliased.
func QualifyOutputs(expression *Expr, dialect *Dialect) {
	scope := BuildScope(expression)
	if scope == nil {
		return
	}
	QualifyOutputsScope(scope, dialect)
}

// QualifyOutputsScope mirrors qualify_outputs(scope, dialect): ensure all output columns are aliased.
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
	for i := 0; i < n; i++ {
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
			sourceQuoted := false
			if selection.IsA(KColumn) {
				// selection.this.quoted: only Identifier has `quoted`; Python raises AttributeError
				// for any other `this` (e.g. a Dot in BigQuery's `p.d.t`.`c`.`f`, or a Parameter).
				this := selection.This()
				if !this.IsA(KIdentifier) {
					className := "NoneType"
					if this != nil {
						className = this.Kind().Name()
					}
					panic(&ValueError{Msg: fmt.Sprintf("'%s' object has no attribute 'quoted'", className)})
				}
				sourceQuoted = this.ArgB("quoted")
			}
			aliasName := selection.OutputName()
			if aliasName == "" {
				aliasName = fmt.Sprintf("_col_%d", i)
			}
			selection = AliasExpr(selection, aliasName, nil, false)
			if sourceQuoted {
				selection.ArgE("alias").Set("quoted", true)
			}
			dialect.NormalizeIdentifier(selection.ArgE("alias"))
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

// QuoteIdentifiers mirrors quote_identifiers(expression, dialect, identify): makes sure all
// identifiers that need to be quoted are quoted.
func QuoteIdentifiers(expression *Expr, d *Dialect, identify bool) *Expr {
	if d == nil {
		d = prototype("")
	}

	// `quote_identifier` only mutates identifiers in place, so we avoid `transform` here
	// because its node replacement machinery is wasteful for this case.
	for node := range expression.Walk(true, nil) {
		if node.IsA(KIdentifier) {
			d.QuoteIdentifier(node, identify)
		}
	}

	return expression
}

// PushdownCTEAliasColumns mirrors pushdown_cte_alias_columns: pushes down the CTE alias columns
// into the projection.
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

// SimplifyParens mirrors sqlglot.optimizer.simplify.simplify_parens.
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
