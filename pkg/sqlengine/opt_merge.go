package sqlengine

// Port of sqlglot/optimizer/merge_subqueries.py.

// MergeSubqueries mirrors sqlglot.optimizer.merge_subqueries.merge_subqueries.
//
// Rewrite sqlglot AST to merge derived tables into the outer query.
// This also merges CTEs if they are selected from only once.
//
// If leaveTablesIsolated is true, this will not merge inner queries into outer
// queries if it would result in multiple table selects in a single query.
func MergeSubqueries(expression *Expr, leaveTablesIsolated bool) *Expr {
	expression = MergeCTEs(expression, leaveTablesIsolated)
	expression = MergeDerivedTables(expression, leaveTablesIsolated)
	return expression
}

// msUnmergableArgs mirrors UNMERGABLE_ARGS: if a derived table has these Select args, it can't be merged.
var msUnmergableArgs = func() []string {
	keep := newStrSet("expressions", "from_", "joins", "where", "order", "hint")
	var out []string
	for _, a := range KSelect.ArgTypes() {
		if !keep.Has(a.name) {
			out = append(out, a.name)
		}
	}
	return out
}()

// msSafeToReplaceUnwrapped mirrors SAFE_TO_REPLACE_UNWRAPPED: projections in the outer query that are
// instances of these types can be replaced without getting wrapped in parentheses, because the
// precedence won't be altered.
var msSafeToReplaceUnwrapped = []Kind{KColumn, KEQ, KFunc, KNEQ, KParen}

type msCTESelection struct {
	outerScope *Scope
	innerScope *Scope
	table      *Expr
}

// MergeCTEs mirrors merge_subqueries.merge_ctes.
func MergeCTEs(expression *Expr, leaveTablesIsolated bool) *Expr {
	scopes := TraverseScope(expression)

	// All places where we select from CTEs.
	// We key on the CTE scope so we can detect CTES that are selected from multiple times.
	var order []*Scope
	cteSelections := map[*Scope][]msCTESelection{}
	for _, outerScope := range scopes {
		for _, ss := range outerScope.SelectedSources().Values() {
			innerScope := ss.Source.Scope
			if innerScope != nil && innerScope.IsCTE() {
				if _, ok := cteSelections[innerScope]; !ok {
					order = append(order, innerScope)
				}
				cteSelections[innerScope] = append(cteSelections[innerScope], msCTESelection{outerScope, innerScope, ss.Node})
			}
		}
	}

	var singularCTESelections []msCTESelection
	for _, k := range order {
		if v := cteSelections[k]; len(v) == 1 {
			singularCTESelections = append(singularCTESelections, v[0])
		}
	}
	for _, sel := range singularCTESelections {
		outerScope, innerScope, table := sel.outerScope, sel.innerScope, sel.table
		fromOrJoin := table.FindAncestor(KFrom, KJoin)
		if !fromOrJoin.IsA(KFrom, KJoin) {
			continue
		}
		if msMergeable(outerScope, innerScope, leaveTablesIsolated, fromOrJoin) {
			alias := table.AliasOrName()
			msRenameInnerSources(outerScope, innerScope, alias)
			msMergeFrom(outerScope, innerScope, table, alias)
			msMergeExpressions(outerScope, innerScope, alias)
			msMergeOrder(outerScope, innerScope)
			msMergeJoins(outerScope, innerScope, fromOrJoin)
			msMergeWhere(outerScope, innerScope, fromOrJoin)
			msMergeHints(outerScope, innerScope)
			msPopCTE(innerScope)
			outerScope.ClearCache()
		}
	}
	return expression
}

// MergeDerivedTables mirrors merge_subqueries.merge_derived_tables.
func MergeDerivedTables(expression *Expr, leaveTablesIsolated bool) *Expr {
	for _, outerScope := range TraverseScope(expression) {
		for _, subquery := range outerScope.DerivedTables() {
			fromOrJoin := subquery.FindAncestor(KFrom, KJoin)
			if !fromOrJoin.IsA(KFrom, KJoin) {
				continue
			}
			alias := subquery.AliasOrName()
			src, ok := outerScope.Sources.Get(alias)
			if !ok {
				panic(&ValueError{Msg: msKeyError(alias)})
			}
			innerScope := src.Scope
			if innerScope == nil {
				continue
			}
			if msMergeable(outerScope, innerScope, leaveTablesIsolated, fromOrJoin) {
				msRenameInnerSources(outerScope, innerScope, alias)
				msMergeFrom(outerScope, innerScope, subquery, alias)
				msMergeExpressions(outerScope, innerScope, alias)
				msMergeOrder(outerScope, innerScope)
				msMergeJoins(outerScope, innerScope, fromOrJoin)
				msMergeWhere(outerScope, innerScope, fromOrJoin)
				msMergeHints(outerScope, innerScope)
				outerScope.ClearCache()
			}
		}
	}

	return expression
}

// msMergeable mirrors merge_subqueries._mergeable: return true if `inner_select` can be merged into
// the outer query.
func msMergeable(outerScope, innerScope *Scope, leaveTablesIsolated bool, fromOrJoin *Expr) bool {
	innerSelect := innerScope.Expression.UnnestSubqueryOrParen()

	// A window function's result depends on the full row set it sees, so merging the
	// subquery into the outer query is unsafe when:
	//   - the outer query filters or joins (WHERE/JOIN), which changes that row set, or
	//   - a window column is referenced in an operation that isn't pushed down
	//     (GROUP BY, ORDER BY, HAVING, aggregate).
	windowProjectionBlocksMerge := func() bool {
		windowAliases := newStrSet()
		for _, s := range innerSelect.Selects() {
			if s.Find(KWindow) != nil {
				windowAliases.Add(s.AliasOrName())
			}
		}
		if len(windowAliases) == 0 {
			return false
		}

		outer := outerScope.Expression
		if outer.ArgB("where") || outer.ArgB("joins") {
			return true
		}

		innerSelectName := fromOrJoin.AliasOrName()
		for _, column := range outerScope.Columns() {
			if column.TableName() == innerSelectName &&
				windowAliases.Has(column.Name()) &&
				column.FindAncestor(KGroup, KOrder, KHaving, KAggFunc) != nil {
				return true
			}
		}
		return false
	}

	// A numeric-literal projection referenced in GROUP BY can't be inlined, because a bare
	// integer literal is positional. A reference that is itself a top-level GROUP BY item
	// can merge as the ordinal of the outer projection that selects it; any other reference,
	// e.g., ROLLUP / CUBE / GROUPING SETS, tuples, expressions, etc, blocks the merge, since
	// ordinals aren't universally supported there, e.g., Presto / Trino only accept columns.
	literalGroupUnmergeable := func() bool {
		group := outerScope.Expression.ArgE("group")
		if group == nil {
			return false
		}

		innerName := fromOrJoin.AliasOrName()
		literalNames := newStrSet()
		for _, s := range innerSelect.Selects() {
			if s.Unalias().IsNumber() {
				literalNames.Add(s.AliasOrName())
			}
		}
		if len(literalNames) == 0 {
			return false
		}

		grouped := newStrSet()
		topLevelIDs := map[*Expr]struct{}{}
		for _, e := range group.Expressions() {
			topLevelIDs[e.UnnestSubqueryOrParen()] = struct{}{}
		}
		for col := range group.FindAll(KColumn) {
			if col.TableName() != innerName || !literalNames.Has(col.Name()) {
				continue
			}
			if _, ok := topLevelIDs[col]; !ok {
				return true
			}
			grouped.Add(col.Name())
		}

		if len(grouped) == 0 {
			return false
		}

		projected := newStrSet()
		for _, s := range outerScope.Expression.Selects() {
			unaliased := s.Unalias()
			if unaliased.IsA(KColumn) && unaliased.TableName() == innerName {
				projected.Add(unaliased.Name())
			}
		}

		// not grouped <= projected
		for name := range grouped {
			if !projected.Has(name) {
				return true
			}
		}
		return false
	}

	// All columns from the inner select in the ON clause must be from the first FROM table.
	//
	// That is, this can be merged:
	//     SELECT * FROM x JOIN (SELECT y.a AS a FROM y JOIN z) AS q ON x.a = q.a
	//                                  ^^^           ^
	// But this can't:
	//     SELECT * FROM x JOIN (SELECT z.a AS a FROM y JOIN z) AS q ON x.a = q.a
	//                                  ^^^                  ^
	outerSelectJoinsOnInnerSelectJoin := func() bool {
		if !fromOrJoin.IsA(KJoin) {
			return false
		}

		alias := fromOrJoin.AliasOrName()

		on := fromOrJoin.ArgE("on")
		if on == nil {
			return false
		}
		var selections []string
		for c := range on.FindAll(KColumn) {
			if c.TableName() == alias {
				selections = append(selections, c.Name())
			}
		}
		innerFrom := innerScope.Expression.ArgE("from_")
		if innerFrom == nil {
			return false
		}
		innerFromTable := innerFrom.AliasOrName()
		innerProjections := map[string]*Expr{}
		for _, s := range innerScope.Expression.Selects() {
			innerProjections[s.AliasOrName()] = s
		}
		for _, selection := range selections {
			projection, ok := innerProjections[selection]
			if !ok {
				panic(&ValueError{Msg: msKeyError(selection)})
			}
			for col := range projection.FindAll(KColumn) {
				if col.TableName() != innerFromTable {
					return true
				}
			}
		}
		return false
	}

	isRecursive := func() bool {
		// Recursive CTEs look like this:
		//     WITH RECURSIVE cte AS (
		//       SELECT * FROM x  <-- inner scope
		//       UNION ALL
		//       SELECT * FROM cte  <-- outer scope
		//     )
		cte := innerScope.Expression.Parent()
		node := outerScope.Expression.Parent()

		for node != nil {
			if node == cte {
				return true
			}
			node = node.Parent()
		}
		return false
	}

	// A numeric-literal projection under a bare ORDER BY key can't merge (would become positional).
	literalInOrderBy := func() bool {
		order := outerScope.Expression.ArgE("order")
		if order == nil {
			return false
		}
		innerName := fromOrJoin.AliasOrName()
		ordered := newStrSet()
		for _, o := range order.Expressions() {
			key := o.This().UnnestSubqueryOrParen()
			if key.IsA(KColumn) && key.TableName() == innerName {
				ordered.Add(key.Name())
			}
		}
		for _, s := range innerSelect.Selects() {
			if ordered.Has(s.AliasOrName()) && s.Unalias().IsNumber() {
				return true
			}
		}
		return false
	}

	if !outerScope.Expression.IsA(KSelect) ||
		outerScope.Expression.IsStar() ||
		!innerSelect.IsA(KSelect) {
		return false
	}
	for _, arg := range msUnmergableArgs {
		if innerSelect.ArgB(arg) {
			return false
		}
	}
	if innerSelect.Arg("from_") == nil || len(outerScope.Pivots()) > 0 {
		return false
	}
	for _, e := range innerSelect.Expressions() {
		if e.Find(KAggFunc, KSelect, KExplode) != nil {
			return false
		}
	}
	if leaveTablesIsolated && outerScope.SelectedSources().Len() > 1 {
		return false
	}
	if fromOrJoin.IsA(KJoin) && innerSelect.ArgB("joins") {
		return false
	}
	if fromOrJoin.IsA(KJoin) && innerSelect.ArgB("where") {
		if side := fromOrJoin.SideText(); side == "FULL" || side == "LEFT" || side == "RIGHT" {
			return false
		}
	}
	if fromOrJoin.IsA(KFrom) && innerSelect.ArgB("where") {
		for _, j := range outerScope.Expression.ArgL("joins") {
			if side := j.SideText(); side == "FULL" || side == "RIGHT" {
				return false
			}
		}
	}
	return !outerSelectJoinsOnInnerSelectJoin() &&
		!windowProjectionBlocksMerge() &&
		!literalGroupUnmergeable() &&
		!literalInOrderBy() &&
		!isRecursive() &&
		!(innerSelect.ArgB("order") && outerScope.IsUnion()) &&
		!seqGet(innerSelect.Expressions(), 0).IsA(KQueryTransform)
}

// msRenameInnerSources mirrors merge_subqueries._rename_inner_sources: renames any sources in the
// inner query that conflict with names in the outer query.
func msRenameInnerSources(outerScope, innerScope *Scope, alias string) {
	innerKeys := innerScope.SelectedSources().Keys()
	outerKeys := outerScope.SelectedSources().Keys()
	innerTaken := newStrSet(innerKeys...)
	outerTaken := newStrSet(outerKeys...)

	// conflicts = outer_taken & inner_taken - {alias}. Python iterates this set in an arbitrary
	// order; the result does not depend on it (new names are never conflicts), so use outer order.
	var conflicts []string
	for _, name := range outerKeys {
		if innerTaken.Has(name) && name != alias {
			conflicts = append(conflicts, name)
		}
	}

	taken := outerTaken.Clone().Add(innerKeys...)

	for _, conflict := range conflicts {
		newName := findNewNameOMap(taken.Has, conflict)

		selected, _ := innerScope.SelectedSources().Get(conflict)
		source := selected.Node
		newAlias := ToIdentifier(newName, nil)

		if source.IsA(KTable) && source.Alias() != "" {
			source.Set("alias", New(KTableAlias, "this", newAlias))
		} else if source.IsA(KTable) {
			source.Replace(AliasExpr(source, newAlias, nil, true))
		} else if source.Parent().IsA(KSubquery) {
			source.Parent().Set("alias", New(KTableAlias, "this", newAlias))
		}

		for _, column := range innerScope.SourceColumns(conflict) {
			column.Set("table", ToIdentifier(newName, nil))
		}

		innerScope.RenameSource(conflict, newName)
	}
}

// msMergeFrom mirrors merge_subqueries._merge_from: merge FROM clause of inner query into outer query.
func msMergeFrom(outerScope, innerScope *Scope, nodeToReplace *Expr, alias string) {
	newSubquery := innerScope.Expression.ArgE("from_").This()
	newSubquery.Set("joins", nodeToReplace.Arg("joins"))
	nodeToReplace.Replace(newSubquery)
	for _, joinHint := range outerScope.JoinHints() {
		for table := range joinHint.FindAll(KTable) {
			if table.AliasOrName() == nodeToReplace.AliasOrName() {
				table.Set("this", ToIdentifier(newSubquery.AliasOrName(), nil))
			}
		}
	}
	outerScope.RemoveSource(alias)
	name := newSubquery.AliasOrName()
	src, ok := innerScope.Sources.Get(name)
	if !ok {
		panic(&ValueError{Msg: msKeyError(name)})
	}
	outerScope.AddSource(name, src)
}

// msMergeJoins mirrors merge_subqueries._merge_joins: merge JOIN clauses of inner query into outer query.
func msMergeJoins(outerScope, innerScope *Scope, fromOrJoin *Expr) {
	var newJoins []*Expr

	joins := innerScope.Expression.ArgL("joins")

	for _, join := range joins {
		newJoins = append(newJoins, join)
		name := join.AliasOrName()
		src, ok := innerScope.Sources.Get(name)
		if !ok {
			panic(&ValueError{Msg: msKeyError(name)})
		}
		outerScope.AddSource(name, src)
	}

	if len(newJoins) > 0 {
		outerJoins := outerScope.Expression.ArgL("joins")

		// Maintain the join order
		position := 0
		if !fromOrJoin.IsA(KFrom) {
			// list.index: first element that is (or equals) from_or_join
			idx := -1
			for i, j := range outerJoins {
				if j == fromOrJoin || j.Equal(fromOrJoin) {
					idx = i
					break
				}
			}
			if idx < 0 {
				panic(&ValueError{Msg: "join is not in list"})
			}
			position = idx + 1
		}
		merged := make([]*Expr, 0, len(outerJoins)+len(newJoins))
		merged = append(merged, outerJoins[:position]...)
		merged = append(merged, newJoins...)
		merged = append(merged, outerJoins[position:]...)

		outerScope.Expression.Set("joins", merged)
	}
}

// msMergeExpressions mirrors merge_subqueries._merge_expressions: merge projections of inner query
// into outer query.
func msMergeExpressions(outerScope, innerScope *Scope, alias string) {
	// Collect all columns that reference the alias of the inner query
	outerColumns := map[string][]*Expr{}
	for _, column := range outerScope.Columns() {
		if column.TableName() == alias {
			outerColumns[column.Name()] = append(outerColumns[column.Name()], column)
		}
	}

	group := outerScope.Expression.ArgE("group")

	// Replace columns with the projection expression in the inner query
	for _, expression := range innerScope.Expression.Expressions() {
		projectionName := expression.AliasOrName()
		if projectionName == "" {
			continue
		}
		columnsToReplace := outerColumns[projectionName]
		if len(columnsToReplace) == 0 {
			continue
		}

		expression = expression.Unalias()
		mustWrapExpression := !expression.IsA(msSafeToReplaceUnwrapped...)

		isNumber := expression.IsNumber()
		last := len(columnsToReplace) - 1

		groupOrdinal := -1 // None
		if isNumber && outerScope.Expression.IsA(KSelect) {
			// Find the ordinal of the outer SELECT that references this inner projection.
			for j, s := range outerScope.Expression.Selects() {
				unaliased := s.Unalias()
				if unaliased.IsA(KColumn) &&
					unaliased.TableName() == alias &&
					unaliased.Name() == projectionName {
					groupOrdinal = j + 1
					break
				}
			}
		}

		for i, column := range columnsToReplace {
			parent := column.Parent()

			// A numeric-literal projection can't be inlined into a top-level GROUP BY item
			// (positional context), canonicalize to the projection's ordinal to match
			// qualify. _mergeable guarantees the ordinal exists.
			if isNumber && group != nil {
				var item *Expr
				for _, e := range group.Expressions() {
					if e.UnnestSubqueryOrParen() == column {
						item = e
						break
					}
				}
				if item != nil {
					if groupOrdinal >= 0 {
						item.Replace(LiteralInt(groupOrdinal))
					} else {
						item.Replace(LiteralNumber("None"))
					}
					continue
				}
			}

			// Ensures we don't alter the intended operator precedence if there's additional
			// context surrounding the outer expression (i.e. it's not a simple projection).
			if parent.IsA(KUnary, KBinary) && mustWrapExpression {
				expression = ParenExpr(expression, false)
			}

			// make sure we do not accidentally change the name of the column
			if parent.IsA(KSelect) && column.Name() != expression.Name() {
				expression = AliasExpr(expression, column.Name(), nil, true)
			}

			// Skip the expensive deep copy for the last reference since the inner query
			// is about to be removed, so we can move the expression directly
			if i < last {
				column.Replace(expression.Copy())
			} else {
				column.Replace(expression)
			}
		}
	}
}

// msMergeWhere mirrors merge_subqueries._merge_where: merge WHERE clause of inner query into outer query.
func msMergeWhere(outerScope, innerScope *Scope, fromOrJoin *Expr) {
	where := innerScope.Expression.ArgE("where")
	if where == nil || where.This() == nil {
		return
	}

	expression := outerScope.Expression

	if fromOrJoin.IsA(KJoin) {
		// Merge predicates from an outer join to the ON clause
		// if it only has columns that are already joined
		from := expression.ArgE("from_")
		sources := newStrSet()
		if from != nil {
			sources.Add(from.AliasOrName())
		}

		for _, join := range expression.ArgL("joins") {
			source := join.AliasOrName()
			sources.Add(source)
			if source == fromOrJoin.AliasOrName() {
				break
			}
		}

		subset := true
		for name := range ColumnTableNames(where.This(), "") {
			if !sources.Has(name) {
				subset = false
				break
			}
		}
		if subset {
			fromOrJoin.JoinOn([]*Expr{where.This()}, true, false)
			fromOrJoin.Set("on", fromOrJoin.Arg("on"))
			return
		}
	}

	expression.QueryWhere([]*Expr{where.This()}, true, false)
}

// msMergeOrder mirrors merge_subqueries._merge_order: merge ORDER clause of inner query into outer query.
func msMergeOrder(outerScope, innerScope *Scope) {
	innerOrder := innerScope.Expression.ArgE("order")
	if innerOrder == nil {
		return
	}

	for _, arg := range []string{"group", "distinct", "having", "order"} {
		if outerScope.Expression.ArgB(arg) {
			return
		}
	}
	if outerScope.SelectedSources().Len() != 1 {
		return
	}
	for _, expression := range outerScope.Expression.Expressions() {
		if expression.Find(KAggFunc) != nil {
			return
		}
	}

	outerScope.Expression.Set("order", innerOrder)
}

// msMergeHints mirrors merge_subqueries._merge_hints.
func msMergeHints(outerScope, innerScope *Scope) {
	innerScopeHint := innerScope.Expression.ArgE("hint")
	if innerScopeHint == nil {
		return
	}
	outerScopeHint := outerScope.Expression.ArgE("hint")
	if outerScopeHint != nil {
		for _, hintExpression := range innerScopeHint.Expressions() {
			outerScopeHint.Append("expressions", hintExpression)
		}
	} else {
		outerScope.Expression.Set("hint", innerScopeHint)
	}
}

// msPopCTE mirrors merge_subqueries._pop_cte: remove CTE from the AST.
func msPopCTE(innerScope *Scope) {
	cte := innerScope.Expression.Parent()
	if cte == nil {
		return
	}
	with := cte.Parent()
	if with == nil {
		return
	}
	if len(with.Expressions()) == 1 {
		with.Pop()
	} else {
		cte.Pop()
	}
}

// msKeyError renders a Python KeyError message for a missing string key.
func msKeyError(key string) string { return "'" + key + "'" }
