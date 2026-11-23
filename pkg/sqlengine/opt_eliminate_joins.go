package sqlengine

// EliminateJoins removes unused joins from an expression.
//
// This only removes joins when we know that the join condition doesn't produce duplicate rows.
//
// Example: "SELECT x.a FROM x LEFT JOIN (SELECT DISTINCT y.b FROM y) AS y ON x.b = y.b"
// -> "SELECT x.a FROM x".
func EliminateJoins(expression *Expr) *Expr {
	for _, scope := range TraverseScope(expression) {
		joins := scope.Expression.ArgL("joins")
		if len(joins) == 0 {
			continue
		}

		// If any columns in this scope aren't qualified, it's hard to determine if a join isn't used.
		// It's probably possible to infer this from the outputs of derived tables.
		// But for now, let's just skip this rule.
		if len(scope.UnqualifiedColumns()) > 0 {
			continue
		}

		// Reverse the joins so we can remove chains of unused joins
		for i := len(joins) - 1; i >= 0; i-- {
			join := joins[i]
			if join.IsSemiOrAntiJoin() || join.ArgB("nearest") {
				continue
			}

			alias := join.AliasOrName()
			if eliminateJoinsShouldEliminateJoin(scope, join, alias) {
				join.Pop()
				scope.RemoveSource(alias)
			}
		}
	}

	return expression
}

// eliminateJoinsShouldEliminateJoin reports whether join can be dropped: it joins a derived table
// that is not otherwise referenced, and the join cannot change the outer rows (a LEFT JOIN on all
// unique outputs, or a condition-less join of a single row).
func eliminateJoinsShouldEliminateJoin(scope *Scope, join *Expr, alias string) bool {
	innerSource, ok := scope.Sources.Get(alias)
	if !ok || !innerSource.IsScope() {
		return false
	}
	inner := innerSource.Scope
	return !eliminateJoinsJoinIsUsed(scope, join, alias) &&
		((join.SideText() == "LEFT" && eliminateJoinsIsJoinedOnAllUniqueOutputs(inner, join)) ||
			(!join.ArgB("on") && eliminateJoinsHasSingleOutputRow(inner)))
}

// eliminateJoinsJoinIsUsed reports whether any column outside the join's ON clause references
// alias.
func eliminateJoinsJoinIsUsed(scope *Scope, join *Expr, alias string) bool {
	on := join.ArgE("on")
	onClauseColumns := map[*Expr]struct{}{}
	if on != nil {
		for column := range on.FindAll(KColumn) {
			onClauseColumns[column] = struct{}{}
		}
	}
	for _, column := range scope.SourceColumns(alias) {
		if _, ok := onClauseColumns[column]; !ok {
			return true
		}
	}
	return false
}

// eliminateJoinsIsJoinedOnAllUniqueOutputs reports whether join's keys cover every unique output
// of scope, so each outer row matches at most one inner row.
func eliminateJoinsIsJoinedOnAllUniqueOutputs(scope *Scope, join *Expr) bool {
	uniqueOutputs := eliminateJoinsUniqueOutputs(scope)
	if len(uniqueOutputs) == 0 {
		return false
	}

	_, joinKeys, _ := JoinCondition(join)
	remaining := uniqueOutputs.Clone()
	for _, c := range joinKeys {
		delete(remaining, c.Name())
	}
	return len(remaining) == 0
}

// eliminateJoinsUniqueOutputs determines the output columns of `scope` that must have a unique
// combination per row.
func eliminateJoinsUniqueOutputs(scope *Scope) StrSet {
	expr := scope.Expression
	if expr.Arg("distinct") != nil {
		return newStrSet(expr.NamedSelects()...)
	}

	if group := expr.ArgE("group"); group != nil {
		// Expression sets: membership is structural equality.
		groupedExpressions := optxExprSet(group.Expressions())
		var groupedOutputs []*Expr

		uniqueOutputs := newStrSet()
		for _, sel := range expr.Selects() {
			output := sel.Unalias()
			if optxExprIn(output, groupedExpressions) {
				if !optxExprIn(output, groupedOutputs) {
					groupedOutputs = append(groupedOutputs, output)
				}
				uniqueOutputs.Add(sel.AliasOrName())
			}
		}

		// All the grouped expressions must be in the output
		for _, g := range groupedExpressions {
			if !optxExprIn(g, groupedOutputs) {
				return newStrSet()
			}
		}
		return uniqueOutputs
	}

	if eliminateJoinsHasSingleOutputRow(scope) {
		return newStrSet(expr.NamedSelects()...)
	}

	return newStrSet()
}

// eliminateJoinsHasSingleOutputRow reports whether scope yields at most one row: a SELECT of only
// aggregates, with LIMIT 1, or without FROM.
func eliminateJoinsHasSingleOutputRow(scope *Scope) bool {
	if !scope.Expression.IsA(KSelect) {
		return false
	}
	allAgg := true
	for _, e := range scope.Expression.Selects() {
		if !e.Unalias().IsA(KAggFunc) {
			allAgg = false
			break
		}
	}
	return allAgg || eliminateJoinsIsLimit1(scope) || !scope.Expression.ArgB("from_")
}

func eliminateJoinsIsLimit1(scope *Scope) bool {
	limit := scope.Expression.ArgE("limit")
	if limit == nil {
		return false
	}
	this, ok := limit.Expression().Arg("this").(string)
	return ok && this == "1"
}

// JoinCondition extracts the join condition from a join expression. Returns (source key,
// join key, remaining predicate).
func JoinCondition(join *Expr) ([]*Expr, []*Expr, *Expr) {
	name := join.AliasOrName()
	on := join.ArgE("on")
	if on == nil {
		on = Boolean(true)
	}
	on = on.Copy()
	sourceKey := []*Expr{}
	joinKey := []*Expr{}

	extractCondition := func(condition *Expr) {
		operands := optxUnnestOperands(condition)
		if len(operands) != 2 {
			panic(&ValueError{Msg: "too many values to unpack (expected 2)"})
		}
		left, right := operands[0], operands[1]
		leftTables := ColumnTableNames(left, "")
		rightTables := ColumnTableNames(right, "")

		if leftTables.Has(name) && !rightTables.Has(name) {
			joinKey = append(joinKey, left)
			sourceKey = append(sourceKey, right)
			condition.Replace(Boolean(true))
		} else if rightTables.Has(name) && !leftTables.Has(name) {
			joinKey = append(joinKey, right)
			sourceKey = append(sourceKey, left)
			condition.Replace(Boolean(true))
		}
	}

	// find the join keys
	// SELECT
	// FROM x
	// JOIN y
	//   ON x.a = y.b AND y.b > 1
	//
	// should pull y.b as the join key and x.a as the source key
	if normalizedCNF(on, false) {
		if !on.IsA(KAnd) {
			on = AndExprOpts([]*Expr{on, Boolean(true)}, false, true)
		}

		for condition := range optxFlattenSeq(on, true) {
			if condition.IsA(KEQ) {
				extractCondition(condition)
			}
		}
	} else if normalizedCNF(on, true) {
		var conditions []*Expr

		for condition := range optxFlattenSeq(on, true) {
			var parts []*Expr
			for part := range optxFlattenSeq(condition, true) {
				if part.IsA(KEQ) {
					parts = append(parts, part)
				}
			}
			if len(conditions) == 0 {
				conditions = parts
			} else {
				var temp []*Expr
				for _, p := range parts {
					var cs []*Expr
					for _, c := range conditions {
						if p.Equal(c) {
							cs = append(cs, c)
						}
					}

					if len(cs) > 0 {
						temp = append(temp, p)
						temp = append(temp, cs...)
					}
				}
				conditions = temp
			}
		}

		for _, condition := range conditions {
			extractCondition(condition)
		}
	}

	return sourceKey, joinKey, on
}

// optxExprSet deduplicates exprs by structural equality, keeping the first occurrence.
func optxExprSet(exprs []*Expr) []*Expr {
	var out []*Expr
	for _, e := range exprs {
		if !optxExprIn(e, out) {
			out = append(out, e)
		}
	}
	return out
}

func optxExprIn(e *Expr, exprs []*Expr) bool {
	for _, x := range exprs {
		if x.Equal(e) {
			return true
		}
	}
	return false
}

func hasNearestJoin(joins []*Expr) bool {
	for _, join := range joins {
		if join.ArgB("nearest") {
			return true
		}
	}
	return false
}
