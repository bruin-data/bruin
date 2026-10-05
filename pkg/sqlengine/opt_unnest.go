package sqlengine

// Port of sqlglot/optimizer/unnest_subqueries.py.

// UnnestSubqueries mirrors sqlglot.optimizer.unnest_subqueries.unnest_subqueries.
//
// Rewrite sqlglot AST to convert some predicates with subqueries into joins.
//
// Convert scalar subqueries into cross joins.
// Convert correlated or vectorized subqueries into a group by so it is not a many to many left join.
func UnnestSubqueries(expression *Expr) *Expr {
	nextAliasName := nameSequence("_u_")

	for _, scope := range TraverseScope(expression) {
		sel := scope.Expression
		parent := sel.ParentSelect()
		if parent == nil {
			continue
		}
		if externalColumns := scope.ExternalColumns(); len(externalColumns) > 0 {
			usDecorrelate(sel, parent, externalColumns, nextAliasName)
		} else if scope.Type == ScopeSubquery {
			usUnnest(sel, parent, nextAliasName)
		}
	}

	return expression
}

// usUnnest mirrors unnest_subqueries.unnest.
func usUnnest(sel, parentSelect *Expr, nextAliasName func() string) {
	if len(sel.Selects()) > 1 {
		return
	}

	predicate := sel.FindAncestor(KCondition)
	if predicate == nil ||
		// Do not unnest subqueries inside table-valued functions such as
		// FROM GENERATE_SERIES(...), FROM UNNEST(...) etc in order to preserve join order
		(predicate.IsA(KFunc) && predicate.Parent().IsA(KTable, KFrom, KJoin)) ||
		parentSelect != predicate.ParentSelect() ||
		!parentSelect.ArgB("from_") ||
		// NOT IN has three-valued semantics that the LEFT-JOIN-anti rewrite doesn't preserve:
		// a NULL in the subquery makes NOT IN evaluate to NULL for every outer row.
		(predicate.IsA(KIn) && predicate.Parent().IsA(KNot)) {
		return
	}

	if sel.IsA(KSetOperation) {
		innerAlias := nextAliasName()
		var projections []*Expr
		for _, s := range sel.Selects() {
			projections = append(projections, AliasExpr(ColumnExpr(s.AliasOrName(), innerAlias, nil, nil, nil, nil, true), s.AliasOrName(), nil, true))
		}
		sel = SelectExpr(projections...).SelectFrom(sel.QuerySubquery(innerAlias, true), true)
	}

	alias := nextAliasName()
	clause := predicate.FindAncestor(KHaving, KWhere, KJoin)

	// This subquery returns a scalar and can just be converted to a cross join
	if !predicate.IsA(KIn, KAny) {
		column := ColumnExpr(sel.Selects()[0].AliasOrName(), alias, nil, nil, nil, nil, true)

		var clauseParentSelect *Expr
		if clause != nil {
			clauseParentSelect = clause.ParentSelect()
		}

		hasAgg := func() bool {
			for _, s := range parentSelect.Selects() {
				if FindInScope(s, KAggFunc) != nil {
					return true
				}
			}
			return false
		}

		if (clause.IsA(KHaving) && clauseParentSelect == parentSelect) ||
			((clause == nil || clauseParentSelect != parentSelect) &&
				(parentSelect.ArgB("group") || hasAgg())) {
			column = New(KMax, "this", column)
		} else if !sel.Parent().IsA(KSubquery) {
			return
		}

		joinType := "CROSS"
		var onClause []*Expr
		if predicate.IsA(KExists) {
			// If a subquery returns no rows, cross-joining against it incorrectly eliminates all rows
			// from the parent query. Therefore, we use a LEFT JOIN that always matches (ON TRUE), then
			// check for non-NULL column values to determine whether the subquery contained rows.
			column = optSQBinop(column, KIs, Null()).ExprNot(true)
			joinType = "LEFT"
			onClause = []*Expr{Boolean(true)}
		}

		usReplace(sel.Parent(), column)
		parentSelect.SelectJoin(sel, onClause, nil, true, joinType, alias, false)

		return
	}

	if sel.Find(KLimit, KOffset) != nil {
		return
	}

	if predicate.IsA(KAny) {
		predicate = predicate.FindAncestor(KEQ)

		if predicate == nil || parentSelect != predicate.ParentSelect() {
			return
		}
	}

	column := usOtherOperand(predicate)
	value := sel.Selects()[0]

	joinKey := ColumnExpr(value.Alias(), alias, nil, nil, nil, nil, true)
	joinKeyNotNull := optSQBinop(joinKey, KIs, Null()).ExprNot(true)

	if clause.IsA(KJoin) {
		usReplace(predicate, Boolean(true))
		parentSelect.QueryWhere([]*Expr{joinKeyNotNull}, true, false)
	} else {
		usReplace(predicate, joinKeyNotNull)
	}

	if group := sel.ArgE("group"); group != nil {
		if !usSameExprSet([]*Expr{value.This()}, group.Expressions()) {
			sel = SelectExpr(AliasExpr(ColumnExpr(value.Alias(), "_q", nil, nil, nil, nil, true), value.Alias(), nil, true)).
				SelectFrom(sel.QuerySubquery("_q", false), false).
				SelectGroupBy([]*Expr{ColumnExpr(value.Alias(), "_q", nil, nil, nil, nil, true)}, true, false)
		}
	} else if FindInScope(value.This(), KAggFunc) == nil {
		sel = sel.SelectGroupBy([]*Expr{value.This()}, true, false)
	}

	parentSelect.SelectJoin(sel, []*Expr{optSQBinop(column, KEQ, joinKey)}, nil, true, "LEFT", alias, false)
}

type usKey struct {
	key       *Expr
	column    *Expr
	predicate *Expr
}

type usKeyAlias struct {
	key   *Expr
	alias string
}

// usDecorrelate mirrors unnest_subqueries.decorrelate.
func usDecorrelate(sel, parentSelect *Expr, externalColumns []*Expr, nextAliasName func() string) {
	where := sel.ArgE("where")

	if where == nil || where.Find(KOr) != nil || sel.Find(KLimit, KOffset) != nil {
		return
	}

	tableAlias := nextAliasName()
	var keys []usKey

	// for all external columns in the where statement, find the relevant predicate
	// keys to convert it into a join
	for _, column := range externalColumns {
		if column.FindAncestor(KWhere) != where {
			return
		}

		predicate := column.FindAncestor(KPredicate)

		if predicate == nil || predicate.FindAncestor(KWhere) != where {
			return
		}

		var key *Expr
		if predicate.IsA(KBinary) {
			inLeft := false
			for node := range predicate.Left().Walk(true, nil) {
				if node == column {
					inLeft = true
					break
				}
			}
			if inLeft {
				key = predicate.Right()
			} else {
				key = predicate.Left()
			}
		} else {
			return
		}

		keys = append(keys, usKey{key, column, predicate})
	}

	anyEQ := false
	for _, k := range keys {
		if k.predicate.IsA(KEQ) {
			anyEQ = true
			break
		}
	}
	if !anyEQ {
		return
	}

	isSubqueryProjection := false
	for _, s := range parentSelect.Selects() {
		node := s.Unalias()
		if node.IsA(KSubquery) && node == sel.Parent() {
			isSubqueryProjection = true
			break
		}
	}

	value := sel.Selects()[0]
	// key_aliases is a dict keyed by expressions (structural equality), in insertion order.
	var keyAliases []usKeyAlias
	keyAliasIndex := func(key *Expr) int {
		for i, ka := range keyAliases {
			if ka.key == key || ka.key.Equal(key) {
				return i
			}
		}
		return -1
	}
	setKeyAlias := func(key *Expr, alias string) {
		if i := keyAliasIndex(key); i >= 0 {
			keyAliases[i].alias = alias
			return
		}
		keyAliases = append(keyAliases, usKeyAlias{key, alias})
	}
	var groupBy []*Expr

	for _, k := range keys {
		key, predicate := k.key, k.predicate
		// if we filter on the value of the subquery, it needs to be unique
		if key.Equal(value.This()) {
			setKeyAlias(key, value.Alias())
			groupBy = append(groupBy, key)
		} else {
			if keyAliasIndex(key) < 0 {
				setKeyAlias(key, nextAliasName())
			}
			// all predicates that are equalities must also be in the unique
			// so that we don't do a many to many join
			if predicate.IsA(KEQ) && !usInList(groupBy, key) {
				groupBy = append(groupBy, key)
			}
		}
	}

	parentPredicate := sel.FindAncestor(KPredicate)

	// When the subquery is embedded inside a function (e.g. COALESCE, TRIM) in the SELECT list,
	// the ancestor chain contains no Predicate node AND the subquery is not a direct projection.
	if parentPredicate == nil && !isSubqueryProjection {
		return
	}

	// if the value of the subquery is not an agg or a key, we need to collect it into an array
	// so that it can be grouped. For subquery projections, we use a MAX aggregation instead.
	aggFunc := KArrayAgg
	if isSubqueryProjection {
		aggFunc = KMax
	}
	if value.Find(KAggFunc) == nil && !usInList(groupBy, value.This()) {
		sel.QuerySelect(
			[]*Expr{AliasExpr(New(aggFunc, "this", value.This()), value.Alias(), boolp(false), true)},
			false,
			false,
		)
	}

	// exists queries should not have any selects as it only checks if there are any rows
	// all selects will be added by the optimizer and only used for join keys
	if parentPredicate.IsA(KExists) {
		sel.Set("expressions", []*Expr{})
	}

	for _, ka := range keyAliases {
		key, alias := ka.key, ka.alias
		if usInList(groupBy, key) {
			// add all keys to the projections of the subquery
			// so that we can use it as a join key
			if parentPredicate.IsA(KExists) || !key.Equal(value.This()) {
				sel.QuerySelect([]*Expr{MaybeParse(exprSQL(key)+" AS "+alias, KExpr, "", nil)}, true, false)
			}
		} else {
			sel.QuerySelect([]*Expr{AliasExpr(New(aggFunc, "this", key.Copy()), alias, boolp(false), true)}, true, false)
		}
	}

	alias := ColumnExpr(value.Alias(), tableAlias, nil, nil, nil, nil, true)
	other := usOtherOperand(parentPredicate)
	opType := KNone // type(parent_predicate.parent) if parent_predicate else None
	if parentPredicate != nil && parentPredicate.Parent() != nil {
		opType = parentPredicate.Parent().Kind()
	}

	switch {
	case parentPredicate.IsA(KExists):
		alias = ColumnExpr(keyAliases[0].alias, tableAlias, nil, nil, nil, nil, true)
		parentPredicate = usReplace(parentPredicate, "NOT "+exprSQL(alias)+" IS NULL")
	case parentPredicate.IsA(KAll):
		usAssertBinary(opType)
		predicate := New(opType, "this", other, "expression", ColumnExpr("_x", nil, nil, nil, nil, nil, true))
		parentPredicate = usReplace(parentPredicate.Parent(), "ARRAY_ALL("+exprSQL(alias)+", _x -> "+exprSQL(predicate)+")")
	case parentPredicate.IsA(KAny):
		usAssertBinary(opType)
		if usInList(groupBy, value.This()) {
			predicate := New(opType, "this", other, "expression", alias)
			parentPredicate = usReplace(parentPredicate.Parent(), predicate)
		} else {
			predicate := New(opType, "this", other, "expression", ColumnExpr("_x", nil, nil, nil, nil, nil, true))
			parentPredicate = usReplace(parentPredicate, "ARRAY_ANY("+exprSQL(alias)+", _x -> "+exprSQL(predicate)+")")
		}
	case parentPredicate.IsA(KIn):
		if usInList(groupBy, value.This()) {
			parentPredicate = usReplace(parentPredicate, exprSQL(other)+" = "+exprSQL(alias))
		} else {
			parentPredicate = usReplace(
				parentPredicate,
				"ARRAY_ANY("+exprSQL(alias)+", _x -> _x = "+exprSQL(parentPredicate.This())+")",
			)
		}
	default:
		if isSubqueryProjection && sel.Parent().Alias() != "" {
			alias = AliasExpr(alias, sel.Parent().Alias(), nil, true)
		}

		// COUNT always returns 0 on empty datasets, so we need take that into consideration here
		// by transforming all counts into 0 and using that as the coalesced value
		if value.Find(KCount) != nil {
			removeAggs := func(node *Expr) *Expr {
				if node.IsA(KCount) {
					return LiteralInt(0)
				} else if node.IsA(KAggFunc) {
					return Null()
				}
				return node
			}

			alias = New(KCoalesce, "this", alias, "expressions", []*Expr{value.This().Transform(removeAggs, true)})
		}

		sel.Parent().Replace(alias)
	}

	for _, k := range keys {
		key, column, predicate := k.key, k.column, k.predicate
		predicate.Replace(Boolean(true))
		nested := ColumnExpr(keyAliases[keyAliasIndex(key)].alias, tableAlias, nil, nil, nil, nil, true)

		if isSubqueryProjection {
			key.Replace(nested)
			if !predicate.IsA(KEQ) {
				parentSelect.QueryWhere([]*Expr{predicate}, true, false)
			}
			continue
		}

		if usInList(groupBy, key) {
			key.Replace(nested)
		} else if predicate.IsA(KEQ) {
			parentPredicate = usReplace(
				parentPredicate,
				"("+exprSQL(parentPredicate)+" AND ARRAY_CONTAINS("+exprSQL(nested)+", "+exprSQL(column)+"))",
			)
		} else {
			key.Replace(ToIdentifier("_x", nil))
			parentPredicate = usReplace(
				parentPredicate,
				"("+exprSQL(parentPredicate)+" AND ARRAY_ANY("+exprSQL(nested)+", _x -> "+exprSQL(predicate)+"))",
			)
		}
	}

	grouped := sel.SelectGroupBy(groupBy, true, false)
	var on []*Expr
	for _, k := range keys {
		if k.predicate.IsA(KEQ) {
			on = append(on, k.predicate)
		}
	}
	parentSelect.SelectJoin(grouped, on, nil, true, "LEFT", tableAlias, false)
}

// usReplace mirrors unnest_subqueries._replace: condition may be an *Expr or a SQL string
// (exp.condition parses strings into a Condition with the default dialect and copies expressions).
func usReplace(expression *Expr, condition any) *Expr {
	var cond *Expr
	switch c := condition.(type) {
	case string:
		cond = MaybeParse(c, KCondition, "", nil)
	case *Expr:
		cond = ConditionExpr(c, true)
	}
	return expression.Replace(cond)
}

// usOtherOperand mirrors unnest_subqueries._other_operand.
func usOtherOperand(expression *Expr) *Expr {
	if expression.IsA(KIn) {
		return expression.This()
	}

	if expression.IsA(KAny, KAll) {
		return usOtherOperand(expression.Parent())
	}

	if expression.IsA(KBinary) {
		if expression.Left().IsA(KSubquery, KAny, KExists, KAll) {
			return expression.Right()
		}
		return expression.Left()
	}

	return nil
}

// usInList mirrors Python's `x in some_list` for expressions (identity or structural equality).
func usInList(list []*Expr, x *Expr) bool {
	for _, y := range list {
		if y == x || (y != nil && y.Equal(x)) {
			return true
		}
	}
	return false
}

// usSameExprSet mirrors `set(a) == set(b)` for expressions (structural equality).
func usSameExprSet(a, b []*Expr) bool {
	for _, x := range a {
		if !usInList(b, x) {
			return false
		}
	}
	for _, y := range b {
		if !usInList(a, y) {
			return false
		}
	}
	return true
}

// usAssertBinary mirrors `assert issubclass(op_type, exp.Binary)`.
func usAssertBinary(k Kind) {
	if k == KNone || !k.IsA(KBinary) {
		panic(&ValueError{Msg: "AssertionError"})
	}
}

// optSQBinop mirrors Expr._binop(klass, other) for an expression `other` (used by .is_(), .eq()).
func optSQBinop(this *Expr, kind Kind, other *Expr) *Expr {
	t := this.Copy()
	o := other.Copy()
	if !t.IsA(kind) && !o.IsA(kind) {
		t = wrapIfKind(t, KBinary)
		o = wrapIfKind(o, KBinary)
	}
	return New(kind, "this", t, "expression", o)
}
