package sqlengine

import (
	"fmt"
	"strconv"
)

// Port of sqlglot/transforms.py (plus helper.find_new_name).
//
// Transform functions take and return *Expr. Factories (explode_projection_to_unnest) return
// closures, and preprocess returns a GenFunc usable as a Generator.TRANSFORMS entry.

// findNewName mirrors sqlglot.helper.find_new_name.
func findNewName(taken StrSet, base string) string {
	if !taken.Has(base) {
		return base
	}
	i := 2
	n := base + "_" + strconv.Itoa(i)
	for taken.Has(n) {
		i++
		n = base + "_" + strconv.Itoa(i)
	}
	return n
}

// transformPreprocess mirrors transforms.preprocess(transforms, generator).
//
// It creates a new transform by chaining a sequence of transformations and converts the resulting
// expression to SQL, using either the "_sql" method corresponding to the resulting expression,
// or the appropriate Generator.TRANSFORMS function. generator may be nil (None).
func transformPreprocess(transforms []func(*Expr) *Expr, generator GenFunc) GenFunc {
	return func(g *Generator, expression *Expr) string {
		expressionType := expression.Kind()

		func() {
			defer func() {
				if r := recover(); r != nil {
					if ue, ok := r.(*UnsupportedError); ok {
						g.unsupported(ue.Msg)
						return
					}
					panic(r)
				}
			}()
			expression = transforms[0](expression)
			for _, transform := range transforms[1:] {
				expression = transform(expression)
			}
		}()

		if generator != nil {
			return generator(g, expression)
		}

		if sqlHandler := g.s.methods[expression.Kind()]; sqlHandler != nil {
			return sqlHandler(g, expression)
		}

		if transformsHandler := g.s.TRANSFORMS[expression.Kind()]; transformsHandler != nil {
			if expressionType == expression.Kind() {
				if expression.IsA(KFunc) {
					return g.functionFallbackSQL(expression)
				}

				// Ensures we don't enter an infinite loop. This can happen when the original expression
				// has the same type as the final expression and there's no _sql method available for it,
				// because then it'd re-enter _to_sql.
				panic(&ValueError{Msg: fmt.Sprintf("Expr type %s requires a _sql method in order to be transformed.", expression.Kind().Name())})
			}

			return transformsHandler(g, expression)
		}

		panic(&ValueError{Msg: fmt.Sprintf("Unsupported expression type %s.", expression.Kind().Name())})
	}
}

// transformUnnestGenerateDateArrayUsingRecursiveCte mirrors transforms.unnest_generate_date_array_using_recursive_cte.
func transformUnnestGenerateDateArrayUsingRecursiveCte(expression *Expr) *Expr {
	if expression.IsA(KSelect) {
		count := 0
		var recursiveCtes []*Expr

		for unnest := range expression.FindAll(KUnnest) {
			if !unnest.Parent().IsA(KFrom, KJoin) ||
				len(unnest.Expressions()) != 1 ||
				!unnest.Expressions()[0].IsA(KGenerateDateArray) {
				continue
			}

			generateDateArray := unnest.Expressions()[0]
			start := generateDateArray.ArgE("start")
			end := generateDateArray.ArgE("end")
			step := generateDateArray.ArgE("step")

			if start == nil || end == nil || !step.IsA(KInterval) {
				continue
			}

			alias := unnest.ArgE("alias")
			var columnName any = "date_value"
			if alias.IsA(KTableAlias) {
				columns := alias.ArgL("columns")
				if len(columns) == 0 {
					panic(&tfmPyError{Type: "IndexError", Msg: "list index out of range"})
				}
				columnName = columns[0]
			}

			start = tfmCast(start, "date", true)
			dateAdd := tfmFunc("date_add", columnName, LiteralNumber(step.Name()), step.ArgE("unit"))
			castDateAdd := tfmCast(dateAdd, "date", true)

			cteName := "_generated_dates"
			if count != 0 {
				cteName += "_" + strconv.Itoa(count)
			}

			baseQuery := tfmSelect([]any{tfmAlias(start, columnName, nil, nil, true)}, true)
			recursiveQuery := tfmWhere(
				tfmSelectFrom(tfmSelect([]any{castDateAdd}, true), cteName, true),
				[]any{tfmBinop(KLTE, castDateAdd, tfmCast(end, "date", true), false)},
				true,
				true,
			)
			cteQuery := tfmUnion(baseQuery, recursiveQuery, false, true)

			generateDatesQuery := tfmSelectFrom(tfmSelect([]any{columnName}, true), cteName, true)
			unnest.Replace(tfmSubquery(generateDatesQuery, cteName, true))

			recursiveCtes = append(recursiveCtes, tfmAlias(New(KCTE, "this", cteQuery), cteName, []any{columnName}, nil, true))
			count++
		}

		if len(recursiveCtes) > 0 {
			withExpression := expression.ArgE("with_")
			if withExpression == nil {
				withExpression = New(KWith)
			}
			withExpression.Set("recursive", true)
			withExpression.Set("expressions", append(append([]*Expr{}, recursiveCtes...), withExpression.Expressions()...))
			expression.Set("with_", withExpression)
		}
	}

	return expression
}

// transformUnnestGenerateSeries mirrors transforms.unnest_generate_series:
// unnests GENERATE_SERIES or SEQUENCE table references.
func transformUnnestGenerateSeries(expression *Expr) *Expr {
	this := expression.This()
	if expression.IsA(KTable) && this.IsA(KGenerateSeries) {
		unnest := New(KUnnest, "expressions", []*Expr{this})
		if alias := expression.Alias(); alias != "" {
			return tfmAlias(unnest, "_u", []any{alias}, nil, false)
		}

		return unnest
	}

	return expression
}

// transformEliminateDistinctOn mirrors transforms.eliminate_distinct_on: converts SELECT DISTINCT ON
// statements to a subquery with a window function.
func transformEliminateDistinctOn(expression *Expr) *Expr {
	if expression.IsA(KSelect) &&
		expression.ArgB("distinct") &&
		expression.ArgE("distinct").ArgE("on").IsA(KTuple) {
		rowNumberWindowAlias := findNewName(newStrSet(expression.NamedSelects()...), "_row_number")

		distinctCols := expression.ArgE("distinct").Pop().ArgE("on").Expressions()
		window := New(KWindow, "this", New(KRowNumber), "partition_by", distinctCols)

		if order := expression.ArgE("order"); order != nil {
			window.Set("order", order.Pop())
		} else {
			copies := make([]*Expr, len(distinctCols))
			for i, c := range distinctCols {
				copies[i] = c.Copy()
			}
			window.Set("order", New(KOrder, "expressions", copies))
		}

		tfmSelectAppend(expression, []any{tfmAlias(window, rowNumberWindowAlias, nil, nil, true)}, true, false)

		// We add aliases to the projections so that we can safely reference them in the outer query
		var newSelects []*Expr
		takenNames := newStrSet(rowNumberWindowAlias)
		selects := expression.Selects()
		for _, sel := range append([]*Expr{}, selects[:len(selects)-1]...) {
			if sel.IsStar() {
				newSelects = []*Expr{Star()}
				break
			}

			if !sel.IsA(KAlias) {
				name := sel.OutputName()
				if name == "" {
					name = "_col"
				}
				alias := findNewName(takenNames, name)
				var quoted *bool
				if sel.IsA(KColumn) {
					quoted = tfmOptBool(sel.This().Arg("quoted"))
				}
				sel = sel.Replace(tfmAlias(sel, alias, nil, quoted, true))
			}

			takenNames.Add(sel.OutputName())
			newSelects = append(newSelects, sel.ArgE("alias"))
		}

		return tfmWhere(
			tfmSelectFrom(tfmSelect(tfmAnys(newSelects), false), tfmSubquery(expression, "_t", false), false),
			[]any{tfmBinop(KEQ, tfmColumn(rowNumberWindowAlias, nil, nil, true), 1, false)},
			true,
			false,
		)
	}

	return expression
}

// transformEliminateQualify mirrors transforms.eliminate_qualify: converts SELECT statements that
// contain the QUALIFY clause into subqueries, filtered equivalently.
//
// Some dialects don't support window functions in the WHERE clause, so we need to include them as
// projections in the subquery, in order to refer to them in the outer filter using aliases. Also,
// if a column is referenced in the QUALIFY clause but is not selected, we need to include it too,
// otherwise we won't be able to refer to it in the outer query's WHERE clause. Finally, if a
// newly aliased projection is referenced in the QUALIFY clause, it will be replaced by the
// corresponding expression to avoid creating invalid column references.
func transformEliminateQualify(expression *Expr) *Expr {
	if expression.IsA(KSelect) && expression.ArgB("qualify") {
		taken := newStrSet(expression.NamedSelects()...)
		for _, sel := range expression.Selects() {
			if sel.AliasOrName() == "" {
				alias := findNewName(taken, "_c")
				sel.Replace(tfmAlias(sel, alias, nil, nil, true))
				taken.Add(alias)
			}
		}

		selectAliasOrName := func(sel *Expr) any {
			aliasOrName := sel.AliasOrName()
			identifier := sel.ArgE("alias")
			if identifier == nil {
				identifier = sel.This()
			}
			if identifier.IsA(KIdentifier) {
				return tfmColumn(aliasOrName, nil, tfmOptBool(identifier.Arg("quoted")), true)
			}
			return aliasOrName
		}

		var outerArgs []any
		for _, sel := range expression.Selects() {
			outerArgs = append(outerArgs, selectAliasOrName(sel))
		}
		outerSelects := tfmSelect(outerArgs, true)
		qualifyFilters := expression.ArgE("qualify").Pop().This()
		expressionByAlias := map[string]*Expr{}
		for _, sel := range expression.Selects() {
			if sel.IsA(KAlias) {
				expressionByAlias[sel.Alias()] = sel.This()
			}
		}

		selectCandidates := []Kind{KWindow, KColumn}
		if expression.IsStar() {
			selectCandidates = []Kind{KWindow}
		}
		for _, selectCandidate := range qualifyFilters.FindAllList(selectCandidates...) {
			if selectCandidate.IsA(KWindow) {
				if len(expressionByAlias) > 0 {
					for column := range selectCandidate.FindAll(KColumn) {
						if expr := expressionByAlias[column.Name()]; expr != nil {
							column.Replace(expr)
						}
					}
				}

				alias := findNewName(newStrSet(expression.NamedSelects()...), "_w")
				tfmSelectAppend(expression, []any{tfmAlias(selectCandidate, alias, nil, nil, true)}, true, false)
				column := tfmColumn(alias, nil, nil, true)

				if selectCandidate.Parent().IsA(KQualify) {
					qualifyFilters = column
				} else {
					selectCandidate.Replace(column)
				}
			} else if !newStrSet(expression.NamedSelects()...).Has(selectCandidate.Name()) {
				tfmSelectAppend(expression, []any{selectCandidate.Copy()}, true, false)
			}
		}

		return tfmWhere(
			tfmSelectFrom(outerSelects, tfmSubquery(expression, "_t", false), false),
			[]any{qualifyFilters},
			true,
			false,
		)
	}

	return expression
}

// transformRemovePrecisionParameterizedTypes mirrors transforms.remove_precision_parameterized_types.
//
// Some dialects only allow the precision for parameterized types to be defined in the DDL and not in
// other expressions. This transforms removes the precision from parameterized types in expressions.
func transformRemovePrecisionParameterizedTypes(expression *Expr) *Expr {
	for node := range expression.FindAll(KDataType) {
		kept := []*Expr{}
		for _, e := range node.Expressions() {
			if !e.IsA(KDataTypeParam) {
				kept = append(kept, e)
			}
		}
		node.Set("expressions", kept)
	}

	return expression
}

// transformUnqualifyUnnest mirrors transforms.unqualify_unnest: removes references to unnest table
// aliases, added by the optimizer's qualify_columns step.
func transformUnqualifyUnnest(expression *Expr) *Expr {
	if expression.IsA(KSelect) {
		unnestAliases := newStrSet()
		for unnest := range FindAllInScope(expression, KUnnest) {
			if unnest.Parent().IsA(KFrom, KJoin) {
				unnestAliases.Add(unnest.Alias())
			}
		}
		if len(unnestAliases) > 0 {
			for column := range expression.FindAll(KColumn) {
				parts := column.Parts()
				if len(parts) == 0 {
					panic(&tfmPyError{Type: "IndexError", Msg: "list index out of range"})
				}
				leftmostPart := parts[0]
				if leftmostPart.ArgKey() != "this" {
					if name, ok := leftmostPart.Arg("this").(string); ok && unnestAliases.Has(name) {
						leftmostPart.Pop()
					}
				}
			}
		}
	}

	return expression
}

// transformUnnestToExplode mirrors transforms.unnest_to_explode(expression) (unnest_using_arrays_zip=True).
func transformUnnestToExplode(expression *Expr) *Expr {
	return transformUnnestToExplodeFull(expression, true)
}

// transformUnnestToExplodeFull mirrors transforms.unnest_to_explode(expression, unnest_using_arrays_zip):
// converts cross join unnest into lateral view explode.
func transformUnnestToExplodeFull(expression *Expr, unnestUsingArraysZip bool) *Expr {
	unnestZipExprs := func(u *Expr, unnestExprs []*Expr, hasMultiExpr bool) []*Expr {
		if hasMultiExpr {
			if !unnestUsingArraysZip {
				panic(&UnsupportedError{Msg: "Cannot transpile UNNEST with multiple input arrays"})
			}

			// Use INLINE(ARRAYS_ZIP(...)) for multiple expressions
			zipExprs := []*Expr{New(KAnonymous, "this", "ARRAYS_ZIP", "expressions", unnestExprs)}
			u.Set("expressions", zipExprs)
			return zipExprs
		}
		return unnestExprs
	}

	udtfType := func(u *Expr, hasMultiExpr bool) Kind {
		if u.ArgB("offset") {
			return KPosexplode
		}
		if hasMultiExpr {
			return KInline
		}
		return KExplode
	}

	// offsetColumn mirrors `offset if isinstance(offset, exp.Identifier) else exp.to_identifier("pos")`.
	offsetColumn := func(offset any) *Expr {
		if o, ok := offset.(*Expr); ok && o.IsA(KIdentifier) {
			return o
		}
		return ToIdentifier("pos", nil)
	}

	if expression.IsA(KSelect) {
		from := expression.ArgE("from_")

		if from != nil && from.This().IsA(KUnnest) {
			unnest := from.This()
			alias := unnest.ArgE("alias")
			exprs := unnest.Expressions()
			hasMultiExpr := len(exprs) > 1
			zipped := unnestZipExprs(unnest, exprs, hasMultiExpr)
			if len(zipped) == 0 {
				panic(&ValueError{Msg: "not enough values to unpack (expected at least 1, got 0)"})
			}
			this := zipped[0]

			var columns []*Expr
			if alias != nil {
				columns = alias.ArgL("columns")
			}
			if offset := unnest.Arg("offset"); truthy(offset) {
				// list.insert mutates alias.args["columns"] in place when it is a non-empty list.
				aliased := len(columns) > 0
				columns = append([]*Expr{offsetColumn(offset)}, columns...)
				if aliased {
					alias.SetArgRaw("columns", columns)
				}
			}

			var tableAlias *Expr
			if alias != nil {
				tableAlias = New(KTableAlias, "this", alias.This(), "columns", columns)
			}
			unnest.Replace(New(
				KTable,
				"this", New(udtfType(unnest, hasMultiExpr), "this", this),
				"alias", tableAlias,
			))
		}

		joins := expression.ArgL("joins")
		for _, join := range append([]*Expr{}, joins...) {
			joinExpr := join.This()

			isLateral := joinExpr.IsA(KLateral)

			unnest := joinExpr
			if isLateral {
				unnest = joinExpr.This()
			}

			if unnest.IsA(KUnnest) {
				var alias *Expr
				if isLateral {
					alias = joinExpr.ArgE("alias")
				} else {
					alias = unnest.ArgE("alias")
				}

				if alias == nil {
					panic(&UnsupportedError{Msg: "CROSS JOIN UNNEST to LATERAL VIEW EXPLODE transformation requires an alias"})
				}

				exprs := unnest.Expressions()
				// The number of unnest.expressions will be changed by _unnest_zip_exprs, we need to record it here
				hasMultiExpr := len(exprs) > 1
				exprs = unnestZipExprs(unnest, exprs, hasMultiExpr)

				// joins.remove(join): removes the first element that is (or equals) join, in place.
				i := tfmListIndex(joins, join)
				if i < 0 {
					panic(&ValueError{Msg: "list.remove(x): x not in list"})
				}
				joins = append(joins[:i:i], joins[i+1:]...)
				expression.SetArgRaw("joins", joins)

				aliasCols := alias.ArgL("columns")

				// # Handle UNNEST to LATERAL VIEW EXPLODE: Exception is raised when there are 0 or > 2 aliases
				// Spark LATERAL VIEW EXPLODE requires single alias for array/struct and two for Map type column unlike unnest in trino/presto which can take an arbitrary amount.
				// Refs: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-lateral-view.html

				if !hasMultiExpr && len(aliasCols) != 1 && len(aliasCols) != 2 {
					panic(&UnsupportedError{Msg: "CROSS JOIN UNNEST to LATERAL VIEW EXPLODE transformation requires explicit column aliases"})
				}

				if offset := unnest.Arg("offset"); truthy(offset) {
					aliased := len(aliasCols) > 0
					aliasCols = append([]*Expr{offsetColumn(offset)}, aliasCols...)
					if aliased {
						alias.SetArgRaw("columns", aliasCols)
					}
				}

				n := min(len(exprs), len(aliasCols))
				for k := range n {
					expression.Append("laterals", New(
						KLateral,
						"this", New(udtfType(unnest, hasMultiExpr), "this", exprs[k]),
						"view", true,
						"alias", New(KTableAlias, "this", alias.This(), "columns", aliasCols),
					))
				}
			}
		}
	}

	return expression
}

// transformExplodeProjectionToUnnest mirrors transforms.explode_projection_to_unnest(index_offset):
// converts explode/posexplode projections into unnests.
func transformExplodeProjectionToUnnest(indexOffset int) func(*Expr) *Expr {
	return func(expression *Expr) *Expr {
		if expression.IsA(KSelect) {
			takenSelectNames := newStrSet(expression.NamedSelects()...)
			takenSourceNames := newStrSet()
			for _, ref := range NewScope(expression, nil, nil, nil, ScopeRoot, nil, nil, false).References() {
				takenSourceNames.Add(ref.Name)
			}

			newName := func(names StrSet, name string) string {
				name = findNewName(names, name)
				names.Add(name)
				return name
			}

			var arrays []*Expr
			seriesAlias := newName(takenSelectNames, "pos")
			series := tfmAlias(
				New(KUnnest, "expressions", []*Expr{New(KGenerateSeries, "start", LiteralInt(indexOffset))}),
				newName(takenSourceNames, "_u"),
				[]any{seriesAlias},
				nil,
				true,
			)

			// we use list here because expression.selects is mutated inside the loop
			for _, sel := range append([]*Expr{}, expression.Selects()...) {
				explode := sel.Find(KExplode)

				if explode != nil {
					var posAlias any = ""
					var explodeAlias any = ""
					var alias *Expr

					if sel.IsA(KAlias) {
						explodeAlias = sel.Arg("alias")
						alias = sel
					} else if sel.IsA(KAliases) {
						aliases := sel.Expressions()
						if len(aliases) < 2 {
							panic(&tfmPyError{Type: "IndexError", Msg: "list index out of range"})
						}
						posAlias = aliases[0]
						explodeAlias = aliases[1]
						alias = sel.Replace(tfmAlias(sel.This(), "", nil, nil, false))
					} else {
						alias = sel.Replace(tfmAlias(sel, "", nil, nil, true))
						explode = alias.Find(KExplode)
						tfmAssert(explode != nil, "")
					}

					isPosexplode := explode.IsA(KPosexplode)
					explodeArg := explode.This()

					if explode.IsA(KExplodeOuter) {
						bracket := New(KBracket, "this", explodeArg.Copy(), "expressions", []*Expr{tfmConvert(0, true)})
						bracket.Set("safe", true)
						bracket.Set("offset", true)
						explodeArg = tfmFunc(
							"IF",
							tfmBinop(KEQ, tfmFunc("ARRAY_SIZE", tfmFunc("COALESCE", explodeArg, New(KArray))), 0, false),
							tfmArray([]any{bracket}, false),
							explodeArg,
						)
					}

					// This ensures that we won't use [POS]EXPLODE's argument as a new selection
					if explodeArg.IsA(KColumn) {
						takenSelectNames.Add(explodeArg.OutputName())
					}

					unnestSourceAlias := newName(takenSourceNames, "_u")

					if !truthy(explodeAlias) {
						explodeAlias = newName(takenSelectNames, "col")

						if isPosexplode {
							posAlias = newName(takenSelectNames, "pos")
						}
					}

					if !truthy(posAlias) {
						posAlias = newName(takenSelectNames, "pos")
					}

					alias.Set("alias", tfmToIdentifier(explodeAlias, nil, true))

					seriesTableAlias := series.ArgE("alias").This()
					column := New(
						KIf,
						"this", tfmBinop(KEQ,
							tfmColumn(seriesAlias, seriesTableAlias, nil, true),
							tfmColumn(posAlias, unnestSourceAlias, nil, true),
							false),
						"true", tfmColumn(explodeAlias, unnestSourceAlias, nil, true),
					)

					explode.Replace(column)

					if isPosexplode {
						expressions := expression.Expressions()
						at := tfmListIndex(expressions, alias)
						if at < 0 {
							panic(&ValueError{Msg: "expression is not in list"})
						}
						posSelect := tfmAlias(New(
							KIf,
							"this", tfmBinop(KEQ,
								tfmColumn(seriesAlias, seriesTableAlias, nil, true),
								tfmColumn(posAlias, unnestSourceAlias, nil, true),
								false),
							"true", tfmColumn(posAlias, unnestSourceAlias, nil, true),
						), posAlias, nil, nil, true)
						inserted := make([]*Expr, 0, len(expressions)+1)
						inserted = append(inserted, expressions[:at+1]...)
						inserted = append(inserted, posSelect)
						inserted = append(inserted, expressions[at+1:]...)
						expression.Set("expressions", inserted)
					}

					if len(arrays) == 0 {
						if expression.ArgB("from_") {
							tfmJoin(expression, series, "CROSS", false)
						} else {
							tfmSelectFrom(expression, series, false)
						}
					}

					size := New(KArraySize, "this", explodeArg.Copy())
					arrays = append(arrays, size)

					// trino doesn't support left join unnest with on conditions
					// if it did, this would be much simpler
					tfmJoin(
						expression,
						tfmAlias(
							New(
								KUnnest,
								"expressions", []*Expr{explodeArg.Copy()},
								"offset", tfmToIdentifier(posAlias, nil, true),
							),
							unnestSourceAlias,
							[]any{explodeAlias},
							nil,
							true,
						),
						"CROSS",
						false,
					)

					if indexOffset != 1 {
						size = tfmBinop(KSub, size, 1, false)
					}

					tfmWhere(
						expression,
						[]any{tfmOr([]any{
							tfmBinop(KEQ,
								tfmColumn(seriesAlias, seriesTableAlias, nil, true),
								tfmColumn(posAlias, unnestSourceAlias, nil, true),
								false),
							tfmAnd([]any{
								tfmBinop(KGT, tfmColumn(seriesAlias, seriesTableAlias, nil, true), size, false),
								tfmBinop(KEQ, tfmColumn(posAlias, unnestSourceAlias, nil, true), size, false),
							}, true, true),
						}, true, true)},
						true,
						false,
					)
				}
			}

			if len(arrays) > 0 {
				end := New(KGreatest, "this", arrays[0], "expressions", append([]*Expr{}, arrays[1:]...))

				if indexOffset != 1 {
					end = tfmBinop(KSub, end, 1-indexOffset, false)
				}
				series.Expressions()[0].Set("end", end)
			}
		}

		return expression
	}
}

// tfmIsPercentile mirrors isinstance(expression, exp.PERCENTILES).
func tfmIsPercentile(e *Expr) bool { return e.IsA(KPercentileCont, KPercentileDisc) }

// transformAddWithinGroupForPercentiles mirrors transforms.add_within_group_for_percentiles:
// transforms percentiles by adding a WITHIN GROUP clause to them.
func transformAddWithinGroupForPercentiles(expression *Expr) *Expr {
	if tfmIsPercentile(expression) &&
		!expression.Parent().IsA(KWithinGroup) &&
		expression.Expression() != nil {
		column := expression.This().Pop()
		expression.Set("this", expression.Expression().Pop())
		order := New(KOrder, "expressions", []*Expr{New(KOrdered, "this", column)})
		expression = New(KWithinGroup, "this", expression, "expression", order)
	}

	return expression
}

// transformRemoveWithinGroupForPercentiles mirrors transforms.remove_within_group_for_percentiles:
// transforms percentiles by getting rid of their corresponding WITHIN GROUP clause.
func transformRemoveWithinGroupForPercentiles(expression *Expr) *Expr {
	if expression.IsA(KWithinGroup) &&
		tfmIsPercentile(expression.This()) &&
		expression.Expression().IsA(KOrder) {
		quantile := expression.This().This()
		inputValue := expression.Find(KOrdered).This()
		return expression.Replace(New(KApproxQuantile, "this", inputValue, "quantile", quantile))
	}

	return expression
}

// transformAddRecursiveCteColumnNames mirrors transforms.add_recursive_cte_column_names: uses
// projection output names in recursive CTE definitions to define the CTEs' columns.
func transformAddRecursiveCteColumnNames(expression *Expr) *Expr {
	if expression.IsA(KWith) && expression.ArgB("recursive") {
		nextName := nameSequence("_c_")

		for _, cte := range expression.Expressions() {
			if len(cte.ArgE("alias").ArgL("columns")) == 0 {
				query := cte.This()
				if query.IsA(KSetOperation) {
					query = query.This()
				}

				columns := []*Expr{}
				for _, s := range query.Selects() {
					name := s.AliasOrName()
					if name == "" {
						name = nextName()
					}
					columns = append(columns, ToIdentifier(name, nil))
				}
				cte.ArgE("alias").Set("columns", columns)
			}
		}
	}

	return expression
}

// transformEpochCastToTs mirrors transforms.epoch_cast_to_ts: replaces 'epoch' in casts by the
// equivalent date literal.
func transformEpochCastToTs(expression *Expr) *Expr {
	if expression.IsA(KCast, KTryCast) &&
		pyLower(expression.Name()) == "epoch" &&
		DataType_TEMPORAL_TYPES.Has(expression.ArgE("to").DTypeOf()) {
		expression.This().Replace(LiteralString("1970-01-01 00:00:00"))
	}

	return expression
}

// transformEliminateSemiAndAntiJoins mirrors transforms.eliminate_semi_and_anti_joins: converts SEMI
// and ANTI joins into equivalent forms that use EXIST instead.
func transformEliminateSemiAndAntiJoins(expression *Expr) *Expr {
	if expression.IsA(KSelect) {
		for _, join := range append([]*Expr{}, expression.ArgL("joins")...) {
			on := join.ArgE("on")
			if kind := join.KindText(); on != nil && (kind == "SEMI" || kind == "ANTI") {
				subquery := tfmWhere(tfmSelectFrom(tfmSelect([]any{"1"}, true), join.This(), true), []any{on}, true, true)
				exists := New(KExists, "this", subquery)
				if join.KindText() == "ANTI" {
					exists = tfmNot(exists, false)
				}

				join.Pop()
				tfmWhere(expression, []any{exists}, true, false)
			}
		}
	}

	return expression
}

// transformEliminateFullOuterJoin mirrors transforms.eliminate_full_outer_join.
//
// Converts a query with a FULL OUTER join to a union of identical queries that
// use LEFT/RIGHT OUTER joins instead. This transformation currently only works
// for queries that have a single FULL OUTER join.
func transformEliminateFullOuterJoin(expression *Expr) *Expr {
	if expression.IsA(KSelect) {
		var fullOuterJoinIndexes []int
		var fullOuterJoins []*Expr
		for index, join := range expression.ArgL("joins") {
			if join.SideText() == "FULL" {
				fullOuterJoinIndexes = append(fullOuterJoinIndexes, index)
				fullOuterJoins = append(fullOuterJoins, join)
			}
		}

		if len(fullOuterJoins) == 1 {
			expressionCopy := expression.Copy()
			expression.Set("limit", nil)
			index, fullOuterJoin := fullOuterJoinIndexes[0], fullOuterJoins[0]

			tables := [2]string{expression.ArgE("from_").AliasOrName(), fullOuterJoin.AliasOrName()}
			joinConditions := fullOuterJoin.ArgE("on")
			if joinConditions == nil {
				using, ok := fullOuterJoin.Arg("using").([]*Expr)
				if !ok {
					panic(&tfmPyError{Type: "TypeError", Msg: "'NoneType' object is not iterable"})
				}
				var conditions []any
				for _, col := range using {
					conditions = append(conditions, tfmBinop(KEQ,
						tfmColumn(col, tables[0], nil, true),
						tfmColumn(col, tables[1], nil, true),
						false))
				}
				joinConditions = tfmAnd(conditions, true, true)
			}

			fullOuterJoin.Set("side", "left")
			antiJoinClause := tfmWhere(
				tfmSelectFrom(tfmSelect([]any{"1"}, true), expression.ArgE("from_"), true),
				[]any{joinConditions},
				true,
				true,
			)
			expressionCopy.ArgL("joins")[index].Set("side", "right")
			expressionCopy = tfmWhere(expressionCopy, []any{tfmNot(New(KExists, "this", antiJoinClause), true)}, true, true)
			expressionCopy.Set("with_", nil) // remove CTEs from RIGHT side
			expression.Set("order", nil)     // remove order by from LEFT side

			return tfmUnion(expression, expressionCopy, false, false)
		}
	}

	return expression
}

// transformMoveCtesToTopLevel mirrors transforms.move_ctes_to_top_level.
//
// Some dialects (e.g. Hive, T-SQL, Spark prior to version 3) only allow CTEs to be
// defined at the top-level, so for example queries like:
//
//	SELECT * FROM (WITH t(c) AS (SELECT 1) SELECT * FROM t) AS subq
//
// are invalid in those dialects. This transformation can be used to ensure all CTEs are
// moved to the top level so that the final SQL code is valid from a syntax standpoint.
//
// TODO: handle name clashes whilst moving CTEs (it can get quite tricky & costly).
func transformMoveCtesToTopLevel(expression *Expr) *Expr {
	topLevelWith := expression.ArgE("with_")
	for innerWith := range expression.FindAll(KWith) {
		if innerWith.Parent() == expression {
			continue
		}

		if topLevelWith == nil {
			topLevelWith = innerWith.Pop()
			expression.Set("with_", topLevelWith)
		} else {
			if innerWith.ArgB("recursive") {
				topLevelWith.Set("recursive", true)
			}

			parentCte := innerWith.FindAncestor(KCTE)
			innerWith.Pop()

			if parentCte != nil {
				existing := topLevelWith.Expressions()
				i := tfmListIndex(existing, parentCte)
				if i < 0 {
					panic(&ValueError{Msg: "CTE is not in list"})
				}
				merged := make([]*Expr, 0, len(existing)+len(innerWith.Expressions()))
				merged = append(merged, existing[:i]...)
				merged = append(merged, innerWith.Expressions()...)
				merged = append(merged, existing[i:]...)
				topLevelWith.Set("expressions", merged)
			} else {
				topLevelWith.Set("expressions", append(append([]*Expr{}, topLevelWith.Expressions()...), innerWith.Expressions()...))
			}
		}
	}

	return expression
}

// tfmEnsureBoolTypes is (exp.DType.UNKNOWN, *exp.DataType.NUMERIC_TYPES).
var tfmEnsureBoolTypes = append([]DType{DT_UNKNOWN}, DataType_NUMERIC_TYPES.Items()...)

// transformEnsureBools mirrors transforms.ensure_bools: converts numeric values used in conditions
// into explicit boolean expressions.
func transformEnsureBools(expression *Expr) *Expr {
	ensureBool := func(node *Expr) {
		if node.IsNumber() ||
			(!node.IsA(KSubqueryPredicate) && tfmIsTypeD(node, tfmEnsureBoolTypes...)) ||
			(node.IsA(KColumn) && node.Type() == nil) {
			node.Replace(tfmBinop(KNEQ, node, 0, false))
		}
	}

	for node := range expression.Walk(true, nil) {
		tfmCanonicalizeEnsureBools(node, ensureBool)
	}

	return expression
}

// tfmCanonicalizeEnsureBools mirrors sqlglot.optimizer.canonicalize.ensure_bools.
func tfmCanonicalizeEnsureBools(expression *Expr, replaceFunc func(*Expr)) *Expr {
	if expression.IsA(KConnector) {
		replaceFunc(expression.Left())
		replaceFunc(expression.Right())
	} else if expression.IsA(KNot) {
		replaceFunc(expression.This())
		// We can't replace num in CASE x WHEN num ..., because it's not the full predicate
	} else if expression.IsA(KIf) && !(expression.Parent().IsA(KCase) && expression.Parent().ArgB("this")) {
		replaceFunc(expression.This())
	} else if expression.IsA(KWhere, KHaving) {
		replaceFunc(expression.This())
	}

	return expression
}

// transformUnqualifyColumns mirrors transforms.unqualify_columns.
func transformUnqualifyColumns(expression *Expr) *Expr {
	for column := range expression.FindAll(KColumn) {
		// We only wanna pop off the table, db, catalog args
		parts := column.Parts()
		if len(parts) == 0 {
			continue
		}
		for _, part := range parts[:len(parts)-1] {
			part.Pop()
		}
	}

	return expression
}

// transformRemoveUniqueConstraints mirrors transforms.remove_unique_constraints.
func transformRemoveUniqueConstraints(expression *Expr) *Expr {
	tfmAssert(expression.IsA(KCreate), "")
	for constraint := range expression.FindAll(KUniqueColumnConstraint) {
		if constraint.Parent() != nil {
			constraint.Parent().Pop()
		}
	}

	return expression
}

// transformCtasWithTmpTablesToCreateTmpView mirrors transforms.ctas_with_tmp_tables_to_create_tmp_view(expression)
// (with the default tmp_storage_provider=lambda e: e).
func transformCtasWithTmpTablesToCreateTmpView(expression *Expr) *Expr {
	return transformCtasWithTmpTablesToCreateTmpViewFull(expression, nil)
}

// transformCtasWithTmpTablesToCreateTmpViewFull mirrors
// transforms.ctas_with_tmp_tables_to_create_tmp_view(expression, tmp_storage_provider).
// A nil tmpStorageProvider means the default identity function.
func transformCtasWithTmpTablesToCreateTmpViewFull(expression *Expr, tmpStorageProvider func(*Expr) *Expr) *Expr {
	if tmpStorageProvider == nil {
		tmpStorageProvider = func(e *Expr) *Expr { return e }
	}

	tfmAssert(expression.IsA(KCreate), "")
	properties := expression.ArgE("properties")
	temporary := false
	if properties != nil {
		for _, prop := range properties.Expressions() {
			if prop.IsA(KTemporaryProperty) {
				temporary = true
				break
			}
		}
	}

	// CTAS with temp tables map to CREATE TEMPORARY VIEW
	if expression.KindText() == "TABLE" && temporary {
		if expression.Expression() != nil {
			return New(
				KCreate,
				"kind", "TEMPORARY VIEW",
				"this", expression.This(),
				"expression", expression.Expression(),
			)
		}
		return tmpStorageProvider(expression)
	}

	return expression
}

// transformMoveSchemaColumnsToPartitionedBy mirrors transforms.move_schema_columns_to_partitioned_by.
//
// In Hive, the PARTITIONED BY property acts as an extension of a table's schema. When the
// PARTITIONED BY value is an array of column names, they are transformed into a schema.
// The corresponding columns are removed from the create statement.
func transformMoveSchemaColumnsToPartitionedBy(expression *Expr) *Expr {
	tfmAssert(expression.IsA(KCreate), "")
	schema := expression.This()
	kind := expression.KindText()
	isPartitionable := kind == "TABLE" || kind == "VIEW"

	if schema.IsA(KSchema) && isPartitionable {
		prop := expression.Find(KPartitionedByProperty)
		if prop != nil && prop.This() != nil && !prop.This().IsA(KSchema) {
			columns := newStrSet()
			for _, v := range prop.This().Expressions() {
				columns.Add(pyUpper(v.Name()))
			}
			schemaExprs := schema.Expressions()
			partitions := []*Expr{}
			for _, col := range schemaExprs {
				if columns.Has(pyUpper(col.Name())) {
					partitions = append(partitions, col)
				}
			}
			kept := []*Expr{}
			for _, e := range schemaExprs {
				if tfmListIndex(partitions, e) < 0 {
					kept = append(kept, e)
				}
			}
			schema.Set("expressions", kept)
			prop.Replace(New(KPartitionedByProperty, "this", New(KSchema, "expressions", partitions)))
			expression.Set("this", schema)
		}
	}

	return expression
}

// transformMovePartitionedByToSchemaColumns mirrors transforms.move_partitioned_by_to_schema_columns.
//
// Spark 3 supports both "HIVEFORMAT" and "DATASOURCE" formats for CREATE TABLE.
//
// Currently, SQLGlot uses the DATASOURCE format for Spark 3.
func transformMovePartitionedByToSchemaColumns(expression *Expr) *Expr {
	tfmAssert(expression.IsA(KCreate), "")
	prop := expression.Find(KPartitionedByProperty)
	if prop != nil && prop.This() != nil && prop.This().IsA(KSchema) {
		allColumnDefs := true
		for _, e := range prop.This().Expressions() {
			if !(e.IsA(KColumnDef) && e.ArgB("kind")) {
				allColumnDefs = false
				break
			}
		}

		if allColumnDefs {
			ids := []*Expr{}
			for _, e := range prop.This().Expressions() {
				ids = append(ids, tfmToIdentifier(e.Arg("this"), nil, true))
			}
			propThis := New(KTuple, "expressions", ids)
			schema := expression.This()
			for _, e := range prop.This().Expressions() {
				schema.Append("expressions", e)
			}
			prop.Set("this", propThis)
		}
	}

	return expression
}

// transformStructKvToAlias mirrors transforms.struct_kv_to_alias: converts struct arguments to
// aliases, e.g. STRUCT(1 AS y).
func transformStructKvToAlias(expression *Expr) *Expr {
	if expression.IsA(KStruct) {
		out := []*Expr{}
		for _, e := range expression.Expressions() {
			if e.IsA(KPropertyEQ) {
				out = append(out, tfmAlias(e.Expression(), e.Arg("this"), nil, nil, true))
			} else {
				out = append(out, e)
			}
		}
		expression.Set("expressions", out)
	}

	return expression
}

// transformEliminateJoinMarks mirrors transforms.eliminate_join_marks.
//
// https://docs.oracle.com/cd/B19306_01/server.102/b14200/queries006.htm#sthref3178
//
//  1. You cannot specify the (+) operator in a query block that also contains FROM clause join syntax.
//  2. The (+) operator can appear only in the WHERE clause or, in the context of left-correlation
//     (that is, when specifying the TABLE clause) in the FROM clause, and can be applied only to a
//     column of a table or view.
//
// The (+) operator does not produce an outer join if you specify one table in the outer query and
// the other table in an inner query. You cannot use the (+) operator to outer-join a table to itself,
// although self joins are valid. The (+) operator can be applied only to a column, not to an arbitrary
// expression. However, an arbitrary expression can contain one or more columns marked with the (+)
// operator. A WHERE condition containing the (+) operator cannot be combined with another condition
// using the OR logical operator. A WHERE condition cannot use the IN comparison condition to compare a
// column marked with the (+) operator with an expression. A WHERE condition cannot compare any column
// marked with the (+) operator with a subquery.
func transformEliminateJoinMarks(expression *Expr) *Expr {
	// we go in reverse to check the main query for left correlation
	scopes := TraverseScope(expression)
	for si := len(scopes) - 1; si >= 0; si-- {
		scope := scopes[si]
		query := scope.Expression

		where := query.ArgE("where")
		joins := query.ArgL("joins")

		if where == nil {
			continue
		}
		hasJoinMark := false
		for c := range where.FindAll(KColumn) {
			if c.ArgB("join_mark") {
				hasJoinMark = true
				break
			}
		}
		if !hasJoinMark {
			continue
		}

		// knockout: we do not support left correlation (see point 2)
		tfmAssert(!scope.IsCorrelatedSubquery(), "Correlated queries are not supported")

		// make sure we have AND of ORs to have clear join terms
		where = NormalizeCNF(where.This(), false, 128)
		tfmAssert(normalizedCNF(where, false), "Cannot normalize JOIN predicates")
		// dict of {name: list of join AND conditions}
		joinsOns := newOMap[[]*Expr]()
		conds := []*Expr{where}
		if where.IsA(KAnd) {
			conds = where.Flatten(true)
		}
		for _, cond := range conds {
			var joinCols []*Expr
			for col := range cond.FindAll(KColumn) {
				if col.ArgB("join_mark") {
					joinCols = append(joinCols, col)
				}
			}

			leftJoinTable := newStrSet()
			for _, col := range joinCols {
				leftJoinTable.Add(col.TableName())
			}
			if len(leftJoinTable) == 0 {
				continue
			}

			tfmAssert(!(len(leftJoinTable) > 1), "Cannot combine JOIN predicates from different tables")

			for _, col := range joinCols {
				col.Set("join_mark", false)
			}

			var table string
			for t := range leftJoinTable {
				table = t
			}
			predicates, _ := joinsOns.Get(table)
			joinsOns.Set(table, append(predicates, cond))
		}

		oldJoins := newOMap[*Expr]()
		for _, join := range joins {
			oldJoins.Set(join.AliasOrName(), join)
		}
		newJoins := newOMap[*Expr]()
		queryFrom := query.ArgE("from_")

		for _, table := range joinsOns.Keys() {
			predicates, _ := joinsOns.Get(table)
			source, ok := oldJoins.Get(table)
			if !ok {
				source = queryFrom
			}
			joinWhat := source.This().Copy()
			newJoins.Set(joinWhat.AliasOrName(), New(
				KJoin,
				"this", joinWhat,
				"on", tfmAnd(tfmAnys(predicates), true, true),
				"kind", "LEFT",
			))

			for _, p := range predicates {
				for p.Parent().IsA(KParen) {
					p.Parent().Replace(p)
				}

				parent := p.Parent()
				p.Pop()
				if parent.IsA(KBinary) {
					if left := parent.ArgE("this"); left == nil {
						parent.Replace(parent.Right())
					} else {
						parent.Replace(left)
					}
				} else if parent.IsA(KWhere) {
					parent.Pop()
				}
			}
		}

		if newJoins.Has(queryFrom.AliasOrName()) {
			// Python iterates `old_joins.keys() - new_joins.keys()` (a set); we take the first
			// remaining key in insertion order.
			var onlyOldJoins []string
			for _, k := range oldJoins.Keys() {
				if !newJoins.Has(k) {
					onlyOldJoins = append(onlyOldJoins, k)
				}
			}
			tfmAssert(len(onlyOldJoins) >= 1, "Cannot determine which table to use in the new FROM clause")

			newFromName := onlyOldJoins[0]
			oldJoin, _ := oldJoins.Get(newFromName)
			query.Set("from_", New(KFrom, "this", oldJoin.This()))
		}

		if newJoins.Len() > 0 {
			for _, n := range oldJoins.Keys() { // preserve any other joins
				j, _ := oldJoins.Get(n)
				if !newJoins.Has(n) && n != query.ArgE("from_").Name() {
					if j.KindText() == "" {
						j.Set("kind", "CROSS")
					}
					newJoins.Set(n, j)
				}
			}
			query.Set("joins", newJoins.Values())
		}
	}

	return expression
}

// transformAnyToExists mirrors transforms.any_to_exists: transforms the ANY operator to Spark's EXISTS.
//
// For example,
//   - Postgres: SELECT * FROM tbl WHERE 5 > ANY(tbl.col)
//   - Spark: SELECT * FROM tbl WHERE EXISTS(tbl.col, x -> x < 5)
//
// Both ANY and EXISTS accept queries but currently only array expressions are supported for this
// transformation.
func transformAnyToExists(expression *Expr) *Expr {
	if expression.IsA(KSelect) {
		for anyExpr := range expression.FindAll(KAny) {
			this := anyExpr.This()
			if this.IsA(KQuery) || anyExpr.Parent().IsA(KLike, KILike) {
				continue
			}

			binop := anyExpr.Parent()
			if binop.IsA(KBinary) {
				lambdaArg := ToIdentifier("x", nil)
				anyExpr.Replace(lambdaArg)
				lambdaExpr := New(KLambda, "this", binop.Copy(), "expressions", []*Expr{lambdaArg})
				binop.Replace(New(KExists, "this", this.Unnest(), "expression", lambdaExpr))
			}
		}
	}

	return expression
}

// transformEliminateWindowClause mirrors transforms.eliminate_window_clause: eliminates the `WINDOW`
// query clause by inling each named window.
func transformEliminateWindowClause(expression *Expr) *Expr {
	windowsArg := expression.Arg("windows")
	if expression.IsA(KSelect) && windowsArg != nil {
		windows, _ := windowsArg.([]*Expr)

		expression.Set("windows", nil)

		windowExpression := map[string]*Expr{}

		inlineInheritedWindow := func(window *Expr) {
			inheritedWindow := windowExpression[pyLower(window.Alias())]
			if inheritedWindow == nil {
				return
			}

			window.Set("alias", nil)
			for _, key := range []string{"partition_by", "order", "spec"} {
				switch arg := inheritedWindow.Arg(key).(type) {
				case *Expr:
					window.Set(key, arg.Copy())
				case []*Expr:
					// list.copy() is shallow: the elements are shared (and re-parented).
					window.Set(key, append([]*Expr{}, arg...))
				}
			}
		}

		for _, window := range windows {
			inlineInheritedWindow(window)
			windowExpression[pyLower(window.Name())] = window
		}

		for window := range FindAllInScope(expression, KWindow) {
			inlineInheritedWindow(window)
		}
	}

	return expression
}

// transformInheritStructFieldNames mirrors transforms.inherit_struct_field_names.
//
// Inherit field names from the first struct in an array.
//
// BigQuery supports implicitly inheriting names from the first STRUCT in an array:
//
//	ARRAY[
//	  STRUCT('Alice' AS name, 85 AS score),  -- defines names
//	  STRUCT('Bob', 92),                     -- inherits names
//	  STRUCT('Diana', 95)                    -- inherits names
//	]
//
// This transformation makes the field names explicit on all structs by adding
// PropertyEQ nodes, in order to facilitate transpilation to other dialects.
func transformInheritStructFieldNames(expression *Expr) *Expr {
	if !expression.IsA(KArray) || !expression.ArgB("struct_name_inheritance") {
		return expression
	}
	firstItem := seqGet(expression.Expressions(), 0)
	if !firstItem.IsA(KStruct) {
		return expression
	}
	for _, fld := range firstItem.Expressions() {
		if !fld.IsA(KPropertyEQ) {
			return expression
		}
	}

	fieldNames := []*Expr{}
	for _, fld := range firstItem.Expressions() {
		fieldNames = append(fieldNames, fld.This())
	}

	// Apply field names to subsequent structs that don't have them
	for _, st := range append([]*Expr{}, expression.Expressions()[1:]...) {
		if !st.IsA(KStruct) || len(st.Expressions()) != len(fieldNames) {
			continue
		}

		// Convert unnamed expressions to PropertyEQ with inherited names
		newExpressions := []*Expr{}
		for i, expr := range st.Expressions() {
			if !expr.IsA(KPropertyEQ) {
				// Create PropertyEQ: field_name := value, preserving the type from the inner expression
				propertyEQ := New(
					KPropertyEQ,
					"this", fieldNames[i].Copy(),
					"expression", expr,
				)
				tfmSetType(propertyEQ, expr.Type())
				newExpressions = append(newExpressions, propertyEQ)
			} else {
				newExpressions = append(newExpressions, expr)
			}
		}

		st.Set("expressions", newExpressions)
	}

	return expression
}

// ---------------------------------------------------------------------------------------------
// Helpers (prefixed tfm): ports of sqlglot builders used by the transforms. They mirror the
// Python copy semantics exactly, since several transforms depend on node identity.
// ---------------------------------------------------------------------------------------------

// tfmPyError mirrors a Python built-in exception (AssertionError, IndexError, TypeError) raised by a transform.
type tfmPyError struct{ Type, Msg string }

func (e *tfmPyError) Error() string { return e.Msg }

// tfmAssert mirrors a Python `assert cond, msg`.
func tfmAssert(cond bool, msg string) {
	if !cond {
		panic(&tfmPyError{Type: "AssertionError", Msg: msg})
	}
}

// tfmBaseDialect mirrors Dialect.get_or_raise(None).
func tfmBaseDialect() *Dialect {
	d, err := GetDialect("")
	if err != nil {
		panic(genPanic{err})
	}
	return d
}

// tfmIsNone reports whether v is Python None (nil or a nil *Expr).
func tfmIsNone(v any) bool {
	if v == nil {
		return true
	}
	if e, ok := v.(*Expr); ok && e == nil {
		return true
	}
	return false
}

// tfmAnys converts an expression list to a []any argument list.
func tfmAnys(xs []*Expr) []any {
	out := make([]any, len(xs))
	for i, x := range xs {
		out[i] = x
	}
	return out
}

// tfmOptBool mirrors a Python `bool | None` value (e.g. Identifier.args.get("quoted")).
func tfmOptBool(v any) *bool {
	if b, ok := v.(bool); ok {
		return &b
	}
	return nil
}

// tfmListIndex mirrors list.index(x) (identity or Expression.__eq__); -1 when absent.
func tfmListIndex(list []*Expr, x *Expr) int {
	for i, y := range list {
		if y == x || (y != nil && y.Equal(x)) {
			return i
		}
	}
	return -1
}

// tfmMaybeParse mirrors exp.maybe_parse(v, into=into, prefix=prefix, copy=copy) with the default
// dialect. into == KNone means None.
func tfmMaybeParse(v any, into Kind, prefix string, copy bool) *Expr {
	var sql string
	switch x := v.(type) {
	case *Expr:
		if x == nil {
			panic(genPanic{&ParseError{Msg: "SQL cannot be None"}})
		}
		if copy {
			return x.Copy()
		}
		return x
	case nil:
		panic(genPanic{&ParseError{Msg: "SQL cannot be None"}})
	case string:
		sql = x
	case int:
		sql = strconv.Itoa(x)
	default:
		panic(&ValueError{Msg: fmt.Sprintf("cannot parse %v", v)})
	}

	if prefix != "" {
		sql = prefix + " " + sql
	}

	d := tfmBaseDialect()
	var e *Expr
	var err error
	if into != KNone {
		e, err = d.ParseOneInto(into, sql, nil)
	} else {
		e, err = d.ParseOne(sql, nil)
	}
	if err != nil {
		panic(genPanic{err})
	}
	return e
}

// tfmToIdentifier mirrors exp.to_identifier(name, quoted=quoted, copy=copy).
func tfmToIdentifier(name any, quoted *bool, copy bool) *Expr {
	switch x := name.(type) {
	case nil:
		return nil
	case *Expr:
		if x == nil {
			return nil
		}
		if x.IsA(KIdentifier) {
			if copy {
				return x.Copy()
			}
			return x
		}
		panic(&ValueError{Msg: "Name needs to be a string or an Identifier, got: " + x.classRepr()})
	case string:
		return ToIdentifier(x, quoted)
	}
	panic(&ValueError{Msg: fmt.Sprintf("Name needs to be a string or an Identifier, got: %T", name)})
}

// tfmConvert mirrors exp.convert(value, copy=copy) for the value types used by the transforms.
func tfmConvert(v any, copy bool) *Expr {
	switch x := v.(type) {
	case nil:
		return Null()
	case *Expr:
		if x == nil {
			return Null()
		}
		if copy {
			return x.Copy()
		}
		return x
	case string:
		return LiteralString(x)
	case bool:
		return Boolean(x)
	case int:
		return LiteralInt(x)
	}
	panic(&ValueError{Msg: fmt.Sprintf("Cannot convert %v", v)})
}

// tfmWrap mirrors exp._wrap(expression, kind).
func tfmWrap(e *Expr, kind Kind) *Expr {
	if e.IsA(kind) {
		return New(KParen, "this", e)
	}
	return e
}

// tfmBinop mirrors Expr._binop(klass, other, reverse) (eq, neq, >, <=, -, ...).
func tfmBinop(kind Kind, self *Expr, other any, reverse bool) *Expr {
	this := self.Copy()
	o := tfmConvert(other, true)
	if !this.IsA(kind) && !o.IsA(kind) {
		this = tfmWrap(this, KBinary)
		o = tfmWrap(o, KBinary)
	}
	if reverse {
		return New(kind, "this", o, "expression", this)
	}
	return New(kind, "this", this, "expression", o)
}

// tfmCombine mirrors exp._combine(expressions, operator, copy=copy, wrap=wrap).
func tfmCombine(expressions []any, operator Kind, copy, wrap bool) *Expr {
	var conditions []*Expr
	for _, x := range expressions {
		if tfmIsNone(x) {
			continue
		}
		conditions = append(conditions, tfmMaybeParse(x, KCondition, "", copy))
	}

	if len(conditions) == 0 {
		panic(&ValueError{Msg: "not enough values to unpack (expected at least 1, got 0)"})
	}
	this, rest := conditions[0], conditions[1:]
	if len(rest) > 0 && wrap {
		this = tfmWrap(this, KConnector)
	}
	for _, x := range rest {
		if wrap {
			x = tfmWrap(x, KConnector)
		}
		this = New(operator, "this", this, "expression", x)
	}

	return this
}

// tfmAnd mirrors exp.and_(*expressions, copy=copy, wrap=wrap) (and Expr.and_).
func tfmAnd(expressions []any, copy, wrap bool) *Expr {
	return tfmCombine(expressions, KAnd, copy, wrap)
}

// tfmOr mirrors exp.or_(*expressions, copy=copy, wrap=wrap) (and Expr.or_).
func tfmOr(expressions []any, copy, wrap bool) *Expr {
	return tfmCombine(expressions, KOr, copy, wrap)
}

// tfmNot mirrors exp.not_(expression, copy=copy) (and Expr.not_).
func tfmNot(expression any, copy bool) *Expr {
	this := tfmMaybeParse(expression, KCondition, "", copy)
	return New(KNot, "this", tfmWrap(this, KConnector))
}

// tfmColumn mirrors exp.column(col, table, quoted=quoted, copy=copy) (db and catalog None).
// table == nil means None (an empty string still builds an Identifier, like Python).
func tfmColumn(col any, table any, quoted *bool, copy bool) *Expr {
	var this *Expr
	if c, ok := col.(*Expr); ok && c.IsA(KStar) {
		this = c
	} else {
		this = tfmToIdentifier(col, quoted, copy)
	}

	return New(
		KColumn,
		"this", this,
		"table", tfmToIdentifier(table, quoted, copy),
		"db", nil,
		"catalog", nil,
	)
}

// tfmAlias mirrors exp.alias_(expression, alias, table=table, quoted=quoted, copy=copy) (and Expr.as_).
// table is the Python `table` sequence; an empty/nil table means False.
func tfmAlias(expression any, alias any, table []any, quoted *bool, copy bool) *Expr {
	exp := tfmMaybeParse(expression, KNone, "", copy)
	aliasID := tfmToIdentifier(alias, quoted, true)

	if len(table) > 0 {
		tableAlias := New(KTableAlias, "this", aliasID)
		exp.Set("alias", tableAlias)

		for _, column := range table {
			tableAlias.Append("columns", tfmToIdentifier(column, quoted, true))
		}

		return exp
	}

	// We don't set the "alias" arg for Window expressions, because that would add an IDENTIFIER node in
	// the AST, representing a "named_window" [1] construct (eg. bigquery). What we want is an ALIAS node
	// for the complete Window expression.
	//
	// [1]: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls

	if exp.Kind().hasArgType("alias") && !exp.Is(KWindow) {
		exp.Set("alias", aliasID)
		return exp
	}
	return New(KAlias, "this", exp, "alias", aliasID)
}

// tfmListBuilder mirrors exp._apply_list_builder(*expressions, instance, arg, append, copy, into).
func tfmListBuilder(instance *Expr, arg string, expressions []any, into Kind, appendFlag, copy bool) *Expr {
	inst := instance
	if copy && instance != nil {
		inst = instance.Copy()
	}

	parsed := []*Expr{}
	for _, x := range expressions {
		if tfmIsNone(x) {
			continue
		}
		parsed = append(parsed, tfmMaybeParse(x, into, "", false))
	}

	if existing := inst.ArgL(arg); appendFlag && len(existing) > 0 {
		parsed = append(append([]*Expr{}, existing...), parsed...)
	}

	inst.Set(arg, parsed)
	return inst
}

// tfmSelect mirrors exp.select(*expressions, copy=copy).
func tfmSelect(expressions []any, copy bool) *Expr {
	return tfmSelectAppend(New(KSelect), expressions, true, copy)
}

// tfmSelectAppend mirrors Select.select(*expressions, append=append, copy=copy).
func tfmSelectAppend(instance *Expr, expressions []any, appendFlag, copy bool) *Expr {
	return tfmListBuilder(instance, "expressions", expressions, KExpr, appendFlag, copy)
}

// tfmSelectFrom mirrors Select.from_(expression, copy=copy).
func tfmSelectFrom(instance *Expr, expression any, copy bool) *Expr {
	if x, ok := expression.(*Expr); ok && x != nil && !x.IsA(KFrom) {
		expression = New(KFrom, "this", x)
	}
	inst := instance
	if copy && instance != nil {
		inst = instance.Copy()
	}
	e := tfmMaybeParse(expression, KFrom, "FROM", false)
	inst.Set("from_", e)
	return inst
}

// tfmWhere mirrors Query.where(*expressions, append=append, copy=copy).
func tfmWhere(instance *Expr, expressions []any, appendFlag, copy bool) *Expr {
	var filtered []any
	for _, x := range expressions {
		if xe, ok := x.(*Expr); ok && xe.IsA(KWhere) {
			x = xe.This()
		}
		if tfmIsNone(x) {
			continue
		}
		if s, ok := x.(string); ok && s == "" {
			continue
		}
		filtered = append(filtered, x)
	}
	if len(filtered) == 0 {
		return instance
	}

	inst := instance
	if copy && instance != nil {
		inst = instance.Copy()
	}

	if existing := inst.ArgE("where"); appendFlag && existing != nil {
		filtered = append([]any{existing.This()}, filtered...)
	}

	node := tfmAnd(filtered, copy, true)

	inst.Set("where", New(KWhere, "this", node))
	return inst
}

// tfmSubquery mirrors Query.subquery(alias, copy=copy).
func tfmSubquery(instance *Expr, alias any, copy bool) *Expr {
	inst := instance
	if copy && instance != nil {
		inst = instance.Copy()
	}

	var a *Expr
	switch x := alias.(type) {
	case *Expr:
		a = x
	case string:
		if x != "" {
			a = New(KTableAlias, "this", ToIdentifier(x, nil))
		}
	}

	return New(KSubquery, "this", inst, "alias", a)
}

// tfmUnion mirrors exp.union(left, right, distinct=distinct, copy=copy) (and Query.union).
func tfmUnion(left, right *Expr, distinct, copy bool) *Expr {
	l := tfmMaybeParse(left, KNone, "", copy)
	r := tfmMaybeParse(right, KNone, "", copy)
	return New(KUnion, "this", l, "expression", r, "distinct", distinct)
}

// tfmJoin mirrors Select.join(expression, join_type=joinType, copy=copy) for an expression argument
// (on/using/join_alias None, append=True).
func tfmJoin(instance *Expr, expression *Expr, joinType string, copy bool) *Expr {
	if expression == nil {
		panic(genPanic{&ParseError{Msg: "SQL cannot be None"}})
	}

	join := expression
	if !expression.IsA(KJoin) {
		join = New(KJoin, "this", expression)
	}

	if join.This().IsA(KSelect) {
		join.This().Replace(tfmSubquery(join.This(), nil, true))
	}

	if joinType != "" {
		newJoin := tfmMaybeParse("FROM _ "+joinType+" JOIN _", KNone, "", false).Find(KJoin)
		method := newJoin.MethodText()
		side := newJoin.SideText()
		kind := newJoin.KindText()

		if method != "" {
			join.Set("method", method)
		}
		if side != "" {
			join.Set("side", side)
		}
		if kind != "" {
			join.Set("kind", kind)
		}
	}

	return tfmListBuilder(instance, "joins", []any{join}, KNone, true, copy)
}

// tfmArray mirrors exp.array(*expressions, copy=copy).
func tfmArray(expressions []any, copy bool) *Expr {
	parsed := []*Expr{}
	for _, x := range expressions {
		parsed = append(parsed, tfmMaybeParse(x, KNone, "", copy))
	}
	return New(KArray, "expressions", parsed)
}

// tfmFunc mirrors exp.func(name, *args) with the default dialect (copy=True).
func tfmFunc(name string, args ...any) *Expr {
	d := tfmBaseDialect()

	converted := make([]*Expr, 0, len(args))
	for _, a := range args {
		converted = append(converted, tfmMaybeParse(a, KNone, "", true))
	}

	var function *Expr
	if constructor := d.P.FUNCTIONS[pyUpper(name)]; constructor != nil {
		function, converted = callFuncBuilder(constructor, converted, d)
	} else {
		function = New(KAnonymous, "this", name, "expressions", converted)
	}

	for _, msg := range function.ErrorMessages(converted) {
		panic(&ValueError{Msg: msg})
	}

	return function
}

// tfmDataTypeBuild mirrors exp.DataType.build(dtype) for a string dtype (DataType.from_str, udt=False).
func tfmDataTypeBuild(dtype string) *Expr {
	if pyUpper(dtype) == "UNKNOWN" {
		return NewDataType(DT_UNKNOWN)
	}
	lvl := ErrorLevelIgnore
	e, err := tfmBaseDialect().ParseOneInto(KDataType, dtype, &ParseOptions{ErrorLevel: &lvl})
	if err != nil {
		panic(genPanic{err})
	}
	return e
}

// tfmCast mirrors exp.cast(expression, to, copy=copy) with the default dialect, for a string type.
func tfmCast(expression any, to string, copy bool) *Expr {
	expr := tfmMaybeParse(expression, KNone, "", copy)
	dataType := tfmDataTypeBuild(to)

	// dont re-cast if the expression is already a cast to the correct type
	if expr.IsA(KCast) {
		typeMapping := tfmBaseDialect().G.TYPE_MAPPING

		existingCastType := expr.ArgE("to").DTypeOf()
		newCastType := dataType.DTypeOf()
		existingMapped, ok := typeMapping[existingCastType]
		if !ok {
			existingMapped = dtypeValues[existingCastType]
		}
		newMapped, ok := typeMapping[newCastType]
		if !ok {
			newMapped = dtypeValues[newCastType]
		}
		typesAreEquivalent := existingMapped == newMapped

		if tfmIsType(expr, dataType) || typesAreEquivalent {
			return expr
		}
	}

	result := New(KCast, "this", expr, "to", dataType)
	result.SetType(dataType)

	return result
}

// tfmTypeOf returns the DataType that Expr.is_type compares against: the node itself for data
// types, Cast.to for casts (Cast.is_type override), otherwise the annotated type.
func tfmTypeOf(e *Expr) *Expr {
	if e == nil {
		return nil
	}
	switch {
	case e.kind.isDataType():
		return e
	case e.kind.isCast():
		return e.ArgE("to")
	}
	return e.RawType()
}

// tfmIsType mirrors Expr.is_type(*dtypes) for DataType expression arguments.
func tfmIsType(e *Expr, dtypes ...*Expr) bool {
	t := tfmTypeOf(e)
	if t == nil {
		return false
	}
	selfThis := t.Arg("this")
	for _, other := range dtypes {
		var matches bool
		if len(other.Expressions()) > 0 || selfThis == any(DT_USERDEFINED) || other.Arg("this") == any(DT_USERDEFINED) {
			matches = t.Equal(other)
		} else {
			matches = selfThis == other.Arg("this")
		}
		if matches {
			return true
		}
	}
	return false
}

// tfmIsTypeD mirrors Expr.is_type(*dtypes) for DType arguments.
func tfmIsTypeD(e *Expr, dtypes ...DType) bool {
	t := tfmTypeOf(e)
	if t == nil {
		return false
	}
	selfThis := t.Arg("this")
	for _, d := range dtypes {
		if selfThis == any(DT_USERDEFINED) || d == DT_USERDEFINED {
			if t.Equal(NewDataType(d)) {
				return true
			}
		} else if selfThis == any(d) {
			return true
		}
	}
	return false
}

// tfmSetType mirrors the Expr.type setter for a DataType (or None) value: values whose class is
// not exactly DataType go through DataType.build, which copies DataType subclasses.
func tfmSetType(e *Expr, dtype *Expr) {
	if dtype != nil && !dtype.Is(KDataType) && dtype.IsA(KDataType) {
		dtype = dtype.Copy()
	}
	e.SetType(dtype)
}
