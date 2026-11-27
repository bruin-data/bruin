package sqlengine

import (
	"fmt"
	"strconv"
)

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

// transformPreprocess chains transforms and renders the result with generator, else with the
// render method of the result's kind or its Generator.TRANSFORMS entry. An UnsupportedError raised
// by a transformation is reported as unsupported.
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

				// Ensures we don't enter an infinite loop. This can happen when the original
				// expression has the same type as the final expression and there's no render method
				// available for it, because then it'd re-enter this transform.
				panic(&ValueError{Msg: fmt.Sprintf("Expr type %s requires a _sql method in order to be transformed.", expression.Kind().Name())})
			}

			return transformsHandler(g, expression)
		}

		panic(&ValueError{Msg: fmt.Sprintf("Unsupported expression type %s.", expression.Kind().Name())})
	}
}

// transformUnnestGenerateDateArrayUsingRecursiveCte replaces UNNEST(GENERATE_DATE_ARRAY(...)) in
// FROM and JOIN with a recursive CTE that generates the dates.
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

// transformEliminateDistinctOn converts SELECT DISTINCT ON statements to a subquery with a window
// function.
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

// transformEliminateQualify rewrites a SELECT with QUALIFY into a filtered subquery. Window
// functions and columns that QUALIFY references become aliased projections of the subquery, since
// some dialects don't allow window functions in WHERE and the outer filter can only refer to
// selected columns. A reference to a newly aliased projection is replaced by its expression, to
// avoid invalid column references.
func transformEliminateQualify(expression *Expr) *Expr {
	if expression.IsA(KSelect) && expression.ArgB("qualify") {
		if parent := expression.Parent(); parent.IsA(KInsert) && parent.Parent().IsA(KMultitableInserts) {
			// A FROM-first multi-table INSERT branch shares its parent's FROM, so it can't be wrapped
			panic(&UnsupportedError{Msg: "QUALIFY is not supported in a FROM-first multi-table INSERT branch"})
		}

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

// transformRemovePrecisionParameterizedTypes drops type parameters such as precision, which some
// dialects only allow in DDL.
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

// transformUnqualifyUnnest removes references to unnest table aliases, added by the optimizer's
// QualifyColumns step.
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

// transformUnnestToExplode is transformUnnestToExplodeFull with ARRAYS_ZIP for multiple arrays.
func transformUnnestToExplode(expression *Expr) *Expr {
	return transformUnnestToExplodeFull(expression, true)
}

// transformUnnestToExplodeFull converts cross join unnest into lateral view explode. Multiple
// arrays are zipped with ARRAYS_ZIP when unnestUsingArraysZip is set and unsupported otherwise.
func transformUnnestToExplodeFull(expression *Expr, unnestUsingArraysZip bool) *Expr {
	unnestZipExprs := func(u *Expr, unnestExprs []*Expr, hasMultiExpr bool) []*Expr {
		if hasMultiExpr {
			if !unnestUsingArraysZip {
				panic(&UnsupportedError{Msg: "Cannot transpile UNNEST with multiple input arrays"})
			}

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
				// A non-empty alias column list is updated in place as well.
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
				// unnestZipExprs changes the number of unnest.expressions, so record it here
				hasMultiExpr := len(exprs) > 1
				exprs = unnestZipExprs(unnest, exprs, hasMultiExpr)

				// Remove the first element that is (or equals) join, in place.
				i := tfmListIndex(joins, join)
				if i < 0 {
					panic(&ValueError{Msg: "list.remove(x): x not in list"})
				}
				joins = append(joins[:i:i], joins[i+1:]...)
				expression.SetArgRaw("joins", joins)

				aliasCols := alias.ArgL("columns")

				// Handle UNNEST to LATERAL VIEW EXPLODE: an error is raised when there are 0 or > 2
				// aliases. Spark LATERAL VIEW EXPLODE requires a single alias for array/struct and
				// two for a Map type column, unlike unnest in Trino/Presto, which takes any number.
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

// transformExplodeProjectionToUnnest returns a transform that converts explode/posexplode
// projections into unnests; indexOffset is the first array index of the target dialect.
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
				explode := FindInScope(sel, KExplode)

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
						explode = FindInScope(alias, KExplode)
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

func tfmIsPercentile(e *Expr) bool { return e.IsA(KPercentileCont, KPercentileDisc) }

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

// transformAddRecursiveCteColumnNames uses projection output names in recursive CTE definitions to
// define the CTEs' columns.
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

// transformEpochCastToTs replaces 'epoch' in casts by the equivalent date literal.
func transformEpochCastToTs(expression *Expr) *Expr {
	if expression.IsA(KCast, KTryCast) &&
		pyLower(expression.Name()) == "epoch" &&
		DataType_TEMPORAL_TYPES.Has(expression.ArgE("to").DTypeOf()) {
		expression.This().Replace(LiteralString("1970-01-01 00:00:00"))
	}

	return expression
}

// transformEliminateSemiAndAntiJoins converts SEMI and ANTI joins into equivalent forms that use
// EXISTS instead.
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

// transformEliminateFullOuterJoin converts a query with a FULL OUTER join to a union of identical
// queries that use LEFT/RIGHT OUTER joins instead. This transformation currently only works for
// queries that have a single FULL OUTER join.
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

			// CTEs and pagination apply to the whole union
			union := tfmUnion(expression, expressionCopy, false, false)
			for _, arg := range []string{"with_", "order", "limit", "offset"} {
				if value := expression.Arg(arg); truthy(value) {
					expression.Set(arg, nil)
					expressionCopy.Set(arg, nil)
					union.Set(arg, value)
				}
			}
			return union
		}
	}

	return expression
}

// transformMoveCtesToTopLevel moves nested CTEs to the top-level WITH, for dialects (e.g. Hive,
// T-SQL, Spark before 3) that reject SELECT * FROM (WITH t(c) AS (SELECT 1) SELECT * FROM t) AS
// subq.
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

// tfmEnsureBoolTypes holds DT_UNKNOWN and the numeric types: values of these types used as
// conditions get an explicit <> 0.
var tfmEnsureBoolTypes = append([]DType{DT_UNKNOWN}, DataType_NUMERIC_TYPES.Items()...)

// transformEnsureBools converts numeric values and boolean casts used in conditions into explicit
// comparisons.
func transformEnsureBools(expression *Expr) *Expr {
	ensureBool := func(node *Expr) {
		if node.IsNumber() ||
			(node.IsA(KCast) && tfmIsTypeD(node, DT_BOOLEAN)) ||
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

// tfmCanonicalizeEnsureBools applies replaceFunc to the condition operands of expression: both
// sides of AND/OR, the operand of NOT, IF conditions (outside simple CASE) and WHERE/HAVING
// predicates.
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

// transformUnqualifyPivotFields unqualifies the columns in a (UN)PIVOT's FOR clause and IN-list,
// which some dialects only accept as simple names, leaving the aggregate qualified.
func transformUnqualifyPivotFields(expression *Expr) *Expr {
	if expression.IsA(KPivot) {
		fields := []*Expr{}
		for _, field := range expression.ArgL("fields") {
			fields = append(fields, transformUnqualifyColumns(field))
		}
		expression.Set("fields", fields)
	}

	return expression
}

// transformUnqualifyColumns removes the table, db and catalog qualifiers of every column.
func transformUnqualifyColumns(expression *Expr) *Expr {
	for column := range expression.FindAll(KColumn) {
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

func transformRemoveUniqueConstraints(expression *Expr) *Expr {
	tfmAssert(expression.IsA(KCreate), "")
	for constraint := range expression.FindAll(KUniqueColumnConstraint) {
		// An unnamed table constraint is the UNIQUE itself, not its parent schema
		if parent := constraint.Parent(); parent.IsA(KColumnConstraint, KConstraint) {
			parent.Pop()
		} else {
			constraint.Pop()
		}
	}

	return expression
}

func transformCtasWithTmpTablesToCreateTmpView(expression *Expr) *Expr {
	return transformCtasWithTmpTablesToCreateTmpViewFull(expression, nil)
}

// transformCtasWithTmpTablesToCreateTmpViewFull turns CREATE TEMPORARY TABLE ... AS query into
// CREATE TEMPORARY VIEW; a temporary table without a query goes through tmpStorageProvider.
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

// transformMoveSchemaColumnsToPartitionedBy moves the columns named by a PARTITIONED BY array out
// of the CREATE's schema into a PARTITIONED BY schema. In Hive, PARTITIONED BY acts as an
// extension of the table's schema.
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

// transformMovePartitionedByToSchemaColumns moves PARTITIONED BY column definitions into the table
// schema, leaving only their names in PARTITIONED BY, as the DATASOURCE CREATE TABLE format used
// for Spark 3 expects.
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

// transformAnyToExists transforms the ANY operator to Spark's EXISTS.
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

// transformEliminateWindowClause eliminates the `WINDOW` query clause by inlining each named
// window.
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
					// A shallow copy: the elements are shared (and re-parented).
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

// transformInheritStructFieldNames inherits field names from the first struct in an array.
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

	for _, st := range append([]*Expr{}, expression.Expressions()[1:]...) {
		if !st.IsA(KStruct) || len(st.Expressions()) != len(fieldNames) {
			continue
		}

		newExpressions := []*Expr{}
		for i, expr := range st.Expressions() {
			if !expr.IsA(KPropertyEQ) {
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

// The tfm helpers below copy or reuse their arguments deliberately: several transforms depend on
// node identity.

// tfmPyError is an internal error raised (as a panic) by a transform. Type names the error kind
// in its historical wording (AssertionError, IndexError, TypeError).
type tfmPyError struct{ Type, Msg string }

func (e *tfmPyError) Error() string { return e.Msg }

func tfmAssert(cond bool, msg string) {
	if !cond {
		panic(&tfmPyError{Type: "AssertionError", Msg: msg})
	}
}

func tfmBaseDialect() *Dialect {
	d, err := GetDialect("")
	if err != nil {
		panic(genPanic{err})
	}
	return d
}

func tfmIsNone(v any) bool {
	if v == nil {
		return true
	}
	if e, ok := v.(*Expr); ok && e == nil {
		return true
	}
	return false
}

func tfmAnys(xs []*Expr) []any {
	out := make([]any, len(xs))
	for i, x := range xs {
		out[i] = x
	}
	return out
}

func tfmOptBool(v any) *bool {
	if b, ok := v.(bool); ok {
		return &b
	}
	return nil
}

func tfmListIndex(list []*Expr, x *Expr) int {
	for i, y := range list {
		if y == x || (y != nil && y.Equal(x)) {
			return i
		}
	}
	return -1
}

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
		panic(&ValueError{Msg: "Name needs to be a string or an Identifier, got: " + x.Kind().Name()})
	case string:
		return ToIdentifier(x, quoted)
	}
	panic(&ValueError{Msg: fmt.Sprintf("Name needs to be a string or an Identifier, got: %T", name)})
}

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

func tfmWrap(e *Expr, kind Kind) *Expr {
	if e.IsA(kind) {
		return New(KParen, "this", e)
	}
	return e
}

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

func tfmAnd(expressions []any, copy, wrap bool) *Expr {
	return tfmCombine(expressions, KAnd, copy, wrap)
}

func tfmOr(expressions []any, copy, wrap bool) *Expr {
	return tfmCombine(expressions, KOr, copy, wrap)
}

func tfmNot(expression any, copy bool) *Expr {
	this := tfmMaybeParse(expression, KCondition, "", copy)
	return New(KNot, "this", tfmWrap(this, KConnector))
}

// tfmColumn builds a column reference from col (a name, Identifier or Star) and an optional table.
// table == nil means no table; an empty string still builds an Identifier.
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

// tfmAlias aliases expression. With table column names it sets a TableAlias with those columns;
// otherwise it sets the alias arg when the kind has one (except for windows) or wraps the
// expression in an Alias. An empty or nil table means a plain alias.
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

func tfmSelect(expressions []any, copy bool) *Expr {
	return tfmSelectAppend(New(KSelect), expressions, true, copy)
}

func tfmSelectAppend(instance *Expr, expressions []any, appendFlag, copy bool) *Expr {
	return tfmListBuilder(instance, "expressions", expressions, KExpr, appendFlag, copy)
}

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

func tfmUnion(left, right *Expr, distinct, copy bool) *Expr {
	l := tfmMaybeParse(left, KNone, "", copy)
	r := tfmMaybeParse(right, KNone, "", copy)
	return New(KUnion, "this", l, "expression", r, "distinct", distinct)
}

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

func tfmArray(expressions []any, copy bool) *Expr {
	parsed := []*Expr{}
	for _, x := range expressions {
		parsed = append(parsed, tfmMaybeParse(x, KNone, "", copy))
	}
	return New(KArray, "expressions", parsed)
}

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

// tfmCast casts expression to the type named to. A cast whose target type already is, or renders
// like, to in the base dialect is returned unchanged.
func tfmCast(expression any, to string, copy bool) *Expr {
	expr := tfmMaybeParse(expression, KNone, "", copy)
	dataType := tfmDataTypeBuild(to)

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

// tfmTypeOf returns the DataType that tfmIsType compares against: the node itself for data types,
// the target type for casts, otherwise the annotated type.
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

// tfmIsType reports whether e has one of the types dtypes. Parameterized and user-defined types
// compare structurally, others by their DType.
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

func tfmSetType(e *Expr, dtype *Expr) {
	if dtype != nil && !dtype.Is(KDataType) && dtype.IsA(KDataType) {
		dtype = dtype.Copy()
	}
	e.SetType(dtype)
}
