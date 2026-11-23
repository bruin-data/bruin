package sqlengine

import "fmt"

// pushdownProjectionsSelection is the set of column names an outer query selects from a scope;
// `all` means the outer query selects ALL columns. Scopes may share one selection through the
// same pointer, so updating it affects all of them.
type pushdownProjectionsSelection struct {
	all   bool
	names StrSet
}

func newPushdownProjectionsSelection() *pushdownProjectionsSelection {
	return &pushdownProjectionsSelection{names: newStrSet()}
}

func pushdownProjectionsSelectAll() *pushdownProjectionsSelection {
	return &pushdownProjectionsSelection{all: true, names: newStrSet()}
}

func (s *pushdownProjectionsSelection) update(other *pushdownProjectionsSelection) {
	if other.all {
		s.all = true
	}
	for n := range other.names {
		s.names.Add(n)
	}
}

// pushdownProjectionsSetReturningFunctions are the set-returning (table) function kinds. They
// multiply the rows of the whole query, so a projection containing one is never pruned, even
// when otherwise unreferenced. Posexplode and the *Outer variants are subkinds of Explode, so
// matching Explode covers them too.
var pushdownProjectionsSetReturningFunctions = []Kind{KExplode, KInline, KUnnest}

func pushdownProjectionsIsSelfReferencingCTE(scope *Scope) bool {
	cte := scope.Expression.Parent()
	if !cte.IsA(KCTE) || !cte.Parent().IsA(KWith) || !cte.Parent().ArgB("recursive") {
		return false
	}
	for table := range scope.Expression.FindAll(KTable) {
		if table.DbName() == "" && table.Name() == cte.Alias() {
			return true
		}
	}
	return false
}

func pushdownProjectionsDefaultSelection(isAgg bool) *Expr {
	var e *Expr
	if isAgg {
		e = New(KMax, "this", LiteralInt(1))
	} else {
		e = LiteralNumber("1")
	}
	// The result is always an Alias: neither Max nor Literal has an "alias" arg.
	return AliasExpr(e, "_", nil, true)
}

// PushdownProjections removes unused column projections, e.g.
// "SELECT y.a AS a FROM (SELECT x.a AS a, x.b AS b FROM x) AS y"
// -> "SELECT y.a AS a FROM (SELECT x.a AS a FROM x) AS y".
func PushdownProjections(expression *Expr, schema *MappingSchema, removeUnusedSelections bool, d *Dialect) *Expr {
	if schema == nil {
		schema = NewMappingSchema(nil, nil, d, true, nil)
	}
	sourceColumnAliasCount := map[any]int{}
	referencedColumns := map[*Scope]*pushdownProjectionsSelection{}

	// We build the scope tree (which is traversed in DFS postorder), then iterate
	// over the result in reverse order. This should ensure that the set of selected
	// columns for a particular scope are completely build by the time we get to it.
	scopes := TraverseScope(expression)
	for i := len(scopes) - 1; i >= 0; i-- {
		scope := scopes[i]
		parentSelections, ok := referencedColumns[scope]
		if !ok {
			parentSelections = pushdownProjectionsSelectAll()
		}
		aliasCount := sourceColumnAliasCount[scope]

		// We can't remove columns SELECT DISTINCT nor UNION DISTINCT.
		if scope.Expression.ArgB("distinct") {
			parentSelections = pushdownProjectionsSelectAll()
		}

		// A recursive CTE's body reads the CTE's own output, so its projections
		// can't be pruned based only on what the enclosing query selects.
		if pushdownProjectionsIsSelfReferencingCTE(scope) {
			parentSelections = pushdownProjectionsSelectAll()
		}

		if scope.Expression.IsA(KSetOperation) {
			setOp := scope.Expression
			if setOp.KindText() != "" || setOp.SideText() != "" {
				// Do not optimize this set operation if it's using the BigQuery specific
				// kind / side syntax (e.g INNER UNION ALL BY NAME) which changes the semantics of the operation
				continue
			}

			if len(scope.UnionScopes) != 2 {
				panic(&ValueError{Msg: fmt.Sprintf("not enough values to unpack (expected 2, got %d)", len(scope.UnionScopes))})
			}
			left, right := scope.UnionScopes[0], scope.UnionScopes[1]
			le := left.Expression
			re := right.Expression

			if !(le.IsA(KSelectable) && re.IsA(KSelectable)) {
				continue
			}

			if len(le.Selects()) != len(re.Selects()) {
				dialect := d
				if dialect == nil {
					dialect = MustDialect("")
				}
				scopeSQL, err := dialect.Generate(scope.Expression, nil)
				if err != nil {
					panic(err)
				}
				panic(&OptimizeError{Msg: fmt.Sprintf("Invalid set operation due to column mismatch: %s.", scopeSQL)})
			}

			referencedColumns[left] = parentSelections

			if re.IsStar() {
				referencedColumns[right] = parentSelections
			} else if !le.IsStar() {
				if scope.Expression.ArgB("by_name") {
					referencedColumns[right] = referencedColumns[left]
				} else {
					rightSelections := newPushdownProjectionsSelection()
					reSelects := re.Selects()
					for i, sel := range le.Selects() {
						if parentSelections.all || parentSelections.names.Has(sel.AliasOrName()) {
							rightSelections.names.Add(reSelects[i].AliasOrName())
						}
					}
					referencedColumns[right] = rightSelections
				}
			}
		}

		if scope.Expression.IsA(KSelect) {
			if removeUnusedSelections {
				pushdownProjectionsRemoveUnusedSelections(scope, parentSelections, schema, aliasCount)
			}

			if scope.ScansAllSubscopeColumns() {
				continue
			}

			selects := map[string]StrSet{}
			for _, col := range scope.Columns() {
				tableName := col.TableName()
				colName := col.Name()
				if selects[tableName] == nil {
					selects[tableName] = newStrSet()
				}
				selects[tableName].Add(colName)
			}

			selectedSources := scope.SelectedSources()
			for _, name := range selectedSources.Keys() {
				v, _ := selectedSources.Get(name)
				node, source := v.Node, v.Source
				if source.IsScope() && source.Scope.Expression.IsA(KSelectable) {
					sel := seqGet(source.Scope.Expression.Selects(), 0)

					var columns *pushdownProjectionsSelection
					if len(scope.Pivots()) > 0 || sel.IsA(KQueryTransform) {
						columns = pushdownProjectionsSelectAll()
					} else {
						columns = newPushdownProjectionsSelection()
						columns.names.Add(selects[name].Sorted()...)
					}

					rc, ok := referencedColumns[source.Scope]
					if !ok {
						rc = newPushdownProjectionsSelection()
						referencedColumns[source.Scope] = rc
					}
					rc.update(columns)
				}

				if columnAliases := node.AliasColumnNames(); len(columnAliases) > 0 {
					sourceColumnAliasCount[source.id()] = len(columnAliases)
				}
			}
		}
	}

	return expression
}

// pushdownProjectionsRemoveUnusedSelections drops the projections of scope that no outer query,
// ORDER BY or positional column alias uses, expanding a removed star into the needed columns.
func pushdownProjectionsRemoveUnusedSelections(scope *Scope, parentSelections *pushdownProjectionsSelection, schema *MappingSchema, aliasCount int) {
	order := scope.Expression.ArgE("order")

	orderRefs := newStrSet()
	if order != nil {
		// Assume columns without a qualified table are references to output columns
		for c := range order.FindAll(KColumn) {
			if c.TableName() == "" {
				orderRefs.Add(c.Name())
			}
		}
	}

	newSelections := []*Expr{}
	removed := false
	star := false
	isAgg := false

	selectAll := parentSelections.all

	for _, selection := range scope.Expression.Selects() {
		name := selection.AliasOrName()

		if selectAll || parentSelections.names.Has(name) || orderRefs.Has(name) || aliasCount > 0 {
			newSelections = append(newSelections, selection)
			aliasCount--
		} else if selection.Find(pushdownProjectionsSetReturningFunctions...) != nil {
			// A set-returning function changes the cardinality of every output column, so this projection
			// is kept. It is not a positional alias slot, so aliasCount is left untouched.
			newSelections = append(newSelections, selection)
		} else {
			if selection.IsStar() {
				star = true
			}
			removed = true
		}

		if !isAgg && selection.Find(KAggFunc) != nil {
			isAgg = true
		}
	}

	if star {
		resolver := NewResolver(scope, schema, true)
		names := newStrSet()
		for _, s := range newSelections {
			names.Add(s.AliasOrName())
		}

		// parentSelections.all can't be set here (nothing is removed in that case)
		for _, name := range parentSelections.names.Sorted() {
			if !names.Has(name) {
				newSelections = append(
					newSelections,
					AliasExpr(ColumnExpr(name, resolver.GetTableByName(name), nil, nil, nil, nil, true), name, nil, false),
				)
			}
		}
	}

	if len(newSelections) == 0 {
		newSelections = append(newSelections, pushdownProjectionsDefaultSelection(isAgg))
	}

	scope.Expression.QuerySelect(newSelections, false, false)

	if removed {
		scope.ClearCache()
	}
}
