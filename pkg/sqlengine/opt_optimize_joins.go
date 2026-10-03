package sqlengine

// Port of sqlglot/optimizer/optimize_joins.py.

// optimizeJoinsJoinAttrs mirrors optimize_joins.JOIN_ATTRS.
var optimizeJoinsJoinAttrs = []string{"on", "side", "kind", "using", "method"}

// OptimizeJoins mirrors optimizer.optimize_joins.optimize_joins: removes cross joins if possible
// and reorders joins based on predicate dependencies.
//
// Example: "SELECT * FROM x CROSS JOIN y JOIN z ON x.a = z.a AND y.a = z.a"
// -> "SELECT * FROM x JOIN z ON x.a = z.a AND TRUE JOIN y ON y.a = z.a"
func OptimizeJoins(expression *Expr) *Expr {
	for sel := range expression.FindAll(KSelect) {
		joins := sel.ArgL("joins")

		if !optimizeJoinsIsReorderable(joins) {
			continue
		}

		references := map[string][]*Expr{}
		type crossJoin struct {
			name string
			join *Expr
		}
		var crossJoins []crossJoin

		for _, join := range joins {
			tables := optimizeJoinsOtherTableNames(join)

			if len(tables) > 0 {
				for table := range tables {
					references[table] = append(append([]*Expr{}, references[table]...), join)
				}
			} else {
				crossJoins = append(crossJoins, crossJoin{join.AliasOrName(), join})
			}
		}

		for _, cj := range crossJoins {
			name, join := cj.name, cj.join
			for _, dep := range references[name] {
				on := dep.ArgE("on")

				if on.IsA(KConnector) {
					if len(optimizeJoinsOtherTableNames(dep)) < 2 {
						continue
					}

					operator := on.Kind()
					for predicate := range optxFlattenSeq(on, true) {
						if ColumnTableNames(predicate, "").Has(name) {
							predicate.Replace(Boolean(true))
							predicate = combineConditions([]*Expr{join.ArgE("on"), predicate}, operator, false, true)
							join.JoinOn([]*Expr{predicate}, false, false)
						}
					}
				}
			}
		}
	}

	expression = ReorderJoins(expression)
	expression = optimizeJoinsNormalize(expression)
	return expression
}

// ReorderJoins mirrors optimize_joins.reorder_joins: reorders joins by topological sort order
// based on predicate references.
func ReorderJoins(expression *Expr) *Expr {
	for from := range expression.FindAll(KFrom) {
		parent := from.Parent()
		if parent == nil {
			panic(&OptimizeError{Msg: "FROM clause without parent expression"})
		}
		joins := parent.ArgL("joins")

		if !optimizeJoinsIsReorderable(joins) {
			continue
		}

		var names []string
		joinsByName := map[string]*Expr{}
		for _, join := range joins {
			name := join.AliasOrName()
			if _, ok := joinsByName[name]; !ok {
				names = append(names, name)
			}
			joinsByName[name] = join
		}
		dag := map[string]StrSet{}
		for _, name := range names {
			dag[name] = optimizeJoinsOtherTableNames(joinsByName[name])
		}

		fromName := from.AliasOrName()
		reordered := []*Expr{}
		for _, name := range optxTsort(names, dag) {
			if join, ok := joinsByName[name]; ok && name != fromName {
				reordered = append(reordered, join)
			}
		}
		parent.Set("joins", reordered)
	}
	return expression
}

// optimizeJoinsNormalize mirrors optimize_joins.normalize: removes INNER and OUTER from joins as
// they are optional.
func optimizeJoinsNormalize(expression *Expr) *Expr {
	for join := range expression.FindAll(KJoin) {
		hasAttr := false
		for _, k := range optimizeJoinsJoinAttrs {
			if join.ArgB(k) {
				hasAttr = true
				break
			}
		}
		if !hasAttr {
			join.Set("kind", "CROSS")
		}

		if join.KindText() == "CROSS" {
			join.Set("on", nil)
		} else {
			if k := join.KindText(); k == "INNER" || k == "OUTER" {
				join.Set("kind", nil)
			}

			if !join.ArgB("on") && !join.ArgB("using") {
				join.Set("on", Boolean(true))
			}
		}
	}
	return expression
}

// optimizeJoinsOtherTableNames mirrors optimize_joins.other_table_names.
func optimizeJoinsOtherTableNames(join *Expr) StrSet {
	if on := join.ArgE("on"); on != nil {
		return ColumnTableNames(on, join.AliasOrName())
	}
	return newStrSet()
}

// optimizeJoinsIsReorderable mirrors optimize_joins._is_reorderable: joins with a side (LEFT,
// RIGHT, FULL) cannot be reordered easily.
func optimizeJoinsIsReorderable(joins []*Expr) bool {
	for _, join := range joins {
		if join.SideText() != "" {
			return false
		}
	}
	return true
}

// optxTsort mirrors sqlglot.helper.tsort for a string DAG whose keys are given in insertion
// order. Panics with a ValueError("Cycle error") when the graph has a cycle.
func optxTsort(keys []string, deps map[string]StrSet) []string {
	result := []string{}

	dag := make(map[string]StrSet, len(keys))
	for _, k := range keys {
		dag[k] = deps[k]
	}
	for _, k := range keys {
		for dep := range dag[k] {
			if _, ok := dag[dep]; !ok {
				dag[dep] = newStrSet()
			}
		}
	}

	for len(dag) > 0 {
		current := newStrSet()
		for node, d := range dag {
			if len(d) == 0 {
				current.Add(node)
			}
		}

		if len(current) == 0 {
			panic(&ValueError{Msg: "Cycle error"})
		}

		for node := range current {
			delete(dag, node)
		}

		for _, d := range dag {
			for node := range current {
				delete(d, node)
			}
		}

		result = append(result, current.Sorted()...)
	}

	return result
}
