package sqlengine

// Port of sqlglot/optimizer/pushdown_predicates.py.

// PushdownPredicates mirrors optimizer.pushdown_predicates.pushdown_predicates: rewrites the AST
// to pushdown predicates in FROMS and JOINS. d == nil means the default dialect.
//
// Example: "SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y WHERE y.a = 1"
// -> "SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x WHERE x.a = 1) AS y WHERE TRUE"
func PushdownPredicates(expression *Expr, d *Dialect) *Expr {
	root := BuildScope(expression)

	if d == nil {
		d = MustDialect("")
	}
	unnestRequiresCrossJoin := d.Is("athena") || d.Is("presto")

	if root != nil {
		scopeRefCount := root.RefCount()

		scopes := root.Traverse()
		for i := len(scopes) - 1; i >= 0; i-- {
			scope := scopes[i]
			sel := scope.Expression
			where := sel.ArgE("where")
			joins := sel.ArgL("joins")
			if where != nil {
				selectedSources := scope.SelectedSources()
				joinIndex := map[string]int{}
				for j, join := range joins {
					joinIndex[join.AliasOrName()] = j
				}

				// a right join can only push down to itself and not the source FROM table
				// presto, trino and athena don't support inner joins where the RHS is an UNNEST expression
				pushdownAllowed := true
				for _, k := range selectedSources.Keys() {
					v, _ := selectedSources.Get(k)
					node := v.Node
					parent := node.FindAncestor(KJoin, KFrom)
					if parent.IsA(KJoin) {
						if parent.SideText() == "RIGHT" {
							only := newOMap[SelectedSource]()
							only.Set(k, v)
							selectedSources = only
							break
						}
						if node.IsA(KUnnest) && unnestRequiresCrossJoin {
							pushdownAllowed = false
							break
						}
					}
				}

				if pushdownAllowed {
					pushdownPredicatesPushdown(where.This(), selectedSources, scopeRefCount, d, joinIndex)
				}
			}

			// joins should only pushdown into itself, not to other joins
			// so we limit the selected sources to only itself
			for _, join := range joins {
				name := join.AliasOrName()
				if v, ok := scope.SelectedSources().Get(name); ok {
					only := newOMap[SelectedSource]()
					only.Set(name, v)
					pushdownPredicatesPushdown(join.ArgE("on"), only, scopeRefCount, d, nil)
				}
			}
		}
	}

	return expression
}

// pushdownPredicatesPushdown mirrors pushdown_predicates.pushdown. joinIndex == nil (or empty)
// mirrors a falsy join_index.
func pushdownPredicatesPushdown(condition *Expr, sources *omap[SelectedSource], scopeRefCount map[any]int, d *Dialect, joinIndex map[string]int) {
	if condition == nil {
		return
	}

	condition = condition.Replace(Simplify(condition, SimplifyOptions{Dialect: d}))
	cnfLike := normalizedCNF(condition, false) || !normalizedCNF(condition, true)

	var predicates []*Expr
	kind := KOr
	if cnfLike {
		kind = KAnd
	}
	if condition.IsA(kind) {
		predicates = condition.Flatten(true)
	} else {
		predicates = []*Expr{condition}
	}

	if cnfLike {
		pushdownPredicatesPushdownCNF(predicates, sources, scopeRefCount, joinIndex)
	} else {
		pushdownPredicatesPushdownDNF(predicates, sources, scopeRefCount, joinIndex)
	}
}

// pushdownPredicatesJoinIndexOf mirrors `join_index[name]` (KeyError when missing).
func pushdownPredicatesJoinIndexOf(joinIndex map[string]int, name string) int {
	i, ok := joinIndex[name]
	if !ok {
		panic(&ValueError{Msg: pyRepr(name)})
	}
	return i
}

// pushdownPredicatesJoinIndexGet mirrors `join_index.get(table, -1)`.
func pushdownPredicatesJoinIndexGet(joinIndex map[string]int, name string) int {
	if i, ok := joinIndex[name]; ok {
		return i
	}
	return -1
}

// pushdownPredicatesPushdownCNF mirrors pushdown_predicates.pushdown_cnf: if the predicates are
// in CNF like form, we can simply replace each block in the parent.
func pushdownPredicatesPushdownCNF(predicates []*Expr, sources *omap[SelectedSource], scopeRefCount map[any]int, joinIndex map[string]int) {
	for _, predicate := range predicates {
		nodes := pushdownPredicatesNodesForPredicate(predicate, sources, scopeRefCount)
		for _, name := range nodes.Keys() {
			node, _ := nodes.Get(name)
			if node.IsA(KJoin) {
				name := node.AliasOrName()
				predicateTables := ColumnTableNames(predicate, name)

				if len(joinIndex) > 0 {
					// Don't push the predicate if it references tables that appear in later joins
					thisIndex := pushdownPredicatesJoinIndexOf(joinIndex, name)
					all := true
					for table := range predicateTables {
						if !(pushdownPredicatesJoinIndexGet(joinIndex, table) < thisIndex) {
							all = false
							break
						}
					}
					if all {
						predicate.Replace(Boolean(true))
						node.JoinOn([]*Expr{predicate}, true, false)
						break
					}
				}
			}
			if node.IsA(KSelect) {
				predicate.Replace(Boolean(true))
				innerPredicate := pushdownPredicatesReplaceAliases(node, predicate)
				if FindInScope(innerPredicate, KAggFunc) != nil {
					node.SelectHaving([]*Expr{innerPredicate}, true, false)
				} else {
					node.QueryWhere([]*Expr{innerPredicate}, true, false)
				}
			}
		}
	}
}

// pushdownPredicatesPushdownDNF mirrors pushdown_predicates.pushdown_dnf: if the predicates are
// in DNF form, we can only push down conditions that are in all blocks. Additionally, we can't
// remove predicates from their original form.
func pushdownPredicatesPushdownDNF(predicates []*Expr, sources *omap[SelectedSource], scopeRefCount map[any]int, joinIndex map[string]int) {
	// find all the tables that can be pushdown too
	// these are tables that are referenced in all blocks of a DNF
	// (a.x AND b.x) OR (a.y AND c.y)
	// only table a can be push down
	pushdownTables := newStrSet()

	for _, a := range predicates {
		aTables := ColumnTableNames(a, "")

		for _, b := range predicates {
			bTables := ColumnTableNames(b, "")
			for t := range aTables {
				if !bTables.Has(t) {
					delete(aTables, t)
				}
			}
		}

		for t := range aTables {
			pushdownTables.Add(t)
		}
	}

	conditions := map[string]*Expr{}

	// pushdown all predicates to their respective nodes
	var nodes *omap[*Expr]
	for _, table := range pushdownTables.Sorted() {
		for _, predicate := range predicates {
			nodes = pushdownPredicatesNodesForPredicate(predicate, sources, scopeRefCount)

			if !nodes.Has(table) {
				continue
			}

			if existing, ok := conditions[table]; ok {
				conditions[table] = OrExpr(existing, predicate)
			} else {
				conditions[table] = predicate
			}
		}

		// `nodes` is the value left over from the last iteration of the loop above
		for _, name := range nodes.Keys() {
			node, _ := nodes.Get(name)
			predicate, ok := conditions[name]
			if !ok {
				continue
			}

			if node.IsA(KJoin) {
				if len(joinIndex) > 0 {
					thisIndex := pushdownPredicatesJoinIndexOf(joinIndex, name)
					predicateTables := ColumnTableNames(predicate, name)
					all := true
					for t := range predicateTables {
						if !(pushdownPredicatesJoinIndexGet(joinIndex, t) < thisIndex) {
							all = false
							break
						}
					}
					if !all {
						continue
					}
				}
				node.JoinOn([]*Expr{predicate}, true, false)
			} else if node.IsA(KSelect) {
				innerPredicate := pushdownPredicatesReplaceAliases(node, predicate)
				if FindInScope(innerPredicate, KAggFunc) != nil {
					node.SelectHaving([]*Expr{innerPredicate}, true, false)
				} else {
					node.QueryWhere([]*Expr{innerPredicate}, true, false)
				}
			}
		}
	}
}

// pushdownPredicatesNodesForPredicate mirrors pushdown_predicates.nodes_for_predicate.
func pushdownPredicatesNodesForPredicate(predicate *Expr, sources *omap[SelectedSource], scopeRefCount map[any]int) *omap[*Expr] {
	nodes := newOMap[*Expr]()
	tables := ColumnTableNames(predicate, "")
	whereCondition := predicate.FindAncestor(KJoin, KWhere).IsA(KWhere)

	for _, table := range tables.Sorted() {
		var node *Expr
		var source Source
		hasSource := false
		if v, ok := sources.Get(table); ok {
			node, source, hasSource = v.Node, v.Source, true
		}

		// if the predicate is in a where statement we can try to push it down
		// we want to find the root join or from statement
		if node != nil && whereCondition {
			node = node.FindAncestor(KJoin, KFrom)
		}

		// a node can reference a CTE which should be pushed down
		if node.IsA(KFrom) && hasSource && source.IsScope() {
			parent := source.Scope.Parent
			if parent == nil {
				panic(&ValueError{Msg: "Source node has no parent"})
			}
			with := parent.Expression.ArgE("with_")
			if with != nil && with.ArgB("recursive") {
				return newOMap[*Expr]()
			}
			node = source.Scope.Expression
		}

		if node.IsA(KJoin) {
			if side := node.SideText(); side != "" && side != "RIGHT" {
				return newOMap[*Expr]()
			}
			nodes.Set(table, node)
		} else if node.IsA(KSelect) && len(tables) == 1 {
			// We can't push down window expressions
			hasWindowExpression := false
			for _, sel := range node.Selects() {
				if sel.Find(KWindow) != nil {
					hasWindowExpression = true
					break
				}
			}
			// we can't push down predicates to select statements if they are referenced in
			// multiple places.
			var sourceID any
			if hasSource {
				sourceID = source.id()
			}
			if !node.ArgB("group") &&
				scopeRefCount[sourceID] < 2 &&
				!hasWindowExpression &&
				// LIMIT/OFFSET and QUALIFY select rows before the outer predicate runs
				!node.ArgB("limit") &&
				!node.ArgB("offset") &&
				!node.ArgB("qualify") {
				nodes.Set(table, node)
			}
		}
	}
	return nodes
}

// pushdownPredicatesReplaceAliases mirrors pushdown_predicates.replace_aliases.
func pushdownPredicatesReplaceAliases(source *Expr, predicate *Expr) *Expr {
	aliases := map[string]*Expr{}

	for _, sel := range source.Selects() {
		if sel.IsA(KAlias) {
			aliases[sel.Alias()] = sel.This()
		} else {
			aliases[sel.Name()] = sel
		}
	}

	replaceAlias := func(column *Expr) *Expr {
		if column.IsA(KColumn) {
			if a, ok := aliases[column.Name()]; ok {
				return a.Copy()
			}
		}
		return column
	}

	return predicate.Transform(replaceAlias, true)
}
