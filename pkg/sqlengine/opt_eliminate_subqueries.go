package sqlengine

// Port of sqlglot/optimizer/eliminate_subqueries.py.

// EliminateSubqueries mirrors optimizer.eliminate_subqueries.eliminate_subqueries: rewrites
// derived tables as CTES, deduplicating if possible.
//
// Example: "SELECT a FROM (SELECT * FROM x) AS y" -> "WITH y AS (SELECT * FROM x) SELECT a FROM y AS y".
func EliminateSubqueries(expression *Expr) *Expr {
	if expression.IsA(KSubquery) {
		// It's possible to have subqueries at the root, e.g. (SELECT * FROM x) LIMIT 1
		EliminateSubqueries(expression.This())
		return expression
	}

	root := BuildScope(expression)

	if root == nil {
		return expression
	}

	// Map of alias->Scope|Table
	// These are all aliases that are already used in the expression.
	// We don't want to create new CTEs that conflict with these names.
	// (The values are always truthy in Python, so only the keys matter.)
	taken := newStrSet()

	// All CTE aliases in the root scope are taken
	for _, scope := range root.CTEScopes {
		if parent := scope.Expression.Parent(); parent != nil {
			taken.Add(parent.Alias())
		}
	}

	// All table names are taken
	for _, scope := range root.Traverse() {
		for _, source := range scope.Sources.Values() {
			if !source.IsScope() {
				taken.Add(source.Table.Name())
			}
		}
	}

	// Map of Expr->alias
	// Existing CTES in the root expression. We'll use this for deduplication.
	existingCTEs := &esExistingCTEs{}

	with := root.Expression.ArgE("with_")
	var recursive any = false
	if with != nil {
		recursive = with.Arg("recursive")
		for _, cte := range with.Expressions() {
			existingCTEs.set(cte.This(), cte.Alias())
		}
	}
	newCTEs := []*Expr{}

	// We're adding more CTEs, but we want to maintain the DAG order.
	// Derived tables within an existing CTE need to come before the existing CTE.
	for _, cteScope := range root.CTEScopes {
		// Append all the new CTEs from this existing CTE
		for _, scope := range cteScope.Traverse() {
			if scope == cteScope {
				// Don't try to eliminate this CTE itself
				continue
			}
			if newCTE := esEliminate(scope, existingCTEs, taken); newCTE != nil {
				newCTEs = append(newCTEs, newCTE)
			}
		}

		// Append the existing CTE itself
		if cteParent := cteScope.Expression.Parent(); cteParent != nil {
			newCTEs = append(newCTEs, cteParent)
		}
	}

	// Now append the rest
	var rest []*Scope
	rest = append(rest, root.UnionScopes...)
	rest = append(rest, root.SubqueryScopes...)
	rest = append(rest, root.TableScopes...)
	for _, scope := range rest {
		for _, childScope := range scope.Traverse() {
			if newCTE := esEliminate(childScope, existingCTEs, taken); newCTE != nil {
				newCTEs = append(newCTEs, newCTE)
			}
		}
	}

	if len(newCTEs) > 0 {
		query := expression
		if expression.IsA(KDDL) {
			query = expression.Expression()
		}
		query.Set("with_", New(KWith, "expressions", newCTEs, "recursive", recursive))
	}

	return expression
}

// esEliminate mirrors eliminate_subqueries._eliminate.
func esEliminate(scope *Scope, existingCTEs *esExistingCTEs, taken StrSet) *Expr {
	if scope.IsDerivedTable() {
		return esEliminateDerivedTable(scope, existingCTEs, taken)
	}

	if scope.IsCTE() {
		return esEliminateCTE(scope, existingCTEs, taken)
	}

	return nil
}

// esEliminateDerivedTable mirrors eliminate_subqueries._eliminate_derived_table.
func esEliminateDerivedTable(scope *Scope, existingCTEs *esExistingCTEs, taken StrSet) *Expr {
	// This makes sure that we don't:
	// - drop the "pivot" arg from a pivoted subquery
	// - eliminate a lateral correlated subquery
	parentScope := scope.Parent
	if parentScope == nil || len(parentScope.Pivots()) > 0 || parentScope.Expression.IsA(KLateral) {
		return nil
	}

	exprParent := scope.Expression.Parent()
	if !exprParent.IsA(KSubquery) {
		return nil
	}

	// Get rid of redundant exp.Subquery expressions, i.e. those that are just used as wrappers
	toReplace := exprParent.Unwrap()
	name, cte := esNewCTE(scope, existingCTEs, taken)
	aliasName := toReplace.Alias()
	if aliasName == "" {
		aliasName = name
	}
	table := AliasExpr(TableExpr(name, nil, nil, nil, nil), aliasName, nil, true)
	table.Set("joins", toReplace.Arg("joins"))

	toReplace.Replace(table)

	return cte
}

// esEliminateCTE mirrors eliminate_subqueries._eliminate_cte.
func esEliminateCTE(scope *Scope, existingCTEs *esExistingCTEs, taken StrSet) *Expr {
	parent := scope.Expression.Parent()
	if parent == nil {
		return nil
	}
	name, cte := esNewCTE(scope, existingCTEs, taken)

	with := parent.Parent()
	parent.Pop()
	if with != nil && len(with.Expressions()) == 0 {
		with.Pop()
	}

	// Rename references to this CTE
	if scope.Parent == nil {
		return cte
	}
	for _, childScope := range scope.Parent.Traverse() {
		for _, selected := range childScope.SelectedSources().Values() {
			if selected.Source.Scope == scope {
				table := selected.Node
				newTable := AliasExpr(TableExpr(name, nil, nil, nil, nil), table.AliasOrName(), nil, false)
				table.Replace(newTable)
			}
		}
	}

	return cte
}

// esNewCTE mirrors eliminate_subqueries._new_cte.
//
// Returns (name, cte) where `name` is a new name for this CTE in the root scope and `cte` is a
// new CTE instance. If this CTE duplicates an existing CTE, `cte` will be nil.
func esNewCTE(scope *Scope, existingCTEs *esExistingCTEs, taken StrSet) (string, *Expr) {
	duplicateCTEAlias, _ := existingCTEs.get(scope.Expression)
	parent := scope.Expression.Parent()
	name := ""
	if parent != nil {
		name = parent.Alias()
	}

	if name == "" {
		name = findNewNameOMap(taken.Has, "cte")
	}

	if duplicateCTEAlias != "" {
		name = duplicateCTEAlias
	} else if taken.Has(name) {
		name = findNewNameOMap(taken.Has, name)
	}

	taken.Add(name)

	var cte *Expr
	if duplicateCTEAlias == "" {
		existingCTEs.set(scope.Expression, name)
		cte = New(
			KCTE,
			"this", scope.Expression,
			"alias", New(KTableAlias, "this", ToIdentifier(name, nil)),
		)
	}
	return name, cte
}

// esExistingCTEs mirrors a Python dict keyed by expressions (dict[exp.Expr, str]). Python stores
// the key's hash at insertion time and compares candidates with `is` or `==` (same class and same
// current hash), so keys mutated after insertion become unreachable, exactly as emulated here.
type esExistingCTEs struct {
	entries []esExistingCTE
}

type esExistingCTE struct {
	key   *Expr
	hash  uint64
	value string
}

func (m *esExistingCTEs) find(key *Expr) int {
	h := key.Hash()
	for i, e := range m.entries {
		if e.hash == h && (e.key == key || e.key.Equal(key)) {
			return i
		}
	}
	return -1
}

func (m *esExistingCTEs) get(key *Expr) (string, bool) {
	if i := m.find(key); i >= 0 {
		return m.entries[i].value, true
	}
	return "", false
}

func (m *esExistingCTEs) set(key *Expr, value string) {
	if i := m.find(key); i >= 0 {
		m.entries[i].value = value
		return
	}
	m.entries = append(m.entries, esExistingCTE{key: key, hash: key.Hash(), value: value})
}
