package sqlengine

// Port of sqlglot/optimizer/eliminate_ctes.py.

// EliminateCTEs mirrors optimizer.eliminate_ctes.eliminate_ctes: removes unused CTEs from an
// expression.
//
// Example: "WITH y AS (SELECT a FROM x) SELECT a FROM z" -> "SELECT a FROM z".
func EliminateCTEs(expression *Expr) *Expr {
	root := BuildScope(expression)

	if root != nil {
		refCount := root.RefCount()

		// Traverse the scope tree in reverse so we can remove chains of unused CTEs
		scopes := root.Traverse()
		for i := len(scopes) - 1; i >= 0; i-- {
			scope := scopes[i]
			if scope.IsCTE() {
				count := refCount[scope]
				if count <= 0 {
					cteNode := scope.Expression.Parent()
					if cteNode == nil {
						continue
					}
					withNode := cteNode.Parent()
					cteNode.Pop()

					// Pop the entire WITH clause if this is the last CTE
					if withNode != nil && len(withNode.Expressions()) <= 0 {
						withNode.Pop()
					}

					// Decrement the ref count for all sources this CTE selects from
					for _, selected := range scope.SelectedSources().Values() {
						if selected.Source.IsScope() {
							refCount[selected.Source.Scope]--
						}
					}
				}
			}
		}
	}

	return expression
}
