package sqlengine

// EliminateCTEs removes unused CTEs from an expression in place and returns it.
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

					if withNode != nil && len(withNode.Expressions()) <= 0 {
						withNode.Pop()
					}

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
