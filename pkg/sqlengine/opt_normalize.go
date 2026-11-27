package sqlengine

// NormalizeIdentifiers folds every identifier in e to the dialect's case, in place. A node
// annotated with a case_sensitive meta comment (one starting with metaCommentPrefix) is left
// untouched, with its subtree.
func NormalizeIdentifiers(e *Expr, d *Dialect) *Expr {
	return NormalizeIdentifiersFull(e, d, false)
}

func NormalizeIdentifiersStr(name string, d *Dialect) *Expr {
	if d == nil {
		d = prototype("")
	}
	return NormalizeIdentifiersFull(parseIdentifier(name, d), d, false)
}

// NormalizeIdentifiersFull is NormalizeIdentifiers; with storeOriginalColumnIdentifiers each
// column also records its pre-normalization dotted path in the "dot_parts" meta entry.
func NormalizeIdentifiersFull(e *Expr, d *Dialect, storeOriginalColumnIdentifiers bool) *Expr {
	if d == nil {
		d = prototype("")
	}

	prune := func(n *Expr) bool { return truthy(n.MetaGet("case_sensitive")) }
	for node := range e.Walk(true, prune) {
		if !truthy(node.MetaGet("case_sensitive")) {
			if storeOriginalColumnIdentifiers && node.IsA(KColumn) {
				// TODO: This does not handle non-column cases, e.g PARSE_JSON(...).key
				parent := node
				for parent != nil && parent.Parent().IsA(KDot) {
					parent = parent.Parent()
				}

				parts := parent.Parts()
				names := make([]string, len(parts))
				for i, p := range parts {
					names[i] = p.Name()
				}
				node.Meta()["dot_parts"] = names
			}

			if node.IsA(KIdentifier) {
				d.NormalizeIdentifier(node)
			}
		}
	}

	return e
}
