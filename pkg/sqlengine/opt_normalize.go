package sqlengine

// Port of sqlglot/optimizer/normalize_identifiers.py.

// NormalizeIdentifiers mirrors normalize_identifiers(expression, dialect) (store_original_column_identifiers=False).
//
// Identifiers are converted to lower or upper case depending on the dialect, respecting
// case-sensitivity. A node annotated with `/* sqlglot.meta case_sensitive */` (and its subtree)
// is left untouched. A nil dialect means the default sqlglot dialect.
func NormalizeIdentifiers(e *Expr, d *Dialect) *Expr {
	return NormalizeIdentifiersFull(e, d, false)
}

// NormalizeIdentifiersStr mirrors normalize_identifiers(<str>, dialect): the string is parsed into an
// Identifier (exp.parse_identifier) and normalized.
func NormalizeIdentifiersStr(name string, d *Dialect) *Expr {
	if d == nil {
		d = prototype("")
	}
	return NormalizeIdentifiersFull(parseIdentifier(name, d), d, false)
}

// NormalizeIdentifiersFull mirrors normalize_identifiers(expression, dialect, store_original_column_identifiers).
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
