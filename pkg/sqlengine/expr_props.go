package sqlengine

// TableName returns the "table" argument's text: a column's qualifier or an UPDATE's target.
func (e *Expr) TableName() string { return e.Text("table") }

// DbName returns the text of the "db" argument of a column or table.
func (e *Expr) DbName() string { return e.Text("db") }

// CatalogName returns the text of the "catalog" argument of a column or table.
func (e *Expr) CatalogName() string { return e.Text("catalog") }

// Parts returns the dotted name parts, outermost qualifier first: catalog, db, table and
// column for a column; catalog, db and name for a table (dotted names flattened); for a Dot,
// the leftmost operand's parts followed by the remaining dotted parts.
func (e *Expr) Parts() []*Expr {
	switch propOwner_parts[e.kind] {
	case KColumn:
		var out []*Expr
		for _, k := range []string{"catalog", "db", "table", "this"} {
			if v := e.ArgE(k); v != nil {
				out = append(out, v)
			}
		}
		return out
	case KTable:
		var out []*Expr
		for _, k := range []string{"catalog", "db", "this"} {
			v := e.ArgE(k)
			if v == nil {
				continue
			}
			if v.IsA(KDot) {
				out = append(out, v.Flatten(true)...)
			} else {
				out = append(out, v)
			}
		}
		return out
	case KDot:
		flat := e.Flatten(true)
		if len(flat) == 0 {
			return nil
		}
		this, rest := flat[0], flat[1:]
		parts := make([]*Expr, 0, len(rest)+4)
		for i := len(rest) - 1; i >= 0; i-- {
			parts = append(parts, rest[i])
		}
		for _, k := range []string{"this", "table", "db", "catalog"} {
			if v := this.ArgE(k); v != nil {
				parts = append(parts, v)
			}
		}
		for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
			parts[i], parts[j] = parts[j], parts[i]
		}
		return parts
	}
	return nil
}

// DotBuild left-folds exprs into a chain of Dot nodes. It panics with a ValueError when
// fewer than two expressions are given.
func DotBuild(exprs []*Expr) *Expr {
	if len(exprs) < 2 {
		panic(&ValueError{Msg: "Dot requires >= 2 expressions."})
	}
	acc := exprs[0]
	for _, x := range exprs[1:] {
		acc = New(KDot, "this", acc, "expression", x)
	}
	return acc
}

// ToDot converts a column into a Dot chain of copies of its parts, or returns its only part
// when it has one. With includeDots, the right-hand sides of enclosing Dot parents are
// appended.
func (e *Expr) ToDot(includeDots bool) *Expr {
	parts := e.Parts()
	parent := e.parent
	if includeDots {
		for parent.IsA(KDot) {
			parts = append(parts, parent.Expression())
			parent = parent.parent
		}
	}
	if len(parts) > 1 {
		copied := make([]*Expr, len(parts))
		for i, p := range parts {
			copied[i] = p.Copy()
		}
		return DotBuild(copied)
	}
	return parts[0]
}

// Selects returns the projections of a query-like expression: a SELECT's expressions, the
// leftmost SELECT's projections for a set operation, the query's projections for a DDL or
// derived table, and the alias columns (plus the offset column for UNNEST) for a table
// function. Tables have none; other kinds return nil.
func (e *Expr) Selects() []*Expr {
	if e == nil {
		return nil
	}
	switch propOwner_selects[e.kind] {
	case KSelect:
		return e.Expressions()
	case KSetOperation:
		x := e
		for x.IsA(KSetOperation) {
			x = x.This().Unnest()
		}
		return x.Selects()
	case KTable:
		return []*Expr{}
	case KDDL:
		ex := e.Expression()
		if ex.IsA(KQuery) {
			return ex.Selects()
		}
		return []*Expr{}
	case KUnnest:
		cols := e.udtfSelects()
		if off := e.Arg("offset"); truthy(off) {
			if b, ok := off.(bool); ok && b {
				cols = append(append([]*Expr{}, cols...), ToIdentifier("offset", nil))
			} else if oe, ok := off.(*Expr); ok {
				cols = append(append([]*Expr{}, cols...), oe)
			}
		}
		return cols
	case KDerivedTable:
		if this := e.This(); this.IsA(KQuery) {
			return this.Selects()
		}
		return []*Expr{}
	case KLateral:
		cols := e.udtfSelects()
		// UNNEST ... WITH ORDINALITY stores the ordinality column's name in Unnest.offset
		if off := e.This().ArgE("offset"); off.IsA(KIdentifier) {
			cols = append(append([]*Expr{}, cols...), off)
		}
		return cols
	case KUDTF:
		return e.udtfSelects()
	}
	return nil
}

func (e *Expr) udtfSelects() []*Expr {
	if a := e.ArgE("alias"); a != nil {
		return a.ArgL("columns")
	}
	return []*Expr{}
}

// NamedSelects returns the output names of the projections. For a SELECT, unaliased
// projections without a name are skipped, except multi-alias projections, which contribute
// each alias.
func (e *Expr) NamedSelects() []string {
	if e == nil {
		return nil
	}
	switch propOwner_named_selects[e.kind] {
	case KSelect:
		var out []string
		for _, x := range e.Expressions() {
			if x.AliasOrName() != "" {
				out = append(out, x.OutputName())
			} else if x.IsA(KAliases) {
				for _, a := range x.Expressions() {
					out = append(out, a.Name())
				}
			}
		}
		return out
	case KSetOperation:
		// The names come from the leftmost query, except that a BY NAME operation outputs the
		// columns of both of its operands, without duplicates
		var names []string
		stack := []*Expr{e}
		byName := false
		for len(stack) > 0 {
			x := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if !x.IsA(KSetOperation) {
				names = append(names, x.NamedSelects()...)
				continue
			}

			reachedQuery := true
			for x.IsA(KSetOperation) {
				if x.ArgB("by_name") {
					byName = true
					stack = append(stack, x.Expression().Unnest(), x.This().Unnest())
					reachedQuery = false
					break
				}
				x = x.This().Unnest()
			}
			if reachedQuery {
				names = append(names, namedSelectsOf(x)...)
			}
		}
		if byName {
			return dedupeStrings(names)
		}
		if names == nil {
			return []string{}
		}
		return names
	case KTable:
		return []string{}
	case KDDL:
		ex := e.Expression()
		if ex.IsA(KQuery) {
			return ex.NamedSelects()
		}
		return []string{}
	}
	return namedSelectsOf(e)
}

func namedSelectsOf(e *Expr) []string {
	out := []string{}
	for _, s := range e.Selects() {
		out = append(out, s.OutputName())
	}
	return out
}

func (e *Expr) CTEs() []*Expr {
	if w := e.ArgE("with_"); w != nil {
		return w.Expressions()
	}
	return []*Expr{}
}

// IsWrapper reports whether every argument other than "this" is unset, i.e. the node only
// wraps its child (no alias, limit, ordering and so on).
func (e *Expr) IsWrapper() bool {
	for _, a := range e.args {
		if a.key != "this" && a.val != nil {
			return false
		}
	}
	return true
}

// UnnestSubquery returns the first non-subquery node found by descending through nested
// subqueries.
func (e *Expr) UnnestSubquery() *Expr {
	x := e
	for x.IsA(KSubquery) {
		x = x.This()
	}
	return x
}

// Unwrap walks up through parents of the same kind while the node is a pure wrapper and
// returns the outermost one.
func (e *Expr) Unwrap() *Expr {
	x := e
	for x.SameParent() && x.IsWrapper() {
		x = x.parent
	}
	return x
}

func (e *Expr) KindText() string { return pyUpper(e.Text("kind")) }

func (e *Expr) SideText() string { return pyUpper(e.Text("side")) }

func (e *Expr) MethodText() string { return pyUpper(e.Text("method")) }

func (e *Expr) IsSemiOrAntiJoin() bool {
	k := e.KindText()
	return k == "SEMI" || k == "ANTI"
}

func (e *Expr) isLimitAll() bool {
	limit := e.Expression()
	return e.IsA(KLimit) && limit.IsA(KVar) && pyUpper(limit.Name()) == "ALL"
}

func (e *Expr) Left() *Expr { return e.This() }

func (e *Expr) Right() *Expr { return e.Expression() }

func (e *Expr) TableAliasColumns() []*Expr { return e.ArgL("columns") }
