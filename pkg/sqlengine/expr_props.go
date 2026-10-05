package sqlengine

// Ports of per-class properties/methods of sqlglot expressions.

// TableText mirrors Column.table / Update.table text.
func (e *Expr) TableName() string { return e.Text("table") }

// DbName mirrors Column.db / Table.db.
func (e *Expr) DbName() string { return e.Text("db") }

// CatalogName mirrors Column.catalog / Table.catalog.
func (e *Expr) CatalogName() string { return e.Text("catalog") }

// Parts mirrors Column.parts, Table.parts and Dot.parts.
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

// DotBuild mirrors Dot.build.
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

// ToDot mirrors Column.to_dot(include_dots).
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

// Selects mirrors the `selects` property.
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

// NamedSelects mirrors the `named_selects` property.
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
		x := e
		for x.IsA(KSetOperation) {
			x = x.This().Unnest()
		}
		return namedSelectsOf(x)
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

// namedSelectsOf mirrors expressions.query._named_selects.
func namedSelectsOf(e *Expr) []string {
	out := []string{}
	for _, s := range e.Selects() {
		out = append(out, s.OutputName())
	}
	return out
}

// CTEs mirrors Query.ctes / DDL.ctes.
func (e *Expr) CTEs() []*Expr {
	if w := e.ArgE("with_"); w != nil {
		return w.Expressions()
	}
	return []*Expr{}
}

// IsWrapper mirrors Subquery.is_wrapper.
func (e *Expr) IsWrapper() bool {
	for _, a := range e.args {
		if a.key != "this" && a.val != nil {
			return false
		}
	}
	return true
}

// UnnestSubquery mirrors Subquery.unnest (first non-subquery).
func (e *Expr) UnnestSubquery() *Expr {
	x := e
	for x.IsA(KSubquery) {
		x = x.This()
	}
	return x
}

// Unwrap mirrors Subquery.unwrap.
func (e *Expr) Unwrap() *Expr {
	x := e
	for x.SameParent() && x.IsWrapper() {
		x = x.parent
	}
	return x
}

// JoinKind mirrors Join.kind / SetOperation.kind (uppercased text).
func (e *Expr) KindText() string { return pyUpper(e.Text("kind")) }

// SideText mirrors Join.side / SetOperation.side.
func (e *Expr) SideText() string { return pyUpper(e.Text("side")) }

// MethodText mirrors Join.method.
func (e *Expr) MethodText() string { return pyUpper(e.Text("method")) }

// IsSemiOrAntiJoin mirrors Join.is_semi_or_anti_join.
func (e *Expr) IsSemiOrAntiJoin() bool {
	k := e.KindText()
	return k == "SEMI" || k == "ANTI"
}

// Left mirrors Binary.left / SetOperation.left.
func (e *Expr) Left() *Expr { return e.This() }

// Right mirrors Binary.right / SetOperation.right.
func (e *Expr) Right() *Expr { return e.Expression() }

// TableAliasColumns mirrors TableAlias.columns.
func (e *Expr) TableAliasColumns() []*Expr { return e.ArgL("columns") }
