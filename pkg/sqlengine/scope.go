package sqlengine

import (
	"fmt"
	"iter"
)

// Port of sqlglot/optimizer/scope.py.

// ScopeType mirrors sqlglot.optimizer.scope.ScopeType.
type ScopeType int

const (
	ScopeRoot ScopeType = iota + 1
	ScopeSubquery
	ScopeDerivedTable
	ScopeCTE
	ScopeUnion
	ScopeUDTF
)

// Source is a scope source: exactly one of Table (an exp.Table) or Scope is set.
type Source struct {
	Table *Expr
	Scope *Scope
}

// IsScope reports whether the source is a Scope (isinstance(source, Scope)).
func (s Source) IsScope() bool { return s.Scope != nil }

func (s Source) id() any {
	if s.Scope != nil {
		return s.Scope
	}
	return s.Table
}

// SelectedSource is a value of Scope.selected_sources: (node, source).
type SelectedSource struct {
	Node   *Expr
	Source Source
}

type scopeRef struct {
	Name string
	Node *Expr
}

// Scope mirrors sqlglot.optimizer.scope.Scope.
type Scope struct {
	Expression         *Expr
	Sources            *omap[Source]
	LateralSources     *omap[Source]
	CTESources         *omap[Source]
	OuterColumns       []string
	Parent             *Scope
	Type               ScopeType
	SubqueryScopes     []*Scope
	DerivedTableScopes []*Scope
	TableScopes        []*Scope
	CTEScopes          []*Scope
	UnionScopes        []*Scope
	UDTFScopes         []*Scope
	CanBeCorrelated    bool

	collected               bool
	scansAllSubscopeColumns bool
	rawColumns              []*Expr
	tableColumns            []*Expr
	stars                   []*Expr
	derivedTables           []*Expr
	udtfs                   []*Expr
	tables                  []*Expr
	ctes                    []*Expr
	subqueries              []*Expr
	joinHints               []*Expr
	semiAntiJoinTables      StrSet
	columnIndex             map[*Expr]struct{}
	selectedSources         *omap[SelectedSource]
	columns                 []*Expr
	columnsSet              bool
	externalColumns         []*Expr
	externalColumnsSet      bool
	localColumns            []*Expr
	localColumnsSet         bool
	pivots                  []*Expr
	pivotsSet               bool
	references              []scopeRef
	referencesSet           bool
}

// NewScope mirrors Scope(expression, sources, outer_columns, parent, scope_type, lateral_sources, cte_sources, can_be_correlated).
func NewScope(expression *Expr, sources *omap[Source], outerColumns []string, parent *Scope, scopeType ScopeType,
	lateralSources, cteSources *omap[Source], canBeCorrelated bool,
) *Scope {
	if scopeType == 0 {
		scopeType = ScopeRoot
	}
	s := &Scope{
		Expression:      expression,
		Sources:         sources,
		LateralSources:  lateralSources,
		CTESources:      cteSources,
		OuterColumns:    outerColumns,
		Parent:          parent,
		Type:            scopeType,
		CanBeCorrelated: canBeCorrelated,
	}
	if s.Sources == nil {
		s.Sources = newOMap[Source]()
	}
	if s.LateralSources == nil {
		s.LateralSources = newOMap[Source]()
	}
	if s.CTESources == nil {
		s.CTESources = newOMap[Source]()
	}
	if s.OuterColumns == nil {
		s.OuterColumns = []string{}
	}
	s.Sources.Update(s.LateralSources)
	s.Sources.Update(s.CTESources)
	s.ClearCache()
	return s
}

// ClearCache mirrors Scope.clear_cache.
func (s *Scope) ClearCache() {
	s.collected = false
	s.scansAllSubscopeColumns = false
	s.rawColumns = nil
	s.tableColumns = nil
	s.stars = nil
	s.derivedTables = nil
	s.udtfs = nil
	s.tables = nil
	s.ctes = nil
	s.subqueries = nil
	s.joinHints = nil
	// Rebuilt by collect before any use.
	s.semiAntiJoinTables = nil
	s.columnIndex = nil
	s.selectedSources = nil
	s.columns, s.columnsSet = nil, false
	s.externalColumns, s.externalColumnsSet = nil, false
	s.localColumns, s.localColumnsSet = nil, false
	s.pivots, s.pivotsSet = nil, false
	s.references, s.referencesSet = nil, false
}

// Branch mirrors Scope.branch.
func (s *Scope) Branch(expression *Expr, scopeType ScopeType, sources, cteSources, lateralSources *omap[Source], outerColumns []string) *Scope {
	var src *omap[Source]
	if sources.Len() > 0 {
		src = sources.Copy()
	}
	cte := s.CTESources.Copy()
	cte.Update(cteSources)
	var lat *omap[Source]
	if lateralSources.Len() > 0 {
		lat = lateralSources.Copy()
	}
	return NewScope(
		expression.Unnest(),
		src,
		outerColumns,
		s,
		scopeType,
		lat,
		cte,
		s.CanBeCorrelated || scopeType == ScopeSubquery || scopeType == ScopeUDTF,
	)
}

var (
	scopeCollectibleTypes = []Kind{KColumn, KDot, KTable, KQuery, KUDTF, KCTE, KStar, KTableColumn, KJoinHint}
	scopeCollectible      = kindMatcher(scopeCollectibleTypes...)
	columnAncestorKinds   = kindMatcher(KSelect, KQualify, KOrder, KHaving, KHint, KTable, KStar, KDistinct)
	cteOrQueryKinds       = kindMatcher(KCTE, KQuery)
)

func (s *Scope) collect() {
	s.tables = nil
	s.ctes = nil
	s.subqueries = nil
	s.derivedTables = nil
	s.udtfs = nil
	s.rawColumns = nil
	s.tableColumns = nil
	s.stars = nil
	s.joinHints = nil
	s.semiAntiJoinTables = newStrSet()
	s.columnIndex = map[*Expr]struct{}{}

	for node := range s.Walk(nil) {
		if node == s.Expression || !scopeCollectible.Has(node.kind) {
			continue
		}
		switch {
		case node.IsA(KDot) && node.IsStar():
			s.stars = append(s.stars, node)
		case node.Is(KColumn):
			s.columnIndex[node] = struct{}{}
			if node.This().IsA(KStar) {
				s.stars = append(s.stars, node)
			} else {
				s.rawColumns = append(s.rawColumns, node)
			}
		case node.IsA(KTable) && !node.Parent().IsA(KJoinHint):
			parent := node.Parent()
			if parent.IsA(KJoin) && parent.IsSemiOrAntiJoin() {
				s.semiAntiJoinTables.Add(node.AliasOrName())
			}
			s.tables = append(s.tables, node)
		case node.IsA(KJoinHint):
			s.joinHints = append(s.joinHints, node)
		case node.Is(KLateral) || (node.IsA(KUDTF) && node.Parent().IsA(KFrom, KJoin)):
			s.udtfs = append(s.udtfs, node)
		case node.IsA(KCTE):
			s.ctes = append(s.ctes, node)
		case isDerivedTable(node) && isFromOrJoin(node):
			s.derivedTables = append(s.derivedTables, node)
		case node.IsA(KSelect, KSetOperation) && !isFromOrJoin(node):
			s.subqueries = append(s.subqueries, node)
		case node.IsA(KTableColumn):
			s.tableColumns = append(s.tableColumns, node)
		case node.IsA(KStar) && (node.ArgB("except_") || !node.Parent().IsA(KCount)):
			s.scansAllSubscopeColumns = true
		}
	}
	s.collected = true
}

func (s *Scope) ensureCollected() {
	if !s.collected {
		s.collect()
	}
}

// Walk mirrors Scope.walk.
func (s *Scope) Walk(prune func(*Expr) bool) iter.Seq[*Expr] { return WalkInScope(s.Expression, prune) }

// Find mirrors Scope.find.
func (s *Scope) Find(kinds ...Kind) *Expr { return FindInScope(s.Expression, kinds...) }

// FindAll mirrors Scope.find_all.
func (s *Scope) FindAll(kinds ...Kind) iter.Seq[*Expr] { return FindAllInScope(s.Expression, kinds...) }

// Replace mirrors Scope.replace.
func (s *Scope) Replace(old, new *Expr) {
	old.Replace(new)
	s.ClearCache()
}

// Tables mirrors Scope.tables.
func (s *Scope) Tables() []*Expr { s.ensureCollected(); return s.tables }

// CTEs mirrors Scope.ctes.
func (s *Scope) CTEs() []*Expr { s.ensureCollected(); return s.ctes }

// DerivedTables mirrors Scope.derived_tables.
func (s *Scope) DerivedTables() []*Expr { s.ensureCollected(); return s.derivedTables }

// UDTFs mirrors Scope.udtfs.
func (s *Scope) UDTFs() []*Expr { s.ensureCollected(); return s.udtfs }

// Subqueries mirrors Scope.subqueries.
func (s *Scope) Subqueries() []*Expr { s.ensureCollected(); return s.subqueries }

// ScansAllSubscopeColumns mirrors Scope.scans_all_subscope_columns.
func (s *Scope) ScansAllSubscopeColumns() bool { s.ensureCollected(); return s.scansAllSubscopeColumns }

// Stars mirrors Scope.stars.
func (s *Scope) Stars() []*Expr { s.ensureCollected(); return s.stars }

// ColumnIndex mirrors Scope.column_index (object identity set).
func (s *Scope) ColumnIndex() map[*Expr]struct{} { s.ensureCollected(); return s.columnIndex }

// Columns mirrors Scope.columns.
func (s *Scope) Columns() []*Expr {
	if !s.columnsSet {
		s.ensureCollected()
		columns := s.rawColumns

		var external []*Expr
		for _, sc := range s.SubqueryScopes {
			external = append(external, sc.ExternalColumns()...)
		}
		for _, sc := range s.UDTFScopes {
			external = append(external, sc.ExternalColumns()...)
		}
		for _, dts := range s.DerivedTableScopes {
			if dts.CanBeCorrelated {
				external = append(external, dts.ExternalColumns()...)
			}
		}

		expr := s.Expression
		namedSelects := newStrSet()
		if expr.IsA(KQuery) {
			namedSelects.Add(expr.NamedSelects()...)
		}

		s.columns = []*Expr{}
		all := append(append([]*Expr{}, columns...), external...)
		for _, column := range all {
			ancestor := column.findAncestorIn(columnAncestorKinds)
			if ancestor == nil ||
				column.Text("table") != "" ||
				ancestor.IsA(KSelect) ||
				(ancestor.IsA(KTable) && !ancestor.This().IsA(KFunc)) ||
				(ancestor.IsA(KOrder, KDistinct) &&
					(ancestor.Parent().IsA(KWindow, KWithinGroup) ||
						!ancestor.Parent().IsA(KSelect) ||
						!namedSelects.Has(column.Name()))) ||
				(ancestor.IsA(KStar) && column.ArgKey() != "except_") {
				s.columns = append(s.columns, column)
			}
		}
		s.columnsSet = true
	}
	return s.columns
}

// TableColumns mirrors Scope.table_columns.
func (s *Scope) TableColumns() []*Expr { s.ensureCollected(); return s.tableColumns }

// SelectedSources mirrors Scope.selected_sources.
func (s *Scope) SelectedSources() *omap[SelectedSource] {
	if s.selectedSources == nil {
		result := newOMap[SelectedSource]()
		for _, ref := range s.References() {
			if s.semiAntiJoinTables.Has(ref.Name) {
				continue
			}
			if result.Has(ref.Name) {
				panic(&OptimizeError{Msg: "Alias already used: " + ref.Name})
			}
			if src, ok := s.Sources.Get(ref.Name); ok {
				result.Set(ref.Name, SelectedSource{Node: ref.Node, Source: src})
			}
		}
		s.selectedSources = result
	}
	return s.selectedSources
}

// References mirrors Scope.references.
func (s *Scope) References() []scopeRef {
	if !s.referencesSet {
		s.references = []scopeRef{}
		for _, table := range s.Tables() {
			s.references = append(s.references, scopeRef{table.AliasOrName(), table})
		}
		for _, e := range append(append([]*Expr{}, s.DerivedTables()...), s.UDTFs()...) {
			node := e
			if !e.ArgB("pivots") {
				node = e.UnnestSubqueryOrParen()
			}
			if !node.IsA(KSelectable) {
				panic(&ValueError{Msg: fmt.Sprintf("%s is not <class 'sqlglot.expressions.query.Selectable'>.", exprSQL(node))})
			}
			s.references = append(s.references, scopeRef{getSourceAlias(e), node})
		}
		s.referencesSet = true
	}
	return s.references
}

// UnnestSubqueryOrParen mirrors Expr.unnest() as dispatched for the receiver's class: Subquery.unnest
// strips nested subqueries, the default strips parentheses.
func (e *Expr) UnnestSubqueryOrParen() *Expr {
	if e.IsA(KSubquery) {
		return e.UnnestSubquery()
	}
	return e.Unnest()
}

// ExternalColumns mirrors Scope.external_columns.
func (s *Scope) ExternalColumns() []*Expr { return s.externalColumnsDepth(0) }

// maxScopeDepth bounds recursion through union scopes, which can be self-referential for malformed
// set operations (see traverseUnion); Python fails with RecursionError there.
const maxScopeDepth = 5000

// maxTraverseScopes bounds Scope.Traverse (see there).
const maxTraverseScopes = 1_000_000

func (s *Scope) externalColumnsDepth(depth int) []*Expr {
	if depth > maxScopeDepth {
		panic(&ValueError{Msg: "maximum recursion depth exceeded"})
	}
	if !s.externalColumnsSet {
		if s.Expression.IsA(KSetOperation) {
			if len(s.UnionScopes) != 2 {
				panic(&ValueError{Msg: fmt.Sprintf("not enough values to unpack (expected 2, got %d)", len(s.UnionScopes))})
			}
			left, right := s.UnionScopes[0], s.UnionScopes[1]
			s.externalColumns = append(append([]*Expr{}, left.externalColumnsDepth(depth+1)...), right.externalColumnsDepth(depth+1)...)
		} else {
			local := newStrSet()
			for _, ref := range s.References() {
				local.Add(ref.Name)
			}
			s.externalColumns = []*Expr{}
			for _, c := range s.Columns() {
				t := c.Text("table")
				if !local.Has(t) && !s.SemiOrAntiJoinTables().Has(t) {
					s.externalColumns = append(s.externalColumns, c)
				}
			}
		}
		s.externalColumnsSet = true
	}
	return s.externalColumns
}

// LocalColumns mirrors Scope.local_columns.
func (s *Scope) LocalColumns() []*Expr {
	if !s.localColumnsSet {
		ext := map[*Expr]struct{}{}
		for _, c := range s.ExternalColumns() {
			ext[c] = struct{}{}
		}
		s.localColumns = []*Expr{}
		for _, c := range s.Columns() {
			if _, ok := ext[c]; !ok {
				s.localColumns = append(s.localColumns, c)
			}
		}
		s.localColumnsSet = true
	}
	return s.localColumns
}

// UnqualifiedColumns mirrors Scope.unqualified_columns.
func (s *Scope) UnqualifiedColumns() []*Expr {
	out := []*Expr{}
	for _, c := range s.Columns() {
		if c.Text("table") == "" {
			out = append(out, c)
		}
	}
	return out
}

// JoinHints mirrors Scope.join_hints.
func (s *Scope) JoinHints() []*Expr { s.ensureCollected(); return s.joinHints }

// Pivots mirrors Scope.pivots.
func (s *Scope) Pivots() []*Expr {
	if !s.pivotsSet {
		s.pivots = []*Expr{}
		for _, ref := range s.References() {
			s.pivots = append(s.pivots, ref.Node.ArgL("pivots")...)
		}
		s.pivotsSet = true
	}
	return s.pivots
}

// SemiOrAntiJoinTables mirrors Scope.semi_or_anti_join_tables.
func (s *Scope) SemiOrAntiJoinTables() StrSet { s.ensureCollected(); return s.semiAntiJoinTables }

// SourceColumns mirrors Scope.source_columns.
func (s *Scope) SourceColumns(sourceName string) []*Expr {
	out := []*Expr{}
	for _, c := range s.Columns() {
		if c.Text("table") == sourceName {
			out = append(out, c)
		}
	}
	return out
}

func (s *Scope) IsSubquery() bool     { return s.Type == ScopeSubquery }
func (s *Scope) IsDerivedTable() bool { return s.Type == ScopeDerivedTable }
func (s *Scope) IsUnion() bool        { return s.Type == ScopeUnion }
func (s *Scope) IsCTE() bool          { return s.Type == ScopeCTE }
func (s *Scope) IsRoot() bool         { return s.Type == ScopeRoot }
func (s *Scope) IsUDTF() bool         { return s.Type == ScopeUDTF }

// IsCorrelatedSubquery mirrors Scope.is_correlated_subquery.
func (s *Scope) IsCorrelatedSubquery() bool {
	return s.CanBeCorrelated && len(s.ExternalColumns()) > 0
}

// RenameSource mirrors Scope.rename_source.
func (s *Scope) RenameSource(oldName, newName string) {
	if v, ok := s.Sources.Get(oldName); ok {
		s.Sources.Delete(oldName)
		s.Sources.Set(newName, v)
	}
}

// AddSource mirrors Scope.add_source.
func (s *Scope) AddSource(name string, src Source) {
	s.Sources.Set(name, src)
	s.ClearCache()
}

// RemoveSource mirrors Scope.remove_source.
func (s *Scope) RemoveSource(name string) {
	s.Sources.Delete(name)
	s.ClearCache()
}

// Traverse mirrors Scope.traverse (DFS post-order).
func (s *Scope) Traverse() []*Scope {
	stack := []*Scope{s}
	var result []*Scope
	for len(stack) > 0 {
		sc := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		result = append(result, sc)
		if len(result) > maxTraverseScopes {
			// Self-referential union scopes (malformed set operations) would make this loop
			// forever, as in Python; fail instead of exhausting memory.
			panic(&ValueError{Msg: "maximum recursion depth exceeded"})
		}
		stack = append(stack, sc.CTEScopes...)
		stack = append(stack, sc.UnionScopes...)
		stack = append(stack, sc.TableScopes...)
		stack = append(stack, sc.SubqueryScopes...)
	}
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

// RefCount mirrors Scope.ref_count, keyed by source identity (*Scope or *Expr).
func (s *Scope) RefCount() map[any]int {
	counts := map[any]int{}
	for _, sc := range s.Traverse() {
		for _, ss := range sc.SelectedSources().Values() {
			counts[ss.Source.id()]++
		}
		for name := range sc.semiAntiJoinTables {
			if src, ok := sc.Sources.Get(name); ok {
				counts[src.id()]++
			}
		}
	}
	return counts
}

// TraverseScope mirrors sqlglot.optimizer.scope.traverse_scope.
//
// Python's _traverse_scope is a generator that traverse_scope fully consumes into a list; the
// port appends to that list directly. The bookkeeping Python runs after each yielded child scope
// (remembering the last child, registering it as a source) is done in the same order once the
// child traversal returns: none of the child traversals read that state (branches copy their
// source maps), so the result is identical.
func TraverseScope(e *Expr) []*Scope {
	if e.IsA(KQuery, KDDL, KDML) {
		var out []*Scope
		traverseScopeInto(NewScope(e, nil, nil, nil, ScopeRoot, nil, nil, false), &out)
		return out
	}
	return nil
}

// BuildScope mirrors sqlglot.optimizer.scope.build_scope.
func BuildScope(e *Expr) *Scope {
	scopes := TraverseScope(e)
	if len(scopes) == 0 {
		return nil
	}
	return scopes[len(scopes)-1]
}

// lastAdded returns the last scope appended to out after index start, or nil.
func lastAdded(out []*Scope, start int) *Scope {
	if len(out) > start {
		return out[len(out)-1]
	}
	return nil
}

func traverseScopeInto(scope *Scope, out *[]*Scope) {
	expression := scope.Expression
	switch {
	case expression.IsA(KSelect):
		traverseSelectInto(scope, out)
	case expression.IsA(KSetOperation):
		traverseCTEsInto(scope, out)
		traverseUnionInto(scope, out)
		return
	case expression.IsA(KSubquery):
		if scope.IsRoot() {
			traverseSelectInto(scope, out)
		} else {
			traverseSubqueriesInto(scope, out)
		}
	case expression.IsA(KTable):
		traverseTablesInto(scope, out)
	case expression.IsA(KUDTF):
		traverseUDTFsInto(scope, out)
	case expression.IsA(KDDL):
		ddl := expression.ArgE("expression")
		if ddl.IsA(KQuery) {
			traverseCTEsInto(scope, out)
			traverseScopeInto(NewScope(ddl, nil, nil, nil, ScopeRoot, nil, scope.CTESources, false), out)
		}
		return
	case expression.IsA(KDML):
		traverseCTEsInto(scope, out)
		for query := range FindAllInScope(expression, KQuery) {
			if !query.Parent().IsA(KCTE, KSubquery) {
				traverseScopeInto(NewScope(query, nil, nil, nil, ScopeRoot, nil, scope.CTESources, false), out)
			}
		}
		return
	default:
		return
	}
	*out = append(*out, scope)
}

func traverseSelectInto(scope *Scope, out *[]*Scope) {
	traverseCTEsInto(scope, out)
	traverseTablesInto(scope, out)
	traverseSubqueriesInto(scope, out)
}

func traverseUnionInto(scope *Scope, out *[]*Scope) {
	var prevScope *Scope
	last := scope
	unionScopeStack := []*Scope{scope}
	setOp := scope.Expression
	exprStack := []*Expr{setOp.ArgE("expression"), setOp.ArgE("this")}
	for len(exprStack) > 0 {
		expression := exprStack[len(exprStack)-1]
		exprStack = exprStack[:len(exprStack)-1]
		unionScope := unionScopeStack[len(unionScopeStack)-1]
		newScope := unionScope.Branch(expression, ScopeUnion, nil, nil, nil, unionScope.OuterColumns)
		if expression.IsA(KSetOperation) {
			traverseCTEsInto(newScope, out)
			unionScopeStack = append(unionScopeStack, newScope)
			exprStack = append(exprStack, expression.ArgE("expression"), expression.ArgE("this"))
			continue
		}
		// Python's loop variable `scope` shadows the parameter and keeps its last value across
		// iterations; when a branch yields no scope (an operand that is not a query), the
		// previous value — initially the set operation's own scope — is used.
		start := len(*out)
		traverseScopeInto(newScope, out)
		if sc := lastAdded(*out, start); sc != nil {
			last = sc
		}
		if prevScope != nil {
			unionScopeStack = unionScopeStack[:len(unionScopeStack)-1]
			unionScope.UnionScopes = []*Scope{prevScope, last}
			prevScope = unionScope
			*out = append(*out, unionScope)
		} else {
			prevScope = last
		}
	}
}

func traverseCTEsInto(scope *Scope, out *[]*Scope) {
	sources := newOMap[Source]()
	for _, cte := range scope.CTEs() {
		cteName := cte.Alias()
		with := scope.Expression.ArgE("with_")
		if with != nil && with.ArgB("recursive") {
			union := cte.This()
			if union.IsA(KSetOperation) {
				sources.Set(cteName, Source{Scope: scope.Branch(union.This(), ScopeCTE, nil, nil, nil, nil)})
			}
		}
		start := len(*out)
		traverseScopeInto(scope.Branch(cte.This(), ScopeCTE, nil, sources, nil, cte.AliasColumnNames()), out)
		if child := lastAdded(*out, start); child != nil {
			sources.Set(cteName, Source{Scope: child})
			scope.CTEScopes = append(scope.CTEScopes, child)
		}
	}
	scope.Sources.Update(sources)
	scope.CTESources.Update(sources)
}

// isDerivedTable mirrors scope._is_derived_table.
func isDerivedTable(e *Expr) bool {
	return e.IsA(KSubquery) && (e.Alias() != "" || e.This().IsA(KSelect, KSetOperation))
}

// isFromOrJoin mirrors scope._is_from_or_join.
func isFromOrJoin(e *Expr) bool {
	parent := e.Parent()
	for parent.Is(KSubquery) {
		parent = parent.Parent()
	}
	return parent.Is(KFrom) || parent.Is(KJoin)
}

func findNewNameOMap(taken func(string) bool, base string) string {
	if !taken(base) {
		return base
	}
	i := 2
	n := fmt.Sprintf("%s_%d", base, i)
	for taken(n) {
		i++
		n = fmt.Sprintf("%s_%d", base, i)
	}
	return n
}

func traverseTablesInto(scope *Scope, out *[]*Scope) {
	sources := newOMap[Source]()
	var expressions []*Expr
	if from := scope.Expression.ArgE("from_"); from != nil {
		expressions = append(expressions, from.This())
	}
	for _, join := range scope.Expression.ArgL("joins") {
		expressions = append(expressions, join.This())
	}
	if scope.Expression.IsA(KTable) {
		expressions = append(expressions, scope.Expression)
	}
	expressions = append(expressions, scope.Expression.ArgL("laterals")...)

	for i := 0; i < len(expressions); i++ {
		expression := expressions[i]
		if expression.IsA(KFinal) {
			expression = expression.This()
		}
		if expression.IsA(KTable) {
			tableName := expression.Name()
			sourceName := expression.AliasOrName()
			if scope.Sources.Has(tableName) && expression.DbName() == "" {
				if pivots := expression.ArgL("pivots"); len(pivots) > 0 {
					sources.Set(pivots[0].Alias(), Source{Table: expression})
				} else {
					src, _ := scope.Sources.Get(tableName)
					sources.Set(sourceName, src)
				}
			} else if sources.Has(sourceName) {
				sources.Set(findNewNameOMap(sources.Has, tableName), Source{Table: expression})
			} else {
				sources.Set(sourceName, Source{Table: expression})
			}
			if expression != scope.Expression {
				for _, join := range expression.ArgL("joins") {
					expressions = append(expressions, join.This())
				}
			}
			continue
		}
		if !expression.IsA(KDerivedTable) {
			continue
		}
		node := expression
		var lateralSources *omap[Source]
		var scopeType ScopeType
		var target *[]*Scope
		if expression.IsA(KUDTF) {
			lateralSources = sources
			scopeType = ScopeUDTF
			target = &scope.UDTFScopes
		} else if isDerivedTable(expression) {
			scopeType = ScopeDerivedTable
			target = &scope.DerivedTableScopes
			for _, join := range node.ArgL("joins") {
				expressions = append(expressions, join.This())
			}
		} else {
			expressions = append(expressions, node.This())
			for _, join := range node.ArgL("joins") {
				expressions = append(expressions, join.This())
			}
			continue
		}
		start := len(*out)
		traverseScopeInto(scope.Branch(node, scopeType, nil, nil, lateralSources, node.AliasColumnNames()), out)
		for _, sc := range (*out)[start:] {
			sources.Set(getSourceAlias(node), Source{Scope: sc})
		}
		if child := lastAdded(*out, start); child != nil {
			*target = append(*target, child)
			scope.TableScopes = append(scope.TableScopes, child)
		}
	}
	scope.Sources.Update(sources)
}

func traverseSubqueriesInto(scope *Scope, out *[]*Scope) {
	for _, subquery := range scope.Subqueries() {
		start := len(*out)
		traverseScopeInto(scope.Branch(subquery, ScopeSubquery, nil, nil, nil, nil), out)
		if top := lastAdded(*out, start); top != nil {
			scope.SubqueryScopes = append(scope.SubqueryScopes, top)
		}
	}
}

func traverseUDTFsInto(scope *Scope, out *[]*Scope) {
	var udtfExprs []*Expr
	if scope.Expression.IsA(KUnnest) {
		udtfExprs = scope.Expression.Expressions()
	} else if scope.Expression.IsA(KLateral) {
		udtfExprs = []*Expr{scope.Expression.This()}
	}
	sources := newOMap[Source]()
	for _, expression := range udtfExprs {
		if expression.IsA(KSubquery) {
			start := len(*out)
			traverseScopeInto(scope.Branch(expression, ScopeSubquery, nil, nil, nil, expression.AliasColumnNames()), out)
			for _, sc := range (*out)[start:] {
				sources.Set(getSourceAlias(expression), Source{Scope: sc})
			}
			if top := lastAdded(*out, start); top != nil {
				scope.SubqueryScopes = append(scope.SubqueryScopes, top)
			}
		}
	}
	scope.Sources.Update(sources)
}

// WalkInScope mirrors sqlglot.optimizer.scope.walk_in_scope.
func WalkInScope(expression *Expr, prune func(*Expr) bool) iter.Seq[*Expr] {
	return func(yield func(*Expr) bool) {
		walkInScopeImpl(expression, prune, yield)
	}
}

func walkInScopeImpl(expression *Expr, prune func(*Expr) bool, yield func(*Expr) bool) bool {
	var buf [32]*Expr
	stack := append(buf[:0], expression)
	for len(stack) > 0 {
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if !yield(node) {
			return false
		}
		if node != expression && node != nil && cteOrQueryKinds.Has(node.kind) &&
			(node.IsA(KCTE) ||
				(node.Parent().IsA(KFrom, KJoin) && isDerivedTable(node)) ||
				node.Parent().IsA(KUDTF) ||
				node.IsA(KSelect, KSetOperation)) {
			if node.IsA(KSubquery, KUDTF) {
				for _, key := range []string{"joins", "laterals", "pivots"} {
					for _, a := range node.ArgL(key) {
						if !walkInScopeImpl(a, nil, yield) {
							return false
						}
					}
				}
			}
			continue
		}
		if prune != nil && prune(node) {
			continue
		}
		if node == nil {
			// Python: walking None fails on `node.args`.
			panic(&ValueError{Msg: "'NoneType' object has no attribute 'args'"})
		}
		stack = node.appendChildren(stack, true)
	}
	return true
}

// FindAllInScope mirrors sqlglot.optimizer.scope.find_all_in_scope.
func FindAllInScope(expression *Expr, kinds ...Kind) iter.Seq[*Expr] {
	return func(yield func(*Expr) bool) {
		for node := range WalkInScope(expression, nil) {
			if node.IsA(kinds...) {
				if !yield(node) {
					return
				}
			}
		}
	}
}

// FindInScope mirrors sqlglot.optimizer.scope.find_in_scope.
func FindInScope(expression *Expr, kinds ...Kind) *Expr {
	for n := range FindAllInScope(expression, kinds...) {
		return n
	}
	return nil
}

// getSourceAlias mirrors scope._get_source_alias.
func getSourceAlias(e *Expr) string {
	aliasArg := e.ArgE("alias")
	aliasName := e.Alias()
	if aliasName == "" && aliasArg.IsA(KTableAlias) && len(aliasArg.ArgL("columns")) == 1 {
		aliasName = aliasArg.ArgL("columns")[0].Name()
	}
	return aliasName
}
