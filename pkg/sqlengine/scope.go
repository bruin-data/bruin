package sqlengine

import (
	"fmt"
	"iter"
	"sync"
)

// ScopeType says how a scope relates to its parent.
type ScopeType uint8

const (
	ScopeRoot ScopeType = iota + 1
	ScopeSubquery
	ScopeDerivedTable
	ScopeCTE
	ScopeUnion
	ScopeUDTF
)

// Source is a scope source: exactly one of Table (a Table expression) or Scope is set.
type Source struct {
	Table *Expr
	Scope *Scope
}

func (s Source) IsScope() bool { return s.Scope != nil }

func (s Source) id() any {
	if s.Scope != nil {
		return s.Scope
	}
	return s.Table
}

// SelectedSource is an entry of SelectedSources: the referencing node and its source.
type SelectedSource struct {
	Node   *Expr
	Source Source
}

type scopeRef struct {
	Name string
	Node *Expr
}

// Scope is a SELECT-like expression with its sources (tables, CTEs, derived tables), its child
// scopes and the columns, tables and subqueries collected from it on demand.
type Scope struct {
	Expression         *Expr
	Sources            *omap[Source]
	LateralSources     *omap[Source]
	CTESources         *omap[Source]
	OuterColumns       []string
	Parent             *Scope
	SubqueryScopes     []*Scope
	DerivedTableScopes []*Scope
	TableScopes        []*Scope
	CTEScopes          []*Scope
	UnionScopes        []*Scope
	UDTFScopes         []*Scope
	Type               ScopeType
	CanBeCorrelated    bool

	collected               bool
	scansAllSubscopeColumns bool
	cached                  scopeCache // which lazily computed lists below are set

	rawColumns         []*Expr
	tableColumns       []*Expr
	stars              []*Expr
	derivedTables      []*Expr
	udtfs              []*Expr
	tables             []*Expr
	orderedSources     []*Expr // tables, UDTFs and derived tables in walk order
	ctes               []*Expr
	subqueries         []*Expr
	joinHints          []*Expr
	semiAntiJoinTables StrSet
	columnIndex        map[*Expr]struct{}
	selectedSources    *omap[SelectedSource]
	columns            []*Expr
	externalColumns    []*Expr
	localColumns       []*Expr
	pivots             []*Expr
	references         []scopeRef
}

type scopeCache uint8

const (
	cacheColumns scopeCache = 1 << iota
	cacheExternalColumns
	cacheLocalColumns
	cachePivots
	cacheReferences
)

type scopeWithMaps struct {
	s    Scope
	maps [3]omap[Source]
}

// NewScope creates a scope for expression. Nil source maps start empty, and the lateral and CTE
// sources are added to sources. A zero scopeType means ScopeRoot.
func NewScope(expression *Expr, sources *omap[Source], outerColumns []string, parent *Scope, scopeType ScopeType,
	lateralSources, cteSources *omap[Source], canBeCorrelated bool,
) *Scope {
	if scopeType == 0 {
		scopeType = ScopeRoot
	}
	var s *Scope
	if sources == nil || lateralSources == nil || cteSources == nil {
		// Allocate the scope together with the empty mappings it needs.
		b := new(scopeWithMaps)
		s = &b.s
		if sources == nil {
			sources = &b.maps[0]
		}
		if lateralSources == nil {
			lateralSources = &b.maps[1]
		}
		if cteSources == nil {
			cteSources = &b.maps[2]
		}
	} else {
		s = new(Scope)
	}
	*s = Scope{
		Expression:      expression,
		Sources:         sources,
		LateralSources:  lateralSources,
		CTESources:      cteSources,
		OuterColumns:    outerColumns,
		Parent:          parent,
		Type:            scopeType,
		CanBeCorrelated: canBeCorrelated,
	}
	if s.OuterColumns == nil {
		s.OuterColumns = []string{}
	}
	s.Sources.Update(s.LateralSources)
	s.Sources.Update(s.CTESources)
	s.ClearCache()
	return s
}

func (s *Scope) ClearCache() {
	s.collected = false
	s.scansAllSubscopeColumns = false
	s.rawColumns = nil
	s.tableColumns = nil
	s.stars = nil
	s.derivedTables = nil
	s.udtfs = nil
	s.tables = nil
	s.orderedSources = nil
	s.ctes = nil
	s.subqueries = nil
	s.joinHints = nil
	// Rebuilt by collect before any use.
	s.semiAntiJoinTables = nil
	s.columnIndex = nil
	s.selectedSources = nil
	s.columns, s.externalColumns, s.localColumns, s.pivots, s.references = nil, nil, nil, nil, nil
	s.cached = 0
}

// ClearColumnCache drops the column classifications (columns, external and local columns), e.g.
// after columns were qualified in place.
func (s *Scope) ClearColumnCache() {
	s.columns, s.externalColumns, s.localColumns = nil, nil, nil
	s.cached &^= cacheColumns | cacheExternalColumns | cacheLocalColumns
}

// Branch creates a child scope for expression (unnested) that inherits the CTE sources. The child
// can be correlated when s can, or when it is a subquery or UDTF scope.
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

// Categories of the nodes collect gathers, in the order of their lists in one shared block.
const (
	collStars = iota
	collRawColumns
	collTables
	collJoinHints
	collUDTFs
	collCTEs
	collDerivedTables
	collSubqueries
	collTableColumns
	numCollected
)

type collectedNode struct {
	cat  uint8
	node *Expr
}

// collectBufPool holds scratch lists for collect, which then stores all gathered lists in a
// single allocation.
var collectBufPool = sync.Pool{New: func() any { b := make([]collectedNode, 0, 64); return &b }}

func (s *Scope) collect() {
	s.semiAntiJoinTables = nil // allocated on the first semi/anti join table
	s.columnIndex = nil

	bufp := collectBufPool.Get().(*[]collectedNode)
	items := (*bufp)[:0]
	var counts [numCollected]int
	var orderedSources []*Expr
	// The right-hand side source of a SEMI/ANTI join is only visible within the join's own ON
	// clause, not the rest of the scope
	addSemiOrAntiJoinTable := func(node *Expr) {
		if parent := node.Parent(); parent.IsA(KJoin) && parent.IsSemiOrAntiJoin() {
			if s.semiAntiJoinTables == nil {
				s.semiAntiJoinTables = newStrSet()
			}
			s.semiAntiJoinTables.Add(node.AliasOrName())
		}
	}
	add := func(cat uint8, node *Expr) {
		items = append(items, collectedNode{cat, node})
		counts[cat]++
		if cat == collTables || cat == collUDTFs || cat == collDerivedTables {
			orderedSources = append(orderedSources, node)
		}
	}
	for node := range s.Walk(nil) {
		if node == s.Expression || !scopeCollectible.Has(node.kind) {
			continue
		}
		switch {
		case node.IsA(KDot) && node.IsStar():
			add(collStars, node)
		case node.Is(KColumn):
			if node.This().IsA(KStar) {
				add(collStars, node)
			} else {
				add(collRawColumns, node)
			}
		case node.IsA(KTable) && !node.Parent().IsA(KJoinHint):
			addSemiOrAntiJoinTable(node)
			add(collTables, node)
		case node.IsA(KJoinHint):
			add(collJoinHints, node)
		case node.Is(KLateral) || (node.IsA(KUDTF) && node.Parent().IsA(KFrom, KJoin)):
			addSemiOrAntiJoinTable(node)
			add(collUDTFs, node)
		case node.IsA(KCTE):
			add(collCTEs, node)
		case isDerivedTable(node) && isFromOrJoin(node):
			addSemiOrAntiJoinTable(node)
			add(collDerivedTables, node)
		case node.IsA(KSelect, KSetOperation) && !isFromOrJoin(node):
			add(collSubqueries, node)
		case node.IsA(KTableColumn):
			add(collTableColumns, node)
		case node.IsA(KStar) && (node.ArgB("except_") || !node.Parent().IsA(KCount)):
			s.scansAllSubscopeColumns = true
		}
	}

	var lists [numCollected][]*Expr // nil when empty, like the lists before any append
	if len(items) > 0 {
		block := make([]*Expr, len(items))
		var next [numCollected]int
		off := 0
		for c, n := range counts {
			next[c] = off
			if n > 0 {
				lists[c] = block[off : off+n : off+n]
			}
			off += n
		}
		for _, it := range items {
			block[next[it.cat]] = it.node
			next[it.cat]++
		}
	}
	s.stars, s.rawColumns, s.tables = lists[collStars], lists[collRawColumns], lists[collTables]
	s.joinHints, s.udtfs, s.ctes = lists[collJoinHints], lists[collUDTFs], lists[collCTEs]
	s.derivedTables, s.subqueries, s.tableColumns = lists[collDerivedTables], lists[collSubqueries], lists[collTableColumns]
	s.orderedSources = orderedSources

	clear(items)
	if cap(items) <= 4096 {
		*bufp = items[:0]
		collectBufPool.Put(bufp)
	}
	s.collected = true
}

func (s *Scope) ensureCollected() {
	if !s.collected {
		s.collect()
	}
}

// Walk yields the nodes of the scope's expression without entering child scopes (see
// WalkInScope).
func (s *Scope) Walk(prune func(*Expr) bool) iter.Seq[*Expr] { return WalkInScope(s.Expression, prune) }

func (s *Scope) Find(kinds ...Kind) *Expr { return FindInScope(s.Expression, kinds...) }

func (s *Scope) FindAll(kinds ...Kind) iter.Seq[*Expr] { return FindAllInScope(s.Expression, kinds...) }

// Replace replaces old with new and clears the scope's cache.
func (s *Scope) Replace(old, new *Expr) {
	old.Replace(new)
	s.ClearCache()
}

func (s *Scope) Tables() []*Expr { s.ensureCollected(); return s.tables }

func (s *Scope) CTEs() []*Expr { s.ensureCollected(); return s.ctes }

// DerivedTables returns the subqueries used as FROM or JOIN sources.
func (s *Scope) DerivedTables() []*Expr { s.ensureCollected(); return s.derivedTables }

// UDTFs returns the table functions (UNNEST, LATERAL, ...) used as sources.
func (s *Scope) UDTFs() []*Expr { s.ensureCollected(); return s.udtfs }

// Subqueries returns the queries nested in the scope outside FROM and JOIN, such as in WHERE or
// in the projections.
func (s *Scope) Subqueries() []*Expr { s.ensureCollected(); return s.subqueries }

// ScansAllSubscopeColumns reports whether the scope has a star that reads every column of its
// sources: any star except the one in COUNT(*), unless it has EXCEPT.
func (s *Scope) ScansAllSubscopeColumns() bool { s.ensureCollected(); return s.scansAllSubscopeColumns }

// Stars returns the star projections: dotted stars and columns named *.
func (s *Scope) Stars() []*Expr { s.ensureCollected(); return s.stars }

// ColumnIndex returns the set of collected Column nodes, by identity. It is rarely needed, so it is
// built on first use from the collected columns and column stars.
func (s *Scope) ColumnIndex() map[*Expr]struct{} {
	s.ensureCollected()
	if s.columnIndex == nil {
		idx := make(map[*Expr]struct{}, len(s.rawColumns))
		for _, c := range s.rawColumns {
			idx[c] = struct{}{}
		}
		for _, c := range s.stars {
			if c.Is(KColumn) {
				idx[c] = struct{}{}
			}
		}
		s.columnIndex = idx
	}
	return s.columnIndex
}

// Columns returns the columns that reference sources: the scope's own columns plus the external
// columns of its subquery, UDTF and correlated derived table scopes, minus unqualified names that
// refer to something else, such as projection aliases in ORDER BY.
func (s *Scope) Columns() []*Expr {
	if s.cached&cacheColumns == 0 {
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
			// Star RENAME sources are column labels, not references.
			if column.Parent().IsA(KAlias) && column.Parent().ArgKey() == "rename" {
				continue
			}

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
		s.cached |= cacheColumns
	}
	return s.columns
}

func (s *Scope) TableColumns() []*Expr { s.ensureCollected(); return s.tableColumns }

// SelectedSources maps each referenced source name to its node and source, skipping semi and
// anti join tables. A name that is used twice panics.
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

// References returns the (alias, node) pairs of the tables, derived tables and UDTFs the scope
// selects from, in the order they appear (which star expansion relies on).
func (s *Scope) References() []scopeRef {
	if s.cached&cacheReferences == 0 {
		s.ensureCollected()
		s.references = []scopeRef{}
		for _, e := range s.orderedSources {
			if e.IsA(KTable) {
				s.references = append(s.references, scopeRef{e.AliasOrName(), e})
				continue
			}
			node := e
			if !e.ArgB("pivots") {
				node = e.UnnestSubqueryOrParen()
			}
			if !node.IsA(KSelectable) {
				panic(&ValueError{Msg: exprSQL(node) + " is not Selectable."})
			}
			s.references = append(s.references, scopeRef{getSourceAlias(e), node})
		}
		s.cached |= cacheReferences
	}
	return s.references
}

func (e *Expr) UnnestSubqueryOrParen() *Expr {
	if e.IsA(KSubquery) {
		return e.UnnestSubquery()
	}
	return e.Unnest()
}

// ExternalColumns returns the columns that reference no source of the scope, such as the outer
// columns of a correlated subquery. For a set operation they are those of both sides.
func (s *Scope) ExternalColumns() []*Expr { return s.externalColumnsDepth(0) }

// maxScopeDepth bounds recursion through union scopes, which can be self-referential for malformed
// set operations (see traverseUnionInto); deeper recursion panics with "maximum recursion depth
// exceeded".
const maxScopeDepth = 5000

// maxTraverseScopes bounds Scope.Traverse (see there).
const maxTraverseScopes = 1_000_000

func (s *Scope) externalColumnsDepth(depth int) []*Expr {
	if depth > maxScopeDepth {
		panic(&ValueError{Msg: "maximum recursion depth exceeded"})
	}
	if s.cached&cacheExternalColumns == 0 {
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
				if !local.Has(t) && !s.semiAntiJoinTables.Has(t) {
					s.externalColumns = append(s.externalColumns, c)
				}
			}
		}
		s.cached |= cacheExternalColumns
	}
	return s.externalColumns
}

// LocalColumns returns the columns that reference a source of the scope.
func (s *Scope) LocalColumns() []*Expr {
	if s.cached&cacheLocalColumns == 0 {
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
		s.cached |= cacheLocalColumns
	}
	return s.localColumns
}

func (s *Scope) UnqualifiedColumns() []*Expr {
	out := []*Expr{}
	for _, c := range s.Columns() {
		if c.Text("table") == "" {
			out = append(out, c)
		}
	}
	return out
}

func (s *Scope) JoinHints() []*Expr { s.ensureCollected(); return s.joinHints }

// Pivots returns the pivots attached to the scope's sources.
func (s *Scope) Pivots() []*Expr {
	if s.cached&cachePivots == 0 {
		s.pivots = []*Expr{}
		for _, ref := range s.References() {
			s.pivots = append(s.pivots, ref.Node.ArgL("pivots")...)
		}
		s.cached |= cachePivots
	}
	return s.pivots
}

func (s *Scope) SemiOrAntiJoinTables() StrSet {
	s.ensureCollected()
	if s.semiAntiJoinTables == nil {
		s.semiAntiJoinTables = newStrSet()
	}
	return s.semiAntiJoinTables
}

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

// IsCorrelatedSubquery reports whether the scope can be correlated and references outer columns.
func (s *Scope) IsCorrelatedSubquery() bool {
	return s.CanBeCorrelated && len(s.ExternalColumns()) > 0
}

// RenameSource renames a source, which moves it to the end of the source order.
func (s *Scope) RenameSource(oldName, newName string) {
	if v, ok := s.Sources.Get(oldName); ok {
		s.Sources.Delete(oldName)
		s.Sources.Set(newName, v)
	}
}

// AddSource adds or replaces a source and clears the cache.
func (s *Scope) AddSource(name string, src Source) {
	s.Sources.Set(name, src)
	s.ClearCache()
}

// RemoveSource removes a source and clears the cache.
func (s *Scope) RemoveSource(name string) {
	s.Sources.Delete(name)
	s.ClearCache()
}

// Traverse returns s and all its descendant scopes, children before parents.
func (s *Scope) Traverse() []*Scope {
	stack := []*Scope{s}
	var result []*Scope
	for len(stack) > 0 {
		sc := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		result = append(result, sc)
		if len(result) > maxTraverseScopes {
			// Self-referential union scopes (malformed set operations) would make this loop
			// forever; fail instead of exhausting memory.
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

// RefCount counts, over s and its descendant scopes, how often each source is selected, keyed by
// source identity (*Scope or *Expr).
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

// TraverseScope builds the scopes of a query, DDL or DML statement and returns them children
// first, the root last; other expressions yield nil.
//
// Scopes are appended to a single list as they are completed. The bookkeeping after each child
// scope (remembering the last child, registering it as a source) runs once that child's traversal
// returns; child traversals never read that state (branches copy their source maps), so this does
// not affect the result.
func TraverseScope(e *Expr) []*Scope {
	if e.IsA(KQuery, KDDL, KDML) {
		var out []*Scope
		traverseScopeInto(NewScope(e, nil, nil, nil, ScopeRoot, nil, nil, false), &out)
		return out
	}
	return nil
}

// BuildScope returns the root scope of e, or nil when e is not a query, DDL or DML statement.
func BuildScope(e *Expr) *Scope {
	scopes := TraverseScope(e)
	if len(scopes) == 0 {
		return nil
	}
	return scopes[len(scopes)-1]
}

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
		// last keeps its value across iterations: when a branch yields no scope (an operand
		// that is not a query), the previous value, initially the set operation's own scope,
		// is used.
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

// isDerivedTable reports whether e is a subquery used as a table: aliased, pivoted, or wrapping a
// SELECT or set operation.
func isDerivedTable(e *Expr) bool {
	return e.IsA(KSubquery) && (e.Alias() != "" || e.ArgB("pivots") || e.This().IsA(KSelect, KSetOperation))
}

// isFromOrJoin reports whether e, looking through enclosing subqueries, is the source of a FROM or
// JOIN.
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
				// A reference to a parent source (e.g. a CTE), not an actual table. If it is
				// pivoted, the pivot's alias is also registered as a new table source.
				src, _ := scope.Sources.Get(tableName)
				sources.Set(sourceName, src)
				if pivots := expression.ArgL("pivots"); len(pivots) > 0 {
					sources.Set(pivots[len(pivots)-1].Alias(), Source{Table: expression})
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

// WalkInScope yields the nodes of expression depth-first without descending into child scopes
// (CTEs, derived tables, UDTF arguments and nested queries); the joins, laterals and pivots of a
// derived table or UDTF are still walked. prune stops the descent below a node.
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
			// a nil node is an error; the message keeps its historical wording
			panic(&ValueError{Msg: "'NoneType' object has no attribute 'args'"})
		}
		stack = node.appendChildren(stack, true)
	}
	return true
}

// FindAllInScope yields the nodes of the given kinds in the scope of expression (see
// WalkInScope).
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

func FindInScope(expression *Expr, kinds ...Kind) *Expr {
	for n := range FindAllInScope(expression, kinds...) {
		return n
	}
	return nil
}

// getSourceAlias returns the alias of a source, or its only column alias when the table alias has
// no name.
func getSourceAlias(e *Expr) string {
	aliasArg := e.ArgE("alias")
	aliasName := e.Alias()
	if aliasName == "" && aliasArg.IsA(KTableAlias) && len(aliasArg.ArgL("columns")) == 1 {
		aliasName = aliasArg.ArgL("columns")[0].Name()
	}
	return aliasName
}
