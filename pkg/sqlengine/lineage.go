package sqlengine

import (
	"fmt"
	"strings"
)

// LineageNode is one step in a column's lineage: a column (or set operation) in some scope,
// with the nodes it reads from in Downstream.
type LineageNode struct {
	Name              string
	Expression        *Expr
	Source            *Expr
	Downstream        []*LineageNode
	SourceName        string
	ReferenceNodeName string
}

// Walk returns n and all nodes reachable through Downstream in depth-first order, each once.
func (n *LineageNode) Walk() []*LineageNode {
	visited := map[*LineageNode]bool{}
	queue := []*LineageNode{n}
	var out []*LineageNode
	for len(queue) > 0 {
		node := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if visited[node] {
			continue
		}
		visited[node] = true
		out = append(out, node)
		for i := len(node.Downstream) - 1; i >= 0; i-- {
			queue = append(queue, node.Downstream[i])
		}
	}
	return out
}

// LineageError reports why lineage could not be computed, e.g. a statement that is not a query or
// an unknown or unnamed column.
type LineageError struct{ Msg string }

func (e *LineageError) Error() string { return e.Msg }

type lineageCacheKey struct {
	column            string
	columnIdx         int
	isIdx             bool
	scope             *Scope
	scopeName         string
	sourceName        string
	referenceNodeName string
}

type lineageScopeMeta struct {
	isStar       bool
	selectByName map[string]*Expr
}

type lineageState struct {
	dialect     *Dialect
	trimSelects bool
	schema      *MappingSchema
	cache       map[lineageCacheKey]*LineageNode
	scopeMeta   map[*Scope]*lineageScopeMeta
}

// LineageOptions configures LineageAll and LineageColumn.
type LineageOptions struct {
	Schema      *MappingSchema
	Dialect     *Dialect
	Scope       *Scope
	TrimSelects bool
	Copy        bool
}

// LineageAll computes the lineage of every output column of expression, returning the column
// names in projection order and a node per name. The expression must already be qualified when
// opts.Scope is provided.
func LineageAll(expression *Expr, opts LineageOptions) (names []string, nodes map[string]*LineageNode, err error) {
	defer recoverLineage(&err)
	scope, st := prepareLineage(expression, opts)
	selectable := scope.Expression
	nodes = map[string]*LineageNode{}
	for _, sel := range selectable.Selects() {
		name := sel.AliasOrName()
		if name == "" {
			s, _ := sel.SQL(opts.Dialect.Name, nil)
			panic(&LineageError{Msg: fmt.Sprintf("Cannot fetch lineage for unnamed projection: %s.", s)})
		}
		node := st.toNode(name, -1, false, scope, "", nil, "", "")
		if _, ok := nodes[name]; !ok {
			names = append(names, name)
		}
		nodes[name] = node
	}
	return names, nodes, nil
}

// LineageColumn computes the lineage of one output column of expression.
func LineageColumn(column string, expression *Expr, opts LineageOptions) (node *LineageNode, err error) {
	defer recoverLineage(&err)
	scope, st := prepareLineage(expression, opts)
	selectable := scope.Expression
	columnName := NormalizeIdentifiers(ToIdentifier(column, nil), opts.Dialect).Name()
	found := false
	for _, sel := range selectable.Selects() {
		if sel.AliasOrName() == columnName {
			found = true
			break
		}
	}
	if !found {
		panic(&LineageError{Msg: fmt.Sprintf("Cannot find column '%s' in query.", columnName)})
	}
	return st.toNode(columnName, -1, false, scope, "", nil, "", ""), nil
}

func recoverLineage(err *error) {
	if r := recover(); r != nil {
		switch x := r.(type) {
		case *LineageError:
			*err = x
		case error:
			*err = x
		case parsePanic:
			*err = x.err
		default:
			*err = fmt.Errorf("%v", x)
		}
	}
}

func prepareLineage(expression *Expr, opts LineageOptions) (*Scope, *lineageState) {
	if opts.Dialect == nil {
		opts.Dialect = prototype("")
	}
	if opts.Copy {
		expression = expression.Copy()
	}
	schema := opts.Schema
	if schema == nil {
		schema = NewMappingSchema(nil, nil, opts.Dialect, true, nil)
	}
	scope := opts.Scope
	if scope == nil {
		expression = Qualify(expression, QualifyOptions{
			Dialect:                opts.Dialect,
			Schema:                 schema,
			ValidateQualifyColumns: false,
			Identify:               false,
			Defaults:               true,
		})
		scope = BuildScope(expression)
	}
	if scope == nil {
		panic(&LineageError{Msg: "Cannot build lineage, sql must be SELECT"})
	}
	if !scope.Expression.IsA(KSelectable) {
		panic(&LineageError{Msg: "Cannot build lineage, sql must be a query"})
	}
	return scope, &lineageState{
		dialect:     opts.Dialect,
		trimSelects: opts.TrimSelects,
		schema:      schema,
		cache:       map[lineageCacheKey]*LineageNode{},
		scopeMeta:   map[*Scope]*lineageScopeMeta{},
	}
}

// toNode builds (or reuses from the cache) the lineage node of a column in scope and links it
// under upstream. column is a name, or columnIdx is an index when isIdx.
func (st *lineageState) toNode(column string, columnIdx int, isIdx bool, scope *Scope, scopeName string,
	upstream *LineageNode, sourceName, referenceNodeName string,
) *LineageNode {
	key := lineageCacheKey{column, columnIdx, isIdx, scope, scopeName, sourceName, referenceNodeName}
	if cached, ok := st.cache[key]; ok {
		if upstream != nil {
			upstream.Downstream = append(upstream.Downstream, cached)
		}
		return cached
	}

	selectable := scope.Expression
	var sel *Expr
	if isIdx {
		selects := selectable.Selects()
		if columnIdx >= len(selects) {
			s, _ := selectable.SQL(st.dialect.Name, nil)
			panic(&LineageError{Msg: fmt.Sprintf("Cannot find column's source with index %d in query: %s", columnIdx, s)})
		}
		sel = selects[columnIdx]
	} else {
		meta := st.scopeMeta[scope]
		if meta == nil {
			meta = &lineageScopeMeta{isStar: selectable.IsStar(), selectByName: map[string]*Expr{}}
			for _, s := range selectable.Selects() {
				if _, ok := meta.selectByName[s.AliasOrName()]; !ok {
					meta.selectByName[s.AliasOrName()] = s
				}
			}
			st.scopeMeta[scope] = meta
		}
		if s, ok := meta.selectByName[column]; ok {
			sel = s
		} else if meta.isStar {
			sel = Star()
		} else {
			sel = scope.Expression
		}
	}

	if scope.Expression.IsA(KSubquery) {
		for _, inner := range scope.SubqueryScopes {
			result := st.toNode(column, columnIdx, isIdx, inner, "", upstream, sourceName, referenceNodeName)
			if result != upstream {
				st.cache[key] = result
			}
			return result
		}
	}

	if scope.Expression.IsA(KSetOperation) {
		name := pyUpper(scope.Expression.Kind().Name())
		createdSetop := upstream == nil
		if upstream == nil {
			upstream = &LineageNode{Name: name, Source: scope.Expression, Expression: sel}
		}
		index := -1
		if isIdx {
			index = columnIdx
		} else {
			for i, s := range selectable.Selects() {
				if s.AliasOrName() == column || s.IsStar() {
					index = i
					break
				}
			}
		}
		if index == -1 {
			panic(&ValueError{Msg: fmt.Sprintf("Could not find %s in %s", column, exprSQL(scope.Expression))})
		}
		for _, s := range scope.UnionScopes {
			st.toNode("", index, true, s, "", upstream, sourceName, referenceNodeName)
		}
		if createdSetop {
			st.cache[key] = upstream
		}
		return upstream
	}

	var source *Expr
	if st.trimSelects && scope.Expression.IsA(KSelect) {
		source = SelectReplaceProjections(scope.Expression, sel)
	} else {
		source = scope.Expression
	}

	nodeName := column
	if isIdx {
		nodeName = itoa(columnIdx)
	}
	if scopeName != "" {
		nodeName = scopeName + "." + nodeName
	}
	node := &LineageNode{
		Name:              nodeName,
		Source:            source,
		Expression:        sel,
		SourceName:        sourceName,
		ReferenceNodeName: referenceNodeName,
	}
	if upstream != nil {
		upstream.Downstream = append(upstream.Downstream, node)
	}

	subqueryScopes := map[*Expr]*Scope{}
	for _, ss := range scope.SubqueryScopes {
		subqueryScopes[ss.Expression] = ss
	}
	for subquery := range FindAllInScope(sel, KSelect, KSetOperation) {
		ss := subqueryScopes[subquery]
		if ss == nil {
			continue
		}
		for _, name := range subquery.NamedSelects() {
			st.toNode(name, -1, false, ss, "", node, "", "")
		}
	}

	if sel.IsA(KStar) {
		for _, src := range scope.Sources.Values() {
			srcExpr := src.Table
			if src.Scope != nil {
				srcExpr = src.Scope.Expression
			}
			starName := exprSQLNoComments(sel)
			node.Downstream = append(node.Downstream, &LineageNode{Name: starName, Source: srcExpr, Expression: srcExpr})
		}
	}

	sourceColumns := dedupeExprs(collectSeq(FindAllInScope(sel, KColumn)))

	var derivedTables []*Expr
	if source.IsA(KUDTF) {
		sourceColumns = dedupeExprs(append(sourceColumns, source.FindAllList(KColumn)...))
		for _, src := range scope.Sources.Values() {
			if src.Scope != nil && src.Scope.IsDerivedTable() && src.Scope.Expression.Parent() != nil {
				derivedTables = append(derivedTables, src.Scope.Expression.Parent())
			}
		}
	} else {
		derivedTables = scope.DerivedTables()
	}

	sourceNames := map[string]string{}
	for _, dt := range derivedTables {
		if len(dt.Comments()) > 0 && strings.HasPrefix(dt.Comments()[0], "source: ") {
			fields := strings.Fields(dt.Comments()[0])
			if len(fields) > 1 {
				sourceNames[dt.Alias()] = fields[1]
			}
		}
	}

	// The scope's pivots only form a chain when they all hang off of the same source; otherwise
	// they can't be folded, so their columns degrade to unresolved leaves
	var lastPivot *Expr
	pivotRenames := newOMap[string]()
	pivotColumnMapping := newOMap[[]*Expr]()
	if pivots := scope.Pivots(); len(pivots) > 0 && pivots[0].Parent() == pivots[len(pivots)-1].Parent() {
		lastPivot = pivots[len(pivots)-1]
		pivotRenames, pivotColumnMapping = pivotChainMapping(pivots, scope, st.schema)
	}

	for _, c := range sourceColumns {
		table := c.TableName()
		colSource, hasSource := scope.Sources.Get(table)

		// A column the optimizer could not qualify (e.g. for lack of a schema) is attributed to
		// the sole named source, so the graph doesn't degrade to a Placeholder leaf. Pseudocolumns
		// (e.g. Oracle's LEVEL) are computed by the query itself, so they never belong to a source
		if !hasSource && table == "" && !c.IsA(KPseudocolumn) {
			var namedSources []string
			for _, name := range scope.Sources.Keys() {
				if name != "" {
					namedSources = append(namedSources, name)
				}
			}
			if len(namedSources) == 1 {
				table = namedSources[0]
				colSource, hasSource = scope.Sources.Get(table)
				c.Set("table", ToIdentifier(table, nil))
			}
		}

		if hasSource && colSource.Scope != nil {
			refName := ""
			if colSource.Scope.Type == ScopeDerivedTable {
				if _, ok := sourceNames[table]; !ok {
					refName = table
				}
			} else if colSource.Scope.Type == ScopeCTE {
				if ss, ok := scope.SelectedSources().Get(table); ok && ss.Node != nil {
					refName = ss.Node.Name()
				}
			}
			sn := sourceNames[table]
			if sn == "" {
				sn = sourceName
			}
			st.toNode(c.Name(), -1, false, colSource.Scope, table, node, sn, refName)
		} else if lastPivot != nil && lastPivot.AliasOrName() == c.TableName() {
			// Only the last operator in a chain names the resulting source
			pivot := lastPivot
			var downstreamColumns []*Expr
			columnName := c.Name()
			if cols, ok := pivotColumnMapping.Get(columnName); ok {
				downstreamColumns = append(downstreamColumns, cols...)
			} else {
				pivotParent := pivot.Parent()
				var col any = c.This()
				if r, ok := pivotRenames.Get(c.Name()); ok {
					col = r
				}
				var tbl any
				if pivotParent != nil {
					tbl = pivotParent.AliasOrName()
				}
				downstreamColumns = append(downstreamColumns, lineageColumn(col, tbl))
			}
			for _, dc := range downstreamColumns {
				if dc.TableName() == "" {
					pivotParent := pivot.Parent()
					var tbl any
					if pivotParent != nil {
						tbl = pivotParent.AliasOrName()
					}
					dc = lineageColumn(dc.This(), tbl)
				}
				tbl := dc.TableName()
				cs, ok := scope.Sources.Get(tbl)
				if ok && cs.Table != nil && cs.Table.DbName() == "" {
					if cte, ok2 := scope.CTESources.Get(cs.Table.Name()); ok2 {
						cs = cte
					}
				}
				if ok && cs.Scope != nil {
					sn := sourceNames[tbl]
					if sn == "" {
						sn = sourceName
					}
					st.toNode(dc.Name(), -1, false, cs.Scope, tbl, node, sn, referenceNodeName)
				} else {
					colExpr := cs.Table
					if colExpr == nil {
						colExpr = New(KPlaceholder)
					}
					node.Downstream = append(node.Downstream, &LineageNode{Name: exprSQLNoComments(dc), Source: colExpr, Expression: colExpr})
				}
			}
		} else {
			var colExpr *Expr
			if hasSource {
				colExpr = colSource.Table
			}
			if colExpr == nil {
				colExpr = New(KPlaceholder)
			}
			node.Downstream = append(node.Downstream, &LineageNode{Name: exprSQLNoComments(c), Source: colExpr, Expression: colExpr})
		}
	}

	st.cache[key] = node
	return node
}

func lineageColumn(col any, table any) *Expr {
	var this *Expr
	switch c := col.(type) {
	case string:
		this = ToIdentifier(c, nil)
	case *Expr:
		this = c.Copy()
	}
	var tbl *Expr
	if t, ok := table.(string); ok && t != "" {
		tbl = ToIdentifier(t, nil)
	}
	return New(KColumn, "this", this, "table", tbl)
}

func collectSeq(seq func(func(*Expr) bool)) []*Expr {
	var out []*Expr
	for e := range seq {
		out = append(out, e)
	}
	return out
}

func dedupeExprs(in []*Expr) []*Expr {
	seen := map[uint64][]*Expr{}
	var out []*Expr
outer:
	for _, e := range in {
		h := e.Hash()
		for _, o := range seen[h] {
			if o.Equal(e) {
				continue outer
			}
		}
		seen[h] = append(seen[h], e)
		out = append(out, e)
	}
	return out
}

// prePivotColumns returns the columns the first operator of a (UN)PIVOT chain sees: the
// projections of a derived table or CTE source, or the schema columns of a physical table. It is
// empty when they can't be determined (e.g. an unexpanded star), since anything positional over
// them would silently shift.
func prePivotColumns(pivot *Expr, scope *Scope, schema *MappingSchema) []string {
	parent := pivot.Parent()
	var columns []string
	if parent.IsA(KDerivedTable) && parent.This().IsA(KQuery) {
		columns = parent.This().NamedSelects()
	} else if parent.IsA(KTable) {
		var cteSource Source
		var hasCTE bool
		if parent.DbName() == "" {
			cteSource, hasCTE = scope.CTESources.Get(parent.Name())
		}
		if hasCTE && cteSource.Scope != nil && cteSource.Scope.Expression.IsA(KQuery) {
			columns = cteSource.Scope.Expression.NamedSelects()
		} else if schema != nil {
			columns = schema.ColumnNames(parent, true, nil, nil)
		}
	}
	if containsStr(columns, "*") {
		return nil
	}
	return columns
}

// pivotChainMapping folds a chain of (UN)PIVOT operators into a single view of its output, as
// each one consumes the previous one's columns rather than the pivoted source's. It returns the
// composed output name -> pre-chain name renames (from alias column lists) and the composed
// output name -> source columns it derives from.
func pivotChainMapping(pivots []*Expr, scope *Scope, schema *MappingSchema) (*omap[string], *omap[[]*Expr]) {
	available := prePivotColumns(pivots[0], scope, schema)
	renames := newOMap[string]()
	mapping := newOMap[[]*Expr]()

	for _, pivot := range pivots {
		// Renames are positional over the operator's full output, so they can only be applied
		// when the columns going into it are known
		stepRenames := newOMap[string]()
		if len(pivot.AliasColumnNames()) > 0 && len(available) > 0 {
			stepRenames = PivotOutputColumns(pivot, available)
		}
		stepMapping := pivotColumnMappingOf(pivot)
		if stepRenames.Len() > 0 {
			remapped := newOMap[[]*Expr]()
			for _, post := range stepRenames.Keys() {
				pre, _ := stepRenames.Get(post)
				if cols, ok := stepMapping.Get(pre); ok {
					remapped.Set(post, cols)
				}
			}
			stepMapping = remapped
		}

		// Columns this operator consumed may have been produced by an earlier one, or be
		// alias-list renames of passthroughs; they resolve through what was folded so far
		resolve := func(col *Expr) []*Expr {
			if cols, ok := mapping.Get(col.Name()); ok {
				return cols
			}
			if r, ok := renames.Get(col.Name()); ok {
				return []*Expr{lineageColumn(r, nil)}
			}
			return []*Expr{col}
		}

		composed := newOMap[[]*Expr]()
		consumed := newStrSet()
		for _, out := range stepMapping.Keys() {
			cols, _ := stepMapping.Get(out)
			var resolved []*Expr
			for _, col := range cols {
				consumed.Add(col.Name())
				resolved = append(resolved, resolve(col)...)
			}
			composed.Set(out, resolved)
		}

		// Whatever an earlier operator produced and this one didn't consume passes through,
		// under whatever name this operator's alias column list gives it
		preToPost := map[string]string{}
		for _, post := range stepRenames.Keys() {
			pre, _ := stepRenames.Get(post)
			preToPost[pre] = post
		}
		for _, out := range mapping.Keys() {
			if consumed.Has(out) {
				continue
			}
			name := out
			if post, ok := preToPost[out]; ok {
				name = post
			}
			if !composed.Has(name) {
				cols, _ := mapping.Get(out)
				composed.Set(name, cols)
			}
		}

		if stepRenames.Len() > 0 {
			composedRenames := newOMap[string]()
			for _, post := range stepRenames.Keys() {
				pre, _ := stepRenames.Get(post)
				if r, ok := renames.Get(pre); ok {
					pre = r
				}
				composedRenames.Set(post, pre)
			}
			renames = composedRenames
		}
		mapping = composed
		if len(available) > 0 {
			available = PivotOutputColumns(pivot, available).Keys()
		}
	}

	return renames, mapping
}

// PivotOutputColumns maps each output column name of a PIVOT/UNPIVOT (after alias renames) to the
// column name it has before the renames, given the input column names. It is empty when nothing
// can be derived.
func PivotOutputColumns(p *Expr, prePivotColumns []string) *omap[string] {
	excluded := newStrSet()
	var outputs []string
	if p.ArgB("unpivot") {
		var nameColumns []*Expr
		for _, field := range p.ArgL("fields") {
			if !field.IsA(KIn) {
				continue
			}
			if field.This().IsA(KIdentifier) {
				nameColumns = append(nameColumns, field.This())
			}
			for _, e := range field.Expressions() {
				for c := range e.FindAll(KColumn) {
					excluded.Add(c.OutputName())
				}
			}
		}
		var valueColumns []*Expr
		for _, e := range p.Expressions() {
			items := []*Expr{e}
			if e.IsA(KTuple) {
				items = e.Expressions()
			}
			for _, id := range items {
				if id.IsA(KIdentifier) {
					valueColumns = append(valueColumns, id)
				}
			}
		}
		for _, i := range append(nameColumns, valueColumns...) {
			outputs = append(outputs, i.Name())
		}
	} else {
		for c := range p.FindAll(KColumn) {
			excluded.Add(c.OutputName())
		}
		for _, c := range p.ArgL("columns") {
			outputs = append(outputs, c.OutputName())
		}
		if len(outputs) == 0 {
			for _, c := range p.Expressions() {
				outputs = append(outputs, c.AliasOrName())
			}
		}
	}
	out := newOMap[string]()
	if len(excluded) == 0 || len(outputs) == 0 {
		return out
	}
	var preRename []string
	for _, c := range prePivotColumns {
		if !excluded.Has(c) {
			preRename = append(preRename, c)
		}
	}
	preRename = append(preRename, outputs...)
	postRename := preRename
	if alias := p.ArgE("alias"); alias != nil {
		if renames := alias.ArgL("columns"); len(renames) > 0 {
			var names []string
			for _, r := range renames {
				names = append(names, r.Name())
			}
			if len(names) < len(preRename) {
				postRename = append(names, preRename[len(names):]...)
			} else {
				postRename = names
			}
		}
	}
	for i := 0; i < len(postRename) && i < len(preRename); i++ {
		out.Set(postRename[i], preRename[i])
	}
	return out
}

func pivotColumnMappingOf(pivot *Expr) *omap[[]*Expr] {
	mapping := newOMap[[]*Expr]()
	if pivot.ArgB("unpivot") {
		var valueColumns []*Expr
		for _, e := range pivot.Expressions() {
			valueColumns = append(valueColumns, e.FindAllList(KIdentifier)...)
		}
		for _, vc := range valueColumns {
			mapping.Set(vc.Name(), []*Expr{})
		}
		for _, field := range pivot.ArgL("fields") {
			if !field.IsA(KIn) {
				continue
			}
			nameKey := field.This().Name()
			if !mapping.Has(nameKey) {
				mapping.Set(nameKey, []*Expr{})
			}
			for _, entry := range field.Expressions() {
				entryColumns := entry.FindAllList(KColumn)
				cur, _ := mapping.Get(nameKey)
				mapping.Set(nameKey, append(cur, entryColumns...))
				if len(entryColumns) == len(valueColumns) {
					for i, vc := range valueColumns {
						cur, _ := mapping.Get(vc.Name())
						mapping.Set(vc.Name(), append(cur, entryColumns[i]))
					}
				} else {
					for _, vc := range valueColumns {
						cur, _ := mapping.Get(vc.Name())
						mapping.Set(vc.Name(), append(cur, entryColumns...))
					}
				}
			}
		}
		return mapping
	}
	pivotColumns := pivot.ArgL("columns")
	n := len(pivot.Expressions())
	for i, agg := range pivot.Expressions() {
		aggCols := agg.FindAllList(KColumn)
		for ci := i; ci < len(pivotColumns); ci += n {
			mapping.Set(pivotColumns[ci].Name(), aggCols)
		}
	}
	return mapping
}

func exprSQLNoComments(e *Expr) string {
	s, err := prototype("").Generate(e, &GenerateOptions{NoComments: true})
	if err != nil {
		panic(err)
	}
	return s
}
