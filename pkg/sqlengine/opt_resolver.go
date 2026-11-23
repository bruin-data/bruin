package sqlengine

// Resolver resolves the source table of a column within a scope, using the scope's sources
// and the schema.
type Resolver struct {
	scope   *Scope
	schema  *MappingSchema
	dialect *Dialect

	sourceColumns      *omap[[]string] // nil until computed
	unambiguousColumns *unambiguousColumns
	allColumns         StrSet // nil until computed
	inferSchema        bool

	getSourceColumnsCache    map[resolverSourceKey][]string
	columnTypeFromScopeCache map[resolverTypeKey]*Expr
	outerResolvers           []*Resolver // nil until computed
	selectAliasSet           StrSet      // nil until computed
}

type resolverSourceKey struct {
	name        string
	onlyVisible bool
}

type resolverTypeKey struct {
	source any // *Scope or *Expr (identity)
	name   string
}

// unambiguousColumns maps column names to their source name. It is either a plain map or, when
// there is a single source, a key set that maps every key to singleValue.
type unambiguousColumns struct {
	single      bool
	singleKeys  StrSet
	singleValue string
	m           map[string]string
}

func (u *unambiguousColumns) get(column string) (string, bool) {
	if u.single {
		if u.singleKeys.Has(column) {
			return u.singleValue, true
		}
		return "", false
	}
	v, ok := u.m[column]
	return v, ok
}

// NewResolver returns a Resolver for scope. The dialect is the schema's (default when unset).
func NewResolver(scope *Scope, schema *MappingSchema, inferSchema bool) *Resolver {
	dialect := schema.Dialect()
	if dialect == nil {
		dialect = prototype("")
	}
	return &Resolver{
		scope:                    scope,
		schema:                   schema,
		dialect:                  dialect,
		inferSchema:              inferSchema,
		getSourceColumnsCache:    map[resolverSourceKey][]string{},
		columnTypeFromScopeCache: map[resolverTypeKey]*Expr{},
	}
}

func (r *Resolver) Dialect() *Dialect { return r.dialect }

// GetTable returns the table identifier of a column (or struct root identifier) attached to the
// query AST, or nil if it cannot be found or inferred.
func (r *Resolver) GetTable(column *Expr) *Expr { return r.getTable(column.Name(), column, nil) }

// GetTableByName is GetTable for a plain column name, which can't use the column's position in
// the query (its join or enclosing projection).
func (r *Resolver) GetTableByName(columnName string) *Expr { return r.getTable(columnName, nil, nil) }

// getTable resolves the table of columnName: from the unambiguous columns of sourceColumns (all
// sources when nil), then from the sources available at the column's join, then by projecting it
// back onto the only table function with unknown columns, then (with schema inference) from the
// only source without a known schema. column is nil when a plain name was given. The result is
// the table identifier, or nil if it cannot be found/inferred.
func (r *Resolver) getTable(columnName string, column *Expr, sourceColumns *omap[[]string]) *Expr {
	var joinContext *Expr
	if sourceColumns == nil && column != nil {
		joinContext = r.getColumnJoinContext(column)
	}

	// A SEMI/ANTI join's own table is excluded from the global lookup, so it is skipped here
	tableName, hasTable := "", false
	if joinContext == nil || !joinContext.IsSemiOrAntiJoin() {
		tableName, hasTable = r.getTableNameFromSources(columnName, sourceColumns)
	}

	if (!hasTable || tableName == "") && joinContext != nil {
		// Fall-back case: If we couldn't find the `table_name` from ALL of the sources,
		// attempt to disambiguate the column based on other characteristics e.g if this column is in a join condition,
		// we may be able to disambiguate based on the source order.
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					if _, ok := rec.(*OptimizeError); !ok {
						panic(rec)
					}
				}
			}()
			tableName, hasTable = r.getTableNameFromSources(columnName, r.getAvailableSourceColumns(joinContext, false))
		}()
	}

	if (!hasTable || tableName == "") && sourceColumns == nil && column != nil &&
		!r.AllColumns().Has(columnName) &&
		column.FindAncestor(KSelect) == r.scope.Expression &&
		!(r.dialect.S.TABLES_REFERENCEABLE_AS_COLUMNS && r.scope.SelectedSources().Has(columnName)) {
		// Alias references are left to alias expansion, unless the column is in its own alias
		ancestor := column.FindAncestor(KAlias, KSelect)
		if !r.selectAliases().Has(columnName) || (ancestor.IsA(KAlias) && ancestor.Alias() == columnName) {
			available := r.getAllSourceColumns()
			if joinContext != nil {
				available = r.getAvailableSourceColumns(joinContext, false)
			}
			tableName = r.getUnknownTVFTable(available)
			hasTable = tableName != ""
		}
	}

	if (!hasTable || tableName == "") && r.inferSchema {
		all := r.getAllSourceColumns()
		var sourcesWithoutSchema []string
		for _, source := range all.Keys() {
			columns, _ := all.Get(source)
			if len(columns) == 0 || containsStr(columns, "*") {
				sourcesWithoutSchema = append(sourcesWithoutSchema, source)
			}
		}
		if len(sourcesWithoutSchema) == 1 {
			tableName, hasTable = sourcesWithoutSchema[0], true
		}
	}

	if !hasTable || tableName == "" {
		return nil
	}

	selected, ok := r.scope.SelectedSources().Get(tableName)
	if !ok {
		return ToIdentifier(tableName, nil)
	}

	node := selected.Node

	if node.IsA(KQuery) {
		for node != nil && node.Alias() != tableName && node.Parent() != nil {
			node = node.Parent()
		}
	}

	if nodeAlias := node.ArgE("alias"); nodeAlias != nil {
		return ToIdentifierAny(nodeAlias.Arg("this"), nil, true)
	}

	return ToIdentifier(tableName, nil)
}

// getUnknownTVFTable returns the only table function among sourceColumns whose columns are
// unknown, provided every other source's columns are known and the scope can't be correlated;
// "" otherwise.
func (r *Resolver) getUnknownTVFTable(sourceColumns *omap[[]string]) string {
	if !r.HasUnknownSources() || r.scope.CanBeCorrelated {
		return ""
	}

	var unknownTVFNames []string
	for _, name := range sourceColumns.Keys() {
		columns, _ := sourceColumns.Get(name)
		if len(columns) > 0 && !containsStr(columns, "*") {
			continue
		}
		// Non-TVF tables are expected to have schemas supplied, so only functions can be unknown
		if source, ok := r.scope.Sources.Get(name); ok && source.Table.IsA(KTable) && source.Table.This().IsA(KFunc) {
			unknownTVFNames = append(unknownTVFNames, name)
		} else {
			return ""
		}
	}

	if len(unknownTVFNames) == 1 {
		return unknownTVFNames[0]
	}
	return ""
}

func (r *Resolver) selectAliases() StrSet {
	if r.selectAliasSet == nil {
		r.selectAliasSet = newStrSet()
		if expression := r.scope.Expression; expression.IsA(KSelect) {
			for _, s := range expression.Selects() {
				if s.IsA(KAlias) {
					r.selectAliasSet.Add(s.Alias())
				}
			}
		}
	}
	return r.selectAliasSet
}

// OuterResolvers returns resolvers for the outer scopes a correlated subquery can reference,
// innermost first.
func (r *Resolver) OuterResolvers() []*Resolver {
	if r.outerResolvers == nil {
		r.outerResolvers = []*Resolver{}
		scope := r.scope
		for scope.CanBeCorrelated && scope.Parent != nil {
			scope = scope.Parent
			r.outerResolvers = append(r.outerResolvers, NewResolver(scope, r.schema, r.inferSchema))
		}
	}
	return r.outerResolvers
}

// HasUnknownSources reports whether the columns of some source can't be determined, e.g. a table
// missing from the schema.
func (r *Resolver) HasUnknownSources() bool {
	for _, columns := range r.getAllSourceColumns().Values() {
		if len(columns) == 0 || containsStr(columns, "*") {
			return true
		}
	}
	return false
}

// HasUnknownTVFSources reports whether the columns of some table function source can't be
// determined. Such a source can own any column, e.g. one an outer source also has.
func (r *Resolver) HasUnknownTVFSources() bool {
	return r.hasUnknownTVFSources(map[*Scope]bool{r.scope: true})
}

// scopeHasUnknownTVFSources reports whether a derived table, CTE or set operation scope selects
// unknown columns from a table function; seen holds the scopes already visited.
func (r *Resolver) scopeHasUnknownTVFSources(scope *Scope, seen map[*Scope]bool) bool {
	if seen[scope] {
		return false
	}
	seen[scope] = true
	if len(scope.UnionScopes) > 0 {
		for _, u := range scope.UnionScopes {
			if r.scopeHasUnknownTVFSources(u, seen) {
				return true
			}
		}
		return false
	}
	return NewResolver(scope, r.schema, r.inferSchema).hasUnknownTVFSources(seen)
}

func (r *Resolver) hasUnknownTVFSources(seen map[*Scope]bool) bool {
	all := r.getAllSourceColumns()
	for _, name := range all.Keys() {
		columns, _ := all.Get(name)
		if len(columns) > 0 && !containsStr(columns, "*") {
			continue
		}
		source, ok := r.scope.Sources.Get(name)
		if !ok {
			continue
		}
		if source.Table.IsA(KTable) && source.Table.This().IsA(KFunc) {
			return true
		}
		if source.Scope != nil && r.scopeHasUnknownTVFSources(source.Scope, seen) {
			return true
		}
	}
	return false
}

func (r *Resolver) AllColumns() StrSet {
	if r.allColumns == nil {
		r.allColumns = newStrSet()
		all := r.getAllSourceColumns()
		for _, k := range all.Keys() {
			columns, _ := all.Get(k)
			r.allColumns.Add(columns...)
		}
	}
	return r.allColumns
}

// GetSourceColumnsFromSetOp returns the output columns of a set operation (or of a SELECT
// operand). With a side or kind modifier, LEFT keeps the left columns, FULL takes the union
// and INNER the intersection; an ON column list overrides both.
func (r *Resolver) GetSourceColumnsFromSetOp(expression *Expr) []string {
	if expression.IsA(KSelect) {
		return expression.NamedSelects()
	}
	if expression.IsA(KSubquery) {
		// Different types of SET modifiers can be chained together if they're explicitly grouped by nesting
		return r.GetSourceColumnsFromSetOp(expression.Unnest())
	}
	if !expression.IsA(KSetOperation) {
		panic(&OptimizeError{Msg: "Unknown set operation: " + chunkFPyStr(expression)})
	}

	setOp := expression

	// BigQuery specific set operations modifiers, e.g INNER UNION ALL BY NAME
	onColumnList := setOp.ArgL("on")

	var columns []string
	if len(onColumnList) > 0 {
		// The resulting columns are the columns in the ON clause:
		// {INNER | LEFT | FULL} UNION ALL BY NAME ON (col1, col2, ...)
		for _, col := range onColumnList {
			columns = append(columns, col.Name())
		}
	} else if setOp.SideText() != "" || setOp.KindText() != "" {
		side := setOp.SideText()
		kind := setOp.KindText()

		left := r.GetSourceColumnsFromSetOp(setOp.This())
		right := r.GetSourceColumnsFromSetOp(setOp.Expression())

		if side == "LEFT" {
			columns = left
		} else if side == "FULL" {
			columns = dedupeStrings(append(append([]string{}, left...), right...))
		} else if kind == "INNER" {
			// Set intersection; the result keeps the left-hand order.
			rightSet := newStrSet(right...)
			for _, c := range dedupeStrings(left) {
				if rightSet.Has(c) {
					columns = append(columns, c)
				}
			}
		} else {
			// Any other side/kind combination is unsupported; the message keeps its historical
			// wording.
			panic(&ValueError{Msg: "cannot access local variable 'columns' where it is not associated with a value"})
		}
	} else {
		columns = setOp.NamedSelects()
	}

	return columns
}

// GetSourceColumns returns the columns of the named source (cached), with column aliases of
// the selecting node shadowing the source's own names. It panics with an OptimizeError for an
// unknown source.
func (r *Resolver) GetSourceColumns(name string, onlyVisible bool) []string {
	cacheKey := resolverSourceKey{name, onlyVisible}
	if _, ok := r.getSourceColumnsCache[cacheKey]; !ok {
		source, ok := r.scope.Sources.Get(name)
		if !ok {
			panic(&OptimizeError{Msg: "Unknown table: " + name})
		}

		// A pivoted CTE reference is stored as a Table in the scope sources (see
		// traverseTablesInto in scope.go), but the underlying CTE Scope still holds the
		// column information we need to resolve pre-pivot columns.
		if source.Table != nil && source.Table.DbName() == "" && source.Table.ArgB("pivots") && r.scope.CTESources.Has(source.Table.Name()) {
			source, _ = r.scope.CTESources.Get(source.Table.Name())
		}

		var columns []string
		if source.Table != nil {
			columns = r.schema.ColumnNames(source.Table, onlyVisible, nil, nil)
		} else if sourceExpr := source.Scope.Expression; sourceExpr.IsA(KValues, KUnnest, KLateral) {
			columns = sourceExpr.NamedSelects()

			// in bigquery, unnest structs are automatically scoped as tables, so you can directly select
			// a struct field in a query. This handles the case where the unnest is statically defined.
			if r.dialect.S.UNNEST_COLUMN_ONLY && sourceExpr.IsA(KUnnest) {
				if t := sourceExpr.Type(); t == nil || DataTypeIsType(t, []any{DT_UNKNOWN}, false) {
					unnestExpr := seqGet(sourceExpr.Expressions(), 0)
					if unnestExpr.IsA(KColumn) && r.scope.Parent != nil {
						colType := r.getUnnestColumnType(unnestExpr, r.scope.Parent)
						if colType != nil && DataTypeIsType(colType, []any{DT_ARRAY}, false) {
							elementTypes := colType.Expressions()
							if len(elementTypes) > 0 {
								sourceExpr.SetType(elementTypes[0].Copy())
							}
						} else if colType != nil {
							sourceExpr.SetType(colType.Copy())
						}
					}
				}

				columns = append(columns, r.structFieldNames(sourceExpr.Type())...)
			} else if sourceExpr.IsA(KLateral) && sourceExpr.This().IsA(KExplode) {
				explodeCol := sourceExpr.This().This()

				// If the column is unqualified at this point, it couldn't be resolved when
				// this scope's children were qualified; disambiguating it here would require
				// enumerating this very source's columns, i.e recurse without bound
				if explodeCol.IsA(KColumn) && explodeCol.TableName() != "" && source.Scope.Parent != nil {
					colType := r.getUnnestColumnType(explodeCol, source.Scope.Parent)
					columns = append(columns, r.structFieldNames(colType)...)
				}
			} else if sourceExpr.IsA(KLateral) && sourceExpr.This().IsA(KQuery) {
				columns = sourceExpr.This().NamedSelects()
			}
		} else if source.Scope.Expression.IsA(KSetOperation) {
			columns = r.GetSourceColumnsFromSetOp(source.Scope.Expression)
		} else {
			selectable := chunkFAssertIs(source.Scope.Expression, KSelectable)
			sel := seqGet(selectable.Selects(), 0)

			if sel.IsA(KQueryTransform) {
				// https://spark.apache.org/docs/3.5.1/sql-ref-syntax-qry-select-transform.html
				if schema := sel.ArgE("schema"); schema != nil {
					for _, c := range schema.Expressions() {
						columns = append(columns, c.Name())
					}
				} else {
					columns = []string{"key", "value"}
				}
			} else {
				columns = selectable.NamedSelects()
			}
		}

		var columnAliases []string
		if selected, ok := r.scope.SelectedSources().Get(name); ok && selected.Node != nil {
			columnAliases = selected.Node.AliasColumnNames()
		} else if !ok && r.scope.SemiOrAntiJoinTables().Has(name) {
			// SEMI/ANTI sources are not selected, but their column alias lists still apply
			for _, ref := range r.scope.References() {
				if ref.Name == name {
					columnAliases = ref.Node.AliasColumnNames()
				}
			}
		}

		if len(columnAliases) > 0 {
			// If the source's columns are aliased, their aliases shadow the corresponding column names.
			// This can be expensive if there are lots of columns, so only do this if column_aliases exist.
			n := len(columns)
			if len(columnAliases) > n {
				n = len(columnAliases)
			}
			aliased := make([]string, n)
			for i := range n {
				var colName, alias string
				if i < len(columns) {
					colName = columns[i]
				}
				if i < len(columnAliases) {
					alias = columnAliases[i]
				}
				if alias != "" {
					aliased[i] = alias
				} else {
					aliased[i] = colName
				}
			}
			columns = aliased
		}

		if columns == nil {
			columns = []string{}
		}
		r.getSourceColumnsCache[cacheKey] = columns
	}

	return r.getSourceColumnsCache[cacheKey]
}

func (r *Resolver) getAllSourceColumns() *omap[[]string] {
	if r.sourceColumns == nil {
		r.sourceColumns = newOMap[[]string]()
		selected := r.scope.SelectedSources()
		for _, sourceName := range selected.Keys() {
			r.sourceColumns.Set(sourceName, r.GetSourceColumns(sourceName, false))
		}
		for _, sourceName := range r.scope.LateralSources.Keys() {
			r.sourceColumns.Set(sourceName, r.GetSourceColumns(sourceName, false))
		}
	}
	return r.sourceColumns
}

// getTableNameFromSources returns the source that unambiguously owns columnName, among
// sourceColumns or (when nil) all sources. The bool result is false when no source does.
func (r *Resolver) getTableNameFromSources(columnName string, sourceColumns *omap[[]string]) (string, bool) {
	var unambiguous *unambiguousColumns
	if sourceColumns == nil {
		if r.unambiguousColumns == nil {
			r.unambiguousColumns = r.getUnambiguousColumns(r.getAllSourceColumns())
		}

		unambiguous = r.unambiguousColumns
	} else {
		unambiguous = r.getUnambiguousColumns(sourceColumns)
	}

	return unambiguous.get(columnName)
}

// getColumnJoinContext returns the join a column (or struct root identifier) appears in when
// that join can qualify it based on the source order, or nil.
func (r *Resolver) getColumnJoinContext(column *Expr) *Expr {
	expr := r.scope.Expression

	if !expr.ArgB("joins") || expr.ArgB("laterals") || expr.ArgB("pivots") {
		// Feature gap: We currently don't try to disambiguate columns if other sources
		// (e.g laterals, pivots) exist alongside joins
		return nil
	}

	joinAncestor := column.FindAncestor(KJoin, KSelect)

	if joinAncestor.IsA(KJoin) {
		joinName := joinAncestor.AliasOrName()
		if r.scope.SelectedSources().Has(joinName) || r.scope.SemiOrAntiJoinTables().Has(joinName) {
			// Ensure that the found ancestor is a join that contains an actual source,
			// e.g in Clickhouse `b` is an array expression in `a ARRAY JOIN b`
			return joinAncestor
		}
	}

	return nil
}

// getAvailableSourceColumns returns the source columns that are available at the point where
// a column is referenced: the FROM source and the sources joined up to the given join. An earlier
// SEMI/ANTI join's table is only visible within its own ON clause. With joinGroup, the enclosing
// join group is used instead of the scope, comma joins acting as boundaries.
func (r *Resolver) getAvailableSourceColumns(joinAncestor *Expr, joinGroup bool) *omap[[]string] {
	expr := r.scope.Expression
	if joinGroup {
		expr = joinAncestor.Parent()
	}

	var sources []*Expr
	if from := expr.ArgE("from_"); from != nil {
		sources = append(sources, from.This())
	} else {
		sources = append(sources, expr)
	}

	joins := expr.ArgL("joins")
	end := joinAncestor.Index() + 1
	if end > len(joins) {
		end = len(joins)
	}
	for _, join := range joins[:end] {
		if joinGroup && !(join.ArgB("on") || join.ArgB("using") || join.MethodText() != "" || join.SideText() != "" || join.KindText() != "") {
			sources = sources[:0]
		}

		if join == joinAncestor || !join.IsSemiOrAntiJoin() {
			sources = append(sources, join.This())
		}
	}

	availableSources := newOMap[[]string]()
	for len(sources) > 0 {
		source := sources[len(sources)-1]
		sources = sources[:len(sources)-1]
		if name := source.AliasOrName(); name != "" {
			availableSources.Set(name, r.GetSourceColumns(name, false))
		} else if source.IsA(KSubquery) {
			sources = append(sources, source.This())
			for _, join := range source.This().ArgL("joins") {
				if !join.IsSemiOrAntiJoin() {
					sources = append(sources, join.This())
				}
			}
		}
	}

	return availableSources
}

// getUnambiguousColumns returns a mapping of column name to source name for every column that
// appears in exactly one source.
func (r *Resolver) getUnambiguousColumns(sourceColumns *omap[[]string]) *unambiguousColumns {
	if sourceColumns.Len() == 0 {
		return &unambiguousColumns{m: map[string]string{}}
	}

	keys := sourceColumns.Keys()
	firstTable := keys[0]
	firstColumns, _ := sourceColumns.Get(firstTable)

	if len(keys) == 1 {
		// Performance optimization - avoid copying firstColumns if there is only one table.
		return &unambiguousColumns{single: true, singleKeys: newStrSet(firstColumns...), singleValue: firstTable}
	}

	// For BigQuery UNNEST_COLUMN_ONLY, build a mapping of original UNNEST aliases
	// from alias.columns[0] to their source names. This is used to resolve shadowing
	// where an UNNEST alias shadows a column name from another table.
	unnestOriginalAliases := map[string]string{}
	if r.dialect.S.UNNEST_COLUMN_ONLY {
		for _, sourceName := range r.scope.Sources.Keys() {
			source, _ := r.scope.Sources.Get(sourceName)
			var sourceExpr *Expr
			if source.Scope != nil {
				sourceExpr = source.Scope.Expression
			} else {
				sourceExpr = source.Table.Expression()
			}
			if sourceExpr.IsA(KUnnest) {
				if aliasArg := sourceExpr.ArgE("alias"); aliasArg != nil && len(aliasArg.ArgL("columns")) > 0 {
					unnestOriginalAliases[aliasArg.ArgL("columns")[0].Name()] = sourceName
				}
			}
		}
	}

	unambiguous := map[string]string{}
	for _, col := range firstColumns {
		unambiguous[col] = firstTable
	}
	allColumns := newStrSet()
	for col := range unambiguous {
		allColumns.Add(col)
	}

	for _, table := range keys[1:] {
		columns, _ := sourceColumns.Get(table)
		unique := newStrSet(columns...)
		ambiguous := newStrSet()
		for c := range unique {
			if allColumns.Has(c) {
				ambiguous.Add(c)
			}
		}
		allColumns.Add(columns...)

		for column := range ambiguous {
			if a, ok := unnestOriginalAliases[column]; ok {
				unambiguous[column] = a
				continue
			}

			delete(unambiguous, column)
		}
		for column := range unique {
			if !ambiguous.Has(column) {
				unambiguous[column] = table
			}
		}
	}

	return &unambiguousColumns{m: unambiguous}
}

func (r *Resolver) structFieldNames(colType *Expr) []string {
	if colType != nil && DataTypeIsType(colType, []any{DT_ARRAY}, false) {
		colType = seqGet(colType.Expressions(), 0)
	}

	if colType != nil && DataTypeIsType(colType, []any{DT_STRUCT}, false) {
		out := []string{}
		for _, k := range colType.Expressions() {
			out = append(out, k.Name())
		}
		return out
	}
	return []string{}
}

// getUnnestColumnType returns the type of a column being unnested/exploded, tracing through
// CTEs/subqueries to find the base table.
func (r *Resolver) getUnnestColumnType(column *Expr, scope *Scope) *Expr {
	var tableName string
	if column.TableName() != "" {
		tableName = column.TableName()
	} else {
		// use the parent scope's resolver to disambiguate the column
		parentResolver := NewResolver(scope, r.schema, r.inferSchema)
		tableIdentifier := parentResolver.GetTable(column)
		if tableIdentifier == nil {
			return nil
		}
		tableName = tableIdentifier.Name()
	}

	source, ok := scope.Sources.Get(tableName)
	if !ok {
		return nil
	}
	return r.getColumnTypeFromScope(source, column)
}

// getColumnTypeFromScope returns a column's type traced through scopes/tables to the base
// table (nil if not found).
func (r *Resolver) getColumnTypeFromScope(source Source, column *Expr) *Expr {
	// A single source can be reachable through many paths of a scope DAG (e.g. a CTE
	// referenced by several other CTEs). The schema and scope are immutable during
	// qualification, so the type of `column` under `source` depends only on
	// `(source, column name)`; memoize it to walk each source once.
	cacheKey := resolverTypeKey{source.id(), column.Name()}
	if cached, ok := r.columnTypeFromScopeCache[cacheKey]; ok {
		return cached
	}

	// nil is a valid result if the DataType could not be determined!
	var result *Expr
	if source.Table != nil {
		colType := r.schema.GetColumnType(source.Table, column, "", nil, nil)
		if colType != nil && !DataTypeIsType(colType, []any{DT_UNKNOWN}, false) {
			result = colType
		}
	} else if source.Scope != nil {
		for _, nestedSource := range source.Scope.Sources.Values() {
			nestedType := r.getColumnTypeFromScope(nestedSource, column)
			if nestedType != nil && !DataTypeIsType(nestedType, []any{DT_UNKNOWN}, false) {
				result = nestedType
				break
			}
		}
	}

	r.columnTypeFromScopeCache[cacheKey] = result
	return result
}

func containsStr(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func indexOfStr(list []string, s string) int {
	for i, x := range list {
		if x == s {
			return i
		}
	}
	return -1
}

func dedupeStrings(items []string) []string {
	seen := newStrSet()
	out := []string{}
	for _, x := range items {
		if !seen.Has(x) {
			seen.Add(x)
			out = append(out, x)
		}
	}
	return out
}
