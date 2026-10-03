package sqlengine

// Port of sqlglot/optimizer/resolver.py.

// Resolver mirrors sqlglot.optimizer.resolver.Resolver: a helper for resolving columns.
type Resolver struct {
	scope   *Scope
	schema  *MappingSchema
	dialect *Dialect

	sourceColumns      *omap[[]string] // nil = None
	unambiguousColumns *unambiguousColumns
	allColumns         StrSet // nil = None
	inferSchema        bool

	getSourceColumnsCache    map[resolverSourceKey][]string
	columnTypeFromScopeCache map[resolverTypeKey]*Expr
}

type resolverSourceKey struct {
	name        string
	onlyVisible bool
}

type resolverTypeKey struct {
	source any // *Scope or *Expr (identity)
	name   string
}

// unambiguousColumns mirrors the Mapping[str, str] returned by _get_unambiguous_columns
// (either a plain dict or a SingleValuedMapping).
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

// NewResolver mirrors Resolver(scope, schema, infer_schema).
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

// Dialect returns the resolver dialect.
func (r *Resolver) Dialect() *Dialect { return r.dialect }

// GetTable mirrors Resolver.get_table(column) for a Column expression.
func (r *Resolver) GetTable(column *Expr) *Expr { return r.getTable(column.Name(), column) }

// GetTableByName mirrors Resolver.get_table(column_name) for a string.
func (r *Resolver) GetTableByName(columnName string) *Expr { return r.getTable(columnName, nil) }

// getTable mirrors Resolver.get_table. column is nil when a plain name was given.
// The result is the table identifier, or nil if it cannot be found/inferred.
func (r *Resolver) getTable(columnName string, column *Expr) *Expr {
	tableName, hasTable := r.getTableNameFromSources(columnName, nil)

	if (!hasTable || tableName == "") && column.IsA(KColumn) {
		// Fall-back case: If we couldn't find the `table_name` from ALL of the sources,
		// attempt to disambiguate the column based on other characteristics e.g if this column is in a join condition,
		// we may be able to disambiguate based on the source order.
		if joinContext := r.getColumnJoinContext(column); joinContext != nil {
			// In this case, the return value will be the join that _may_ be able to disambiguate the column
			// and we can use the source columns available at that join to get the table name
			// catch OptimizeError if column is still ambiguous and try to resolve with schema inference below
			func() {
				defer func() {
					if rec := recover(); rec != nil {
						if _, ok := rec.(*OptimizeError); !ok {
							panic(rec)
						}
					}
				}()
				tableName, hasTable = r.getTableNameFromSources(columnName, r.getAvailableSourceColumns(joinContext))
			}()
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

	selected, ok := r.scope.SelectedSources().Get(tableName)
	if !hasTable || !ok {
		if !hasTable {
			return nil
		}
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

// AllColumns mirrors Resolver.all_columns: all available columns of all sources in this scope.
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

// GetSourceColumnsFromSetOp mirrors Resolver.get_source_columns_from_set_op.
func (r *Resolver) GetSourceColumnsFromSetOp(expression *Expr) []string {
	if expression.IsA(KSelect) {
		return expression.NamedSelects()
	}
	if expression.IsA(KSubquery) && expression.This().IsA(KSetOperation) {
		// Different types of SET modifiers can be chained together if they're explicitly grouped by nesting
		return r.GetSourceColumnsFromSetOp(expression.This())
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

		// Visit the children UNIONs (if any) in a post-order traversal
		left := r.GetSourceColumnsFromSetOp(setOp.This())
		right := r.GetSourceColumnsFromSetOp(setOp.Expression())

		// We use dict.fromkeys to deduplicate keys and maintain insertion order
		if side == "LEFT" {
			columns = left
		} else if side == "FULL" {
			columns = dedupeStrings(append(append([]string{}, left...), right...))
		} else if kind == "INNER" {
			// Python computes a set intersection here (unordered); keep the left-hand order.
			rightSet := newStrSet(right...)
			for _, c := range dedupeStrings(left) {
				if rightSet.Has(c) {
					columns = append(columns, c)
				}
			}
		} else {
			panic(&ValueError{Msg: "cannot access local variable 'columns' where it is not associated with a value"})
		}
	} else {
		columns = setOp.NamedSelects()
	}

	return columns
}

// GetSourceColumns mirrors Resolver.get_source_columns(name, only_visible).
func (r *Resolver) GetSourceColumns(name string, onlyVisible bool) []string {
	cacheKey := resolverSourceKey{name, onlyVisible}
	if _, ok := r.getSourceColumnsCache[cacheKey]; !ok {
		source, ok := r.scope.Sources.Get(name)
		if !ok {
			panic(&OptimizeError{Msg: "Unknown table: " + name})
		}

		// A pivoted CTE reference is stored as an exp.Table in the scope sources (see
		// _traverse_tables in scope.py), but the underlying CTE Scope still holds the
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
		}

		if len(columnAliases) > 0 {
			// If the source's columns are aliased, their aliases shadow the corresponding column names.
			// This can be expensive if there are lots of columns, so only do this if column_aliases exist.
			n := len(columns)
			if len(columnAliases) > n {
				n = len(columnAliases)
			}
			aliased := make([]string, n)
			for i := 0; i < n; i++ {
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

// getTableNameFromSources mirrors Resolver._get_table_name_from_sources. The bool result is false
// when Python would return None.
func (r *Resolver) getTableNameFromSources(columnName string, sourceColumns *omap[[]string]) (string, bool) {
	var unambiguous *unambiguousColumns
	if sourceColumns.Len() == 0 {
		// If not supplied, get all sources to calculate unambiguous columns
		if r.unambiguousColumns == nil {
			r.unambiguousColumns = r.getUnambiguousColumns(r.getAllSourceColumns())
		}

		unambiguous = r.unambiguousColumns
	} else {
		unambiguous = r.getUnambiguousColumns(sourceColumns)
	}

	return unambiguous.get(columnName)
}

// getColumnJoinContext mirrors Resolver._get_column_join_context: check if a column participating
// in a join can be qualified based on the source order.
func (r *Resolver) getColumnJoinContext(column *Expr) *Expr {
	expr := r.scope.Expression

	if !expr.ArgB("joins") || expr.ArgB("laterals") || expr.ArgB("pivots") {
		// Feature gap: We currently don't try to disambiguate columns if other sources
		// (e.g laterals, pivots) exist alongside joins
		return nil
	}

	joinAncestor := column.FindAncestor(KJoin, KSelect)

	if joinAncestor.IsA(KJoin) && r.scope.SelectedSources().Has(joinAncestor.AliasOrName()) {
		// Ensure that the found ancestor is a join that contains an actual source,
		// e.g in Clickhouse `b` is an array expression in `a ARRAY JOIN b`
		return joinAncestor
	}

	return nil
}

// getAvailableSourceColumns mirrors Resolver._get_available_source_columns: the source columns
// that are available at the point where a column is referenced (FROM + joins up to the given join).
func (r *Resolver) getAvailableSourceColumns(joinAncestor *Expr) *omap[[]string] {
	expr := r.scope.Expression

	// Collect tables in order: FROM clause tables + joined tables up to current join
	from := expr.ArgE("from_")
	if from == nil {
		panic(&ValueError{Msg: "'from_'"})
	}
	fromName := from.AliasOrName()
	availableSources := newOMap[[]string]()
	availableSources.Set(fromName, r.GetSourceColumns(fromName, false))

	joins := expr.ArgL("joins")
	end := joinAncestor.Index() + 1
	if end > len(joins) {
		end = len(joins)
	}
	for _, join := range joins[:end] {
		availableSources.Set(join.AliasOrName(), r.GetSourceColumns(join.AliasOrName(), false))
	}

	return availableSources
}

// getUnambiguousColumns mirrors Resolver._get_unambiguous_columns: a mapping of column name to
// source name for every column that appears in exactly one source.
func (r *Resolver) getUnambiguousColumns(sourceColumns *omap[[]string]) *unambiguousColumns {
	if sourceColumns.Len() == 0 {
		return &unambiguousColumns{m: map[string]string{}}
	}

	keys := sourceColumns.Keys()
	firstTable := keys[0]
	firstColumns, _ := sourceColumns.Get(firstTable)

	if len(keys) == 1 {
		// Performance optimization - avoid copying first_columns if there is only one table.
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

// structFieldNames mirrors Resolver._struct_field_names.
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

// getUnnestColumnType mirrors Resolver._get_unnest_column_type: the type of a column being
// unnested/exploded, tracing through CTEs/subqueries to find the base table.
func (r *Resolver) getUnnestColumnType(column *Expr, scope *Scope) *Expr {
	// if column is qualified, use that table, otherwise disambiguate using the resolver
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

// getColumnTypeFromScope mirrors Resolver._get_column_type_from_scope: a column's type traced
// through scopes/tables to the base table (nil if not found).
func (r *Resolver) getColumnTypeFromScope(source Source, column *Expr) *Expr {
	// A single source can be reachable through many paths of a scope DAG (e.g. a CTE
	// referenced by several other CTEs). The schema and scope are immutable during
	// qualification, so the type of `column` under `source` depends only on
	// `(source, column name)`; memoize it to walk each source once.
	cacheKey := resolverTypeKey{source.id(), column.Name()}
	if cached, ok := r.columnTypeFromScopeCache[cacheKey]; ok {
		return cached
	}

	// None is a valid result if DataType could not be determined!
	var result *Expr
	if source.Table != nil {
		// base table - get the column type from schema
		colType := r.schema.GetColumnType(source.Table, column, "", nil, nil)
		if colType != nil && !DataTypeIsType(colType, []any{DT_UNKNOWN}, false) {
			result = colType
		}
	} else if source.Scope != nil {
		// iterate over all sources in the scope
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

// dedupeStrings mirrors list(dict.fromkeys(items)).
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
