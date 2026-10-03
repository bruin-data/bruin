package sqlengine

import (
	"fmt"
	"sync"
	"time"
	"unicode/utf8"
)

// Port of sqlglot/optimizer/annotate_types.py.
//
// Python values typed `exp.DataType | exp.DType | None` are represented as `any` holding a
// *Expr (DataType), a DType or nil. Use annT to turn a possibly-nil *Expr into such a value
// (a typed nil *Expr must never be stored in an `any`).

// BIGINT_EXTRACT_DATE_PARTS: EXTRACT/DATE_PART specifiers that return BIGINT instead of INT
var BIGINT_EXTRACT_DATE_PARTS = newStrSet(
	"EPOCH_SECOND",
	"EPOCH_MILLISECOND",
	"EPOCH_MICROSECOND",
	"EPOCH_NANOSECOND",
	"NANOSECOND",
)

// AnnotateOptions mirrors the keyword arguments of annotate_types.
type AnnotateOptions struct {
	// Schema is used as-is when set (ensure_schema returns Schema instances unchanged, so
	// Dialect is then ignored for the schema, exactly like Python).
	Schema *MappingSchema
	// SchemaMap is a raw nested mapping schema (Python dict), wrapped in a MappingSchema.
	SchemaMap *SchemaMap
	// Dialect is used when constructing the MappingSchema.
	Dialect *Dialect
	// ExpressionMetadata maps expression classes to annotators (default: the dialect's).
	ExpressionMetadata ExprMetadataType
	// CoercesTo maps a type to the set of types it can be coerced into (default: the dialect's).
	CoercesTo map[DType]DTypeSet
	// OverwriteTypes re-annotates already annotated nodes. nil means True (Python's default).
	OverwriteTypes *bool
}

// AnnotateTypes mirrors annotate_types(expression, schema, expression_metadata, coerces_to,
// dialect, overwrite_types): infers the types of an expression, annotating its AST accordingly.
func AnnotateTypes(expression *Expr, o AnnotateOptions) *Expr {
	schema := annEnsureSchema(o.Schema, o.SchemaMap, o.Dialect)
	overwriteTypes := o.OverwriteTypes == nil || *o.OverwriteTypes

	a := newTypeAnnotator(schema, o.ExpressionMetadata, o.CoercesTo, nil, overwriteTypes)
	if expression != nil {
		// Every node ends up visited; presize the set.
		a.visited = make(map[*Expr]struct{}, expression.nodeCount())
	}
	return a.annotate(expression, true)
}

// annotateTypes mirrors annotate_types(expression, dialect=d) (used by generator/dialect code).
func annotateTypes(e *Expr, d *Dialect) *Expr {
	return AnnotateTypes(e, AnnotateOptions{Dialect: d})
}

// annEnsureSchema mirrors sqlglot.schema.ensure_schema(schema, dialect=dialect).
func annEnsureSchema(schema *MappingSchema, mapping *SchemaMap, dialect *Dialect) *MappingSchema {
	if schema != nil {
		return schema
	}
	return NewMappingSchema(mapping, nil, dialect, true, nil)
}

func annCoerceDateLiteral(l *Expr, unit *Expr) DType {
	dateText := l.Name()
	isISODate := annIsISODate(dateText)

	if isISODate && annIsDateUnit(unit) {
		return DT_DATE
	}

	// An ISO date is also an ISO datetime, but not vice versa
	if isISODate || annIsISODatetime(dateText) {
		return DT_DATETIME
	}

	return DT_UNKNOWN
}

func annCoerceDate(l *Expr, unit *Expr) any {
	if !annIsDateUnit(unit) {
		return DT_DATETIME
	}
	if t := l.Type(); t != nil {
		return t.Arg("this")
	}
	return DT_UNKNOWN
}

// annBinaryCoercion mirrors BinaryCoercionFunc: (left, right) -> DataType | DType | None.
type annBinaryCoercion func(l, r *Expr) any

func swapArgs(fn annBinaryCoercion) annBinaryCoercion {
	return func(l, r *Expr) any { return fn(r, l) }
}

func swapAll(coercions map[[2]DType]annBinaryCoercion) map[[2]DType]annBinaryCoercion {
	out := make(map[[2]DType]annBinaryCoercion, 2*len(coercions))
	for k, fn := range coercions {
		out[k] = fn
	}
	for k, fn := range coercions {
		out[[2]DType{k[1], k[0]}] = swapArgs(fn)
	}
	return out
}

func annBuildCoercesTo() map[DType]DTypeSet {
	// Highest-to-lowest type precedence, as specified in Spark's docs (ANSI):
	// https://spark.apache.org/docs/3.2.0/sql-ref-ansi-compliance.html
	textPrecedence := []DType{DT_TEXT, DT_NVARCHAR, DT_VARCHAR, DT_NCHAR, DT_CHAR}
	numericPrecedence := []DType{
		DT_DECFLOAT, DT_DOUBLE, DT_FLOAT, DT_BIGDECIMAL, DT_DECIMAL, DT_BIGINT, DT_INT, DT_SMALLINT, DT_TINYINT,
	}
	timelikePrecedence := []DType{DT_TIMESTAMPLTZ, DT_TIMESTAMPTZ, DT_TIMESTAMP, DT_DATETIME, DT_DATE}

	result := map[DType]DTypeSet{}
	for _, typePrecedence := range [][]DType{textPrecedence, numericPrecedence, timelikePrecedence} {
		var coercesTo DTypeSet
		for _, dataType := range typePrecedence {
			result[dataType] = coercesTo
			coercesTo = coercesTo.With(dataType)
		}
	}
	return result
}

func annCopyCoercesTo(m map[DType]DTypeSet) map[DType]DTypeSet {
	out := make(map[DType]DTypeSet, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Coercion tables. sqlglot builds these at import time and BigQuery mutates the base table in
// place when its dialect module is imported:
//
//	COERCES_TO = {**TypeAnnotator.COERCES_TO, BIGDECIMAL: {DOUBLE}}   # shallow copy
//	COERCES_TO[DECIMAL] |= {BIGDECIMAL}                               # mutates the shared sets
//	COERCES_TO[BIGINT] |= {BIGDECIMAL}
//	COERCES_TO[VARCHAR] |= {DATE, DATETIME, TIME, TIMESTAMP, TIMESTAMPTZ}
//
// so TypeAnnotator.COERCES_TO depends on whether BigQuery has been loaded. We mirror that by
// checking whether the BigQuery dialect has been instantiated (the Go analog of importing the
// module). Hive and Databricks deep-copy the base table when they are imported; we use the
// pristine base table for them (i.e. assume they were loaded before BigQuery).
var (
	annCoercesOnce            sync.Once
	annBaseCoercesTo          map[DType]DTypeSet // _COERCES_TO before BigQuery is imported
	annBaseCoercesToBQ        map[DType]DTypeSet // _COERCES_TO after BigQuery is imported
	annBigQueryCoercesTo      map[DType]DTypeSet
	annHiveCoercesTo          map[DType]DTypeSet
	annDatabricksCoercesTo    map[DType]DTypeSet
	annBinaryCoercionsDefault map[[2]DType]annBinaryCoercion
)

func annInitCoercions() {
	annCoercesOnce.Do(func() {
		annBaseCoercesTo = annBuildCoercesTo()

		// sqlglot/dialects/bigquery.py
		mutated := annCopyCoercesTo(annBaseCoercesTo)
		mutated[DT_DECIMAL] = mutated[DT_DECIMAL].With(DT_BIGDECIMAL)
		mutated[DT_BIGINT] = mutated[DT_BIGINT].With(DT_BIGDECIMAL)
		mutated[DT_VARCHAR] = mutated[DT_VARCHAR].With(DT_DATE, DT_DATETIME, DT_TIME, DT_TIMESTAMP, DT_TIMESTAMPTZ)
		annBaseCoercesToBQ = mutated
		annBigQueryCoercesTo = annCopyCoercesTo(mutated)
		annBigQueryCoercesTo[DT_BIGDECIMAL] = newDTypeSet(DT_DOUBLE)

		// sqlglot/dialects/hive.py: support only the non-ANSI mode (default for Hive, Spark2, Spark)
		annHiveCoercesTo = annCopyCoercesTo(annBaseCoercesTo)
		for _, targetType := range DataType_NUMERIC_TYPES.Union(DataType_TEMPORAL_TYPES).With(DT_INTERVAL).Items() {
			annHiveCoercesTo[targetType] = annHiveCoercesTo[targetType].Union(DataType_TEXT_TYPES)
		}

		// sqlglot/dialects/databricks.py
		annDatabricksCoercesTo = annCopyCoercesTo(annBaseCoercesTo)
		for _, textType := range DataType_TEXT_TYPES.Items() {
			annDatabricksCoercesTo[textType] = annDatabricksCoercesTo[textType].
				Union(DataType_NUMERIC_TYPES).
				Union(DataType_TEMPORAL_TYPES).
				With(DT_BINARY, DT_BOOLEAN, DT_INTERVAL)
		}

		annBinaryCoercionsDefault = annBuildBinaryCoercions()
	})
}

// annBigQueryLoaded reports whether the BigQuery dialect has been instantiated.
func annBigQueryLoaded() bool {
	dialectMu.Lock()
	defer dialectMu.Unlock()
	_, ok := dialectCache["bigquery"]
	return ok
}

// typeAnnotatorCoercesTo mirrors TypeAnnotator.COERCES_TO (see the comment on the tables above).
func typeAnnotatorCoercesTo() map[DType]DTypeSet {
	annInitCoercions()
	if annBigQueryLoaded() {
		return annBaseCoercesToBQ
	}
	return annBaseCoercesTo
}

// coercesToFor mirrors the dialect class attribute COERCES_TO (nil for the empty base dict).
func coercesToFor(d *Dialect) map[DType]DTypeSet {
	annInitCoercions()
	for _, name := range append([]string{d.Name}, d.parents...) {
		switch name {
		case "bigquery":
			return annBigQueryCoercesTo
		case "databricks":
			return annDatabricksCoercesTo
		case "hive":
			return annHiveCoercesTo
		}
	}
	return nil
}

// annBuildBinaryCoercions mirrors TypeAnnotator.BINARY_COERCIONS.
func annBuildBinaryCoercions() map[[2]DType]annBinaryCoercion {
	out := map[[2]DType]annBinaryCoercion{}

	textInterval := map[[2]DType]annBinaryCoercion{}
	for _, t := range DataType_TEXT_TYPES.Items() {
		textInterval[[2]DType{t, DT_INTERVAL}] = func(l, r *Expr) any { return annCoerceDateLiteral(l, r.ArgE("unit")) }
	}
	for k, v := range swapAll(textInterval) {
		out[k] = v
	}

	textNumeric := map[[2]DType]annBinaryCoercion{}
	for _, text := range DataType_TEXT_TYPES.Items() {
		for _, numeric := range DataType_NUMERIC_TYPES.Items() {
			// text + numeric will yield the numeric type to match most dialects' semantics
			textNumeric[[2]DType{text, numeric}] = func(l, r *Expr) any {
				// Python: `l.type if l.type in exp.DataType.NUMERIC_TYPES else r.type`. l.type is a
				// DataType and NUMERIC_TYPES holds DType members, so the membership test is always
				// False and the right operand's type is returned.
				return annT(r.Type())
			}
		}
	}
	for k, v := range swapAll(textNumeric) {
		out[k] = v
	}

	dateInterval := map[[2]DType]annBinaryCoercion{
		{DT_DATE, DT_INTERVAL}: func(l, r *Expr) any { return annCoerceDate(l, r.ArgE("unit")) },
	}
	for k, v := range swapAll(dateInterval) {
		out[k] = v
	}
	return out
}

// TypeAnnotator mirrors sqlglot.optimizer.annotate_types.TypeAnnotator.
type TypeAnnotator struct {
	schema             *MappingSchema
	dialect            *Dialect
	expressionMetadata ExprMetadataType
	coercesTo          map[DType]DTypeSet
	binaryCoercions    map[[2]DType]annBinaryCoercion

	// Caches the annotated sub-expressions, to ensure we only visit them once
	visited map[*Expr]struct{}

	// Caches NULL-annotated expressions to set them to UNKNOWN after type inference is completed
	nullExpressions *annExprSet

	// Databricks and Spark ≥v3 actually support NULL (i.e., VOID) as a type
	supportsNullType bool

	// Maps an exp.SetOperation (e.g. UNION) to its projection types. This is computed if the
	// exp.SetOperation is the expression of a scope source, as selecting from it multiple times
	// would reprocess the entire subtree to coerce the types of its operands' projections
	setopColumnTypes map[*Expr]*omap[any]

	// When set to False, this enables partial annotation by skipping already-annotated nodes
	overwriteTypes bool

	// Maps (Scope, source_name) to its column projections and types
	scopeSourceSelects map[annScopeSourceKey]*omap[any]
}

type annScopeSourceKey struct {
	scope *Scope
	name  string
}

// newTypeAnnotator mirrors TypeAnnotator(schema, expression_metadata, coerces_to,
// binary_coercions, overwrite_types). Empty/nil optional arguments fall back to the dialect's
// tables, like Python's `x or default`.
func newTypeAnnotator(schema *MappingSchema, expressionMetadata ExprMetadataType, coercesTo map[DType]DTypeSet,
	binaryCoercions map[[2]DType]annBinaryCoercion, overwriteTypes bool,
) *TypeAnnotator {
	annInitCoercions()
	if schema == nil {
		schema = annEnsureSchema(nil, nil, nil)
	}
	dialect := schema.Dialect()
	if dialect == nil {
		dialect = prototype("")
	}
	a := &TypeAnnotator{
		schema:             schema,
		dialect:            dialect,
		expressionMetadata: expressionMetadata,
		coercesTo:          coercesTo,
		binaryCoercions:    binaryCoercions,
		visited:            map[*Expr]struct{}{},
		nullExpressions:    newAnnExprSet(),
		supportsNullType:   dialect.S.SUPPORTS_NULL_TYPE,
		setopColumnTypes:   map[*Expr]*omap[any]{},
		overwriteTypes:     overwriteTypes,
		scopeSourceSelects: map[annScopeSourceKey]*omap[any]{},
	}
	if len(a.expressionMetadata) == 0 {
		a.expressionMetadata = expressionMetadataFor(dialect)
	}
	if len(a.coercesTo) == 0 {
		a.coercesTo = coercesToFor(dialect)
		if len(a.coercesTo) == 0 {
			a.coercesTo = typeAnnotatorCoercesTo()
		}
	}
	if len(a.binaryCoercions) == 0 {
		a.binaryCoercions = annBinaryCoercionsDefault
	}
	return a
}

// Schema returns the annotator's schema.
func (a *TypeAnnotator) Schema() *MappingSchema { return a.schema }

// Dialect returns the annotator's dialect.
func (a *TypeAnnotator) Dialect() *Dialect { return a.dialect }

// Clear mirrors TypeAnnotator.clear() (exported for the Simplifier's annotator interface).
func (a *TypeAnnotator) Clear() { a.clear() }

// Annotate mirrors TypeAnnotator.annotate(expression, annotate_scope) (exported for the
// Simplifier's annotator interface).
func (a *TypeAnnotator) Annotate(expression *Expr, annotateScope bool) *Expr {
	return a.annotate(expression, annotateScope)
}

func init() {
	// Simplifier: TypeAnnotator(schema=ensure_schema(None, dialect=self.dialect), overwrite_types=False)
	smpNewTypeAnnotator = func(d *Dialect) smpTypeAnnotator {
		return newTypeAnnotator(annEnsureSchema(nil, nil, d), nil, nil, nil, false)
	}
}

func (a *TypeAnnotator) clear() {
	a.visited = map[*Expr]struct{}{}
	a.nullExpressions = newAnnExprSet()
	a.setopColumnTypes = map[*Expr]*omap[any]{}
	a.scopeSourceSelects = map[annScopeSourceKey]*omap[any]{}
}

// uncache evicts `expression` (or its subtree, if `deep`) from the annotation caches. This must
// be called when an already-annotated tree is about to be mutated, so that a subsequent
// annotation pass doesn't skip it.
func (a *TypeAnnotator) uncache(expression *Expr, deep bool) {
	nodes := []*Expr{expression}
	if deep {
		nodes = nodes[:0]
		for n := range expression.Walk(true, nil) {
			nodes = append(nodes, n)
		}
	}
	for _, node := range nodes {
		delete(a.visited, node)
		a.nullExpressions.delete(node)
		delete(a.setopColumnTypes, node)
	}
}

// setType mirrors TypeAnnotator._set_type. targetType is nil, a DType or a DataType *Expr.
func (a *TypeAnnotator) setType(expression *Expr, targetType any) *Expr {
	prevType := expression.Type()

	var dtype *Expr
	switch t := targetType.(type) {
	case nil:
		dtype = NewDataType(DT_UNKNOWN)
	case DType:
		dtype = NewDataType(t)
	case *Expr:
		switch {
		case t == nil:
			dtype = NewDataType(DT_UNKNOWN)
		case t.IsA(KDataType):
			dtype = t
		default:
			panic(&annAttributeError{Msg: fmt.Sprintf("'%s' object has no attribute 'into_expr'", t.Kind().Name())})
		}
	default:
		panic(&annAttributeError{Msg: fmt.Sprintf("'%T' object has no attribute 'into_expr'", targetType)})
	}
	expression.SetType(dtype)
	a.visited[expression] = struct{}{}

	if !a.supportsNullType && expression.Type().Arg("this") == DT_NULL {
		a.nullExpressions.add(expression)
	} else if prevType != nil && prevType.Arg("this") == DT_NULL {
		a.nullExpressions.delete(expression)
	}

	return expression
}

// annotate mirrors TypeAnnotator.annotate(expression, annotate_scope).
func (a *TypeAnnotator) annotate(expression *Expr, annotateScope bool) *Expr {
	// This flag is used to avoid costly scope traversals when we only care about annotating
	// non-column expressions (partial type inference), e.g., when simplifying in the optimizer
	if annotateScope {
		for _, scope := range TraverseScope(expression) {
			a.annotateScope(scope)
		}
	}

	// This takes care of non-traversable expressions
	a.annotateExpression(expression, nil)

	// Replace NULL type with the default type of the targeted dialect, since the former is not an actual type;
	// it is mostly used to aid type coercion, e.g. in query set operations.
	for _, expr := range a.nullExpressions.values() {
		a.setType(expr, a.dialect.S.DEFAULT_NULL_TYPE)
	}

	return expression
}

// annSourceExpression mirrors `source.expression` for a scope source (Scope.expression, or the
// `expression` property of a non-Table source expression).
func annSourceExpression(src Source) *Expr {
	if src.Scope != nil {
		return src.Scope.Expression
	}
	return src.Table.Expression()
}

func (a *TypeAnnotator) getScopeSourceSelects(scope *Scope, sourceName string) *omap[any] {
	key := annScopeSourceKey{scope, sourceName}
	selects, ok := a.scopeSourceSelects[key]

	if !ok {
		selects = newOMap[any]()
		source, hasSource := scope.Sources.Get(sourceName)

		if hasSource && source.Scope != nil {
			expression := source.Scope.Expression

			if expression.IsA(KUDTF) {
				var values []*Expr

				if expression.IsA(KLateral) {
					if expression.This().IsA(KExplode) {
						values = []*Expr{expression.This().This()}
					}
				} else if expression.IsA(KUnnest) {
					values = []*Expr{expression}
				} else if !expression.IsA(KTableFromRows) {
					values = seqGet(expression.Expressions(), 0).Expressions()
				}

				if len(values) == 0 {
					return newOMap[any]()
				}

				aliasColumnNames := expression.AliasColumnNames()

				var expType *Expr
				if expression.IsA(KUnnest) {
					expType = expression.Type()
				} else if expression.IsA(KLateral) && expression.This().IsA(KExplode) {
					expType = expression.This().Type()
				}

				var structType *Expr
				if expType != nil && expType.IsTypeOf(DT_STRUCT) {
					structType = expType
				}

				if structType != nil {
					for _, colDef := range structType.Expressions() {
						if colDef.IsA(KColumnDef) && colDef.ArgE("kind") != nil {
							selects.Set(colDef.Name(), colDef.ArgE("kind"))
						}
					}
				} else {
					for i := 0; i < len(aliasColumnNames) && i < len(values); i++ {
						selects.Set(aliasColumnNames[i], annT(values[i].Type()))
					}
				}
			} else if expression.IsA(KSetOperation) &&
				len(expression.This().Selects()) == len(expression.Expression().Selects()) {
				selects = a.getSetopColumnTypes(expression)
			} else if expression.IsA(KSelectable) {
				for _, s := range expression.Selects() {
					if s.Type() != nil {
						selects.Set(s.AliasOrName(), s.Type())
					}
				}
			}
		} else {
			var pivots []*Expr
			if hasSource && source.Table.IsA(KTable) {
				pivots = source.Table.ArgL("pivots")
			} else {
				pivots = scope.Pivots()
			}
			for _, pivot := range pivots {
				if pivot.AliasOrName() == sourceName {
					parent := pivot.Parent()
					var parentSource Source
					hasParentSource := false
					if parent != nil {
						parentSource, hasParentSource = scope.Sources.Get(parent.AliasOrName())
					}

					var srcTypes *omap[any]
					if parent != nil && hasParentSource && parentSource.Scope != nil {
						srcTypes = a.getScopeSourceSelects(scope, parent.AliasOrName())
					} else if parent.IsA(KTable) {
						srcTypes = a.schema.Find(parent, false, true)
						if srcTypes.Len() == 0 {
							srcTypes = newOMap[any]()
						}
					} else {
						srcTypes = newOMap[any]()
					}

					if pivot.ArgB("unpivot") {
						selects = a.getUnpivotColumnTypes(pivot, srcTypes)
					} else {
						selects = a.getPivotColumnTypes(pivot, srcTypes)
					}
					break
				}
			}
		}

		a.scopeSourceSelects[key] = selects
	}

	return selects
}

// annotateScope mirrors TypeAnnotator.annotate_scope.
func (a *TypeAnnotator) annotateScope(scope *Scope) {
	// isinstance(self.schema, MappingSchema) always holds in the Go port.
	for _, tableColumn := range scope.TableColumns() {
		source, _ := scope.Sources.Get(tableColumn.Name())

		if source.Table.IsA(KTable) {
			schema := a.schema.Find(source.Table, false, true)
			if schema == nil {
				continue
			}

			columnDefs := make([]*Expr, 0, schema.Len())
			for _, c := range schema.Keys() {
				v, _ := schema.Get(c)
				kind, _ := v.(*Expr)
				columnDefs = append(columnDefs, New(KColumnDef, "this", ToIdentifier(c, nil), "kind", kind))
			}
			structType := New(KDataType, "this", DT_STRUCT, "expressions", columnDefs, "nested", true)
			a.setType(tableColumn, structType)
		} else if source.Scope != nil && source.Scope.Expression.IsA(KQuery) &&
			annQueryTypeOrUnknown(source.Scope.Expression).IsTypeOf(DT_STRUCT) {
			queryType, _ := source.Scope.Expression.MetaGet("query_type").(*Expr)
			a.setType(tableColumn, annT(queryType))
		}
	}

	// Iterate through all the expressions of the current scope in post-order, and annotate
	a.annotateExpression(scope.Expression, scope)
	a.fixupOrderByAliases(scope)

	if a.dialect.S.QUERY_RESULTS_ARE_STRUCTS && scope.Expression.IsA(KQuery) {
		selects := scope.Expression.Selects()
		columnDefs := make([]*Expr, 0, len(selects))
		for _, sel := range selects {
			var kind *Expr
			if t := sel.Type(); t != nil {
				kind = t.Copy()
			}
			columnDefs = append(columnDefs, New(KColumnDef, "this", ToIdentifier(sel.OutputName(), nil), "kind", kind))
		}
		structType := New(KDataType, "this", DT_STRUCT, "expressions", columnDefs, "nested", true)

		anyUnknown := false
		for _, cd := range structType.Expressions() {
			if kind := cd.ArgE("kind"); kind != nil && kind.IsTypeOf(DT_UNKNOWN) {
				anyUnknown = true
				break
			}
		}
		if !anyUnknown {
			// We don't use `_set_type` on purpose here. If we annotated the query directly, then
			// using it in other contexts (e.g., ARRAY(<query>)) could result in incorrect type
			// annotations, i.e., it shouldn't be interpreted as a STRUCT value.
			scope.Expression.Meta()["query_type"] = structType
		}
	}
}

// annQueryTypeOrUnknown mirrors `expression.meta_get("query_type") or exp.DType.UNKNOWN.into_expr()`.
func annQueryTypeOrUnknown(e *Expr) *Expr {
	if qt, ok := e.MetaGet("query_type").(*Expr); ok && qt != nil {
		return qt
	}
	return NewDataType(DT_UNKNOWN)
}

type annStackItem struct {
	expr              *Expr
	childrenAnnotated bool
}

// annotateExpression mirrors TypeAnnotator._annotate_expression. scope may be nil.
func (a *TypeAnnotator) annotateExpression(expression *Expr, scope *Scope) {
	stack := []annStackItem{{expression, false}}

	for len(stack) > 0 {
		item := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		expr := item.expr

		if _, ok := a.visited[expr]; ok ||
			(!a.overwriteTypes && expr.Type() != nil && !expr.IsTypeOf(DT_UNKNOWN)) {
			continue // We've already inferred the expression's type
		}

		if !item.childrenAnnotated {
			stack = append(stack, annStackItem{expr, true})
			for _, childExpr := range expr.IterExpressions(false) {
				stack = append(stack, annStackItem{childExpr, false})
			}
			continue
		}

		if scope != nil && expr.IsA(KColumn) && expr.TableName() != "" {
			var source Source
			found := false
			sourceScope := scope
			for sourceScope != nil && !found {
				source, found = sourceScope.Sources.Get(expr.TableName())
				if !found {
					sourceScope = sourceScope.Parent
				}
			}

			if found && source.Table.IsA(KTable) {
				var tableColType any = a.schema.GetColumnType(source.Table, expr, "", nil, nil)
				if dt, ok := tableColType.(*Expr); ok && dt.IsTypeOf(DT_UNKNOWN) && source.Table.ArgB("pivots") {
					tableColType = DT_UNKNOWN
					ss := sourceScope
					if ss == nil {
						ss = scope
					}
					if t, _ := a.getScopeSourceSelects(ss, expr.TableName()).Get(expr.Name()); t != nil {
						tableColType = t
					}
				}

				a.setType(expr, tableColType)
			} else if found && sourceScope != nil {
				colType, _ := a.getScopeSourceSelects(sourceScope, expr.TableName()).Get(expr.Name())
				if colType != nil {
					a.setType(expr, colType)
				} else if srcExpr := annSourceExpression(source); srcExpr.IsA(KUnnest) {
					a.setType(expr, annT(srcExpr.Type()))
				} else {
					a.setType(expr, DT_UNKNOWN)
				}
			} else if colType := a.annPivotColType(found, scope, expr); colType != nil {
				a.setType(expr, colType)
			} else {
				a.setType(expr, DT_UNKNOWN)
			}

			if expr.IsTypeOf(DT_JSON) {
				if dotParts, _ := expr.MetaGet("dot_parts").([]string); len(dotParts) > 0 {
					// JSON dot access is case sensitive across all dialects, so we need to undo the normalization.
					i := 0
					parent := expr.Parent()
					for parent.IsA(KDot) {
						if i >= len(dotParts) {
							panic(&ValueError{Msg: "StopIteration"})
						}
						parent.Expression().Replace(ToIdentifier(dotParts[i], boolp(true)))
						i++
						parent = parent.Parent()
					}

					delete(expr.Meta(), "dot_parts")
				}
			}

			if t := expr.Type(); t != nil {
				if nullable, ok := t.Arg("nullable").(bool); ok && !nullable {
					expr.Meta()["nonnull"] = true
				}
			}
			continue
		}

		spec := a.expressionMetadata[expr.Kind()]

		if spec != nil && spec.Annotator != nil {
			spec.Annotator(a, expr)
		} else if spec != nil && spec.Returns != nil {
			a.setType(expr, spec.Returns)
		} else {
			a.setType(expr, DT_UNKNOWN)
		}
	}
}

// annPivotColType mirrors the `not source and scope.pivots and (col_type := ...)` branch of
// _annotate_expression, returning nil when it does not apply.
func (a *TypeAnnotator) annPivotColType(found bool, scope *Scope, expr *Expr) any {
	if found || len(scope.Pivots()) == 0 {
		return nil
	}
	colType, _ := a.getScopeSourceSelects(scope, expr.TableName()).Get(expr.Name())
	return colType
}

func (a *TypeAnnotator) fixupOrderByAliases(scope *Scope) {
	query := scope.Expression
	if !query.IsA(KQuery) {
		return
	}

	order := query.ArgE("order")
	if order == nil {
		return
	}

	// Build alias -> type map from fully-annotated projections (last match wins,
	// consistent with how _expand_alias_refs handles duplicate aliases).
	aliasTypes := map[string]*Expr{}
	for _, sel := range query.Selects() {
		if sel.IsA(KAlias) && sel.This().Type() != nil && !sel.This().IsTypeOf(DT_UNKNOWN) {
			aliasTypes[sel.Alias()] = sel.This().Type()
		}
	}

	if len(aliasTypes) == 0 {
		return
	}

	for _, ordered := range order.Expressions() {
		var aliasCols []*Expr
		for c := range ordered.FindAll(KColumn) {
			if _, ok := aliasTypes[c.Name()]; ok && c.TableName() == "" {
				aliasCols = append(aliasCols, c)
			}
		}
		for _, col := range aliasCols {
			a.setType(col, aliasTypes[col.Name()])
		}

		if len(aliasCols) > 0 {
			for node := range ordered.Walk(true, func(n *Expr) bool { return n.IsA(KSubquery) }) {
				if !node.IsA(KColumn, KLiteral) {
					delete(a.visited, node)
				}
			}
			a.annotateExpression(ordered, scope)
		}
	}
}

// maybeCoerce mirrors TypeAnnotator._maybe_coerce: returns type2 if type1 can be coerced into
// it, otherwise type1.
//
// If either type is parameterized (e.g. DECIMAL(18, 2) contains two parameters), we assume
// type1 does not coerce into type2, so we also return it in this case.
func (a *TypeAnnotator) maybeCoerce(type1, type2 any) any {
	var type1Value, type2Value any
	if dt, ok := type1.(*Expr); ok && dt != nil {
		if len(dt.Expressions()) > 0 {
			return dt
		}
		type1Value = dt.Arg("this")
	} else if !ok {
		type1Value = type1
	}

	if dt, ok := type2.(*Expr); ok && dt != nil {
		if len(dt.Expressions()) > 0 {
			return dt
		}
		type2Value = dt.Arg("this")
	} else if !ok {
		type2Value = type2
	}

	// We propagate the UNKNOWN type upwards if found
	if type1Value == DT_UNKNOWN || type2Value == DT_UNKNOWN {
		return DT_UNKNOWN
	}

	if type1Value == DT_NULL {
		return type2Value
	}
	if type2Value == DT_NULL {
		return type1Value
	}

	if d1, ok := type1Value.(DType); ok {
		if d2, ok := type2Value.(DType); ok {
			if set, ok := a.coercesTo[d1]; ok && set.Has(d2) {
				return type2Value
			}
		}
	}
	return type1Value
}

// getSetopColumnTypes mirrors TypeAnnotator._get_setop_column_types: computes the coerced column
// types for a SetOperation (UNION, INTERSECT, EXCEPT, ...), coercing types across left and right
// operands for all projections.
func (a *TypeAnnotator) getSetopColumnTypes(setop *Expr) *omap[any] {
	if cached, ok := a.setopColumnTypes[setop]; ok {
		return cached
	}

	colTypes := newOMap[any]()

	// Validate that left and right have same number of projections
	if !(setop.IsA(KSetOperation) &&
		len(setop.This().Selects()) > 0 &&
		len(setop.Expression().Selects()) > 0 &&
		len(setop.This().Selects()) == len(setop.Expression().Selects())) {
		return colTypes
	}

	// Process a chain / sub-tree of set operations
	for setOp := range setop.Walk(true, func(n *Expr) bool { return !n.IsA(KSetOperation, KSubquery) }) {
		if !setOp.IsA(KSetOperation) {
			continue
		}

		setopCols := newOMap[any]()
		if setOp.ArgB("by_name") {
			rTypeBySelect := newOMap[any]()
			for _, s := range setOp.Expression().Selects() {
				rTypeBySelect.Set(s.AliasOrName(), annT(s.Type()))
			}
			for _, s := range setOp.This().Selects() {
				rType, _ := rTypeBySelect.Get(s.AliasOrName())
				if rType == nil {
					rType = DT_UNKNOWN
				}
				setopCols.Set(s.AliasOrName(), a.maybeCoerce(annT(s.Type()), rType))
			}
		} else {
			ls, rs := setOp.This().Selects(), setOp.Expression().Selects()
			for i := 0; i < len(ls) && i < len(rs); i++ {
				setopCols.Set(ls[i].AliasOrName(), a.maybeCoerce(annT(ls[i].Type()), annT(rs[i].Type())))
			}
		}

		// Coerce intermediate results with the previously registered types, if they exist
		for _, colName := range setopCols.Keys() {
			colType, _ := setopCols.Get(colName)
			prev, ok := colTypes.Get(colName)
			if !ok {
				prev = DT_NULL
			}
			colTypes.Set(colName, a.maybeCoerce(colType, prev))
		}
	}

	a.setopColumnTypes[setop] = colTypes
	return colTypes
}

func (a *TypeAnnotator) getUnpivotColumnTypes(pivot *Expr, srcTypes *omap[any]) *omap[any] {
	newTypes := newOMap[any]()

	for _, field := range pivot.ArgL("fields") {
		fieldCol := field.This()
		first := seqGet(field.Expressions(), 0)

		var inSrc *Expr
		if aliasNode := first.ArgE("alias"); first.IsA(KPivotAlias) && aliasNode != nil {
			newTypes.Set(fieldCol.Name(), annT(aliasNode.Type()))
			inSrc = first.This()
		} else {
			newTypes.Set(fieldCol.Name(), NewDataType(DT_VARCHAR))
			inSrc = first
		}

		inCols := []*Expr{inSrc}
		if inSrc.IsA(KTuple) {
			inCols = inSrc.Expressions()
		}
		valExpr := seqGet(pivot.Expressions(), 0)
		valCols := []*Expr{valExpr}
		if valExpr.IsA(KTuple) {
			valCols = valExpr.Expressions()
		}
		for i := 0; i < len(valCols) && i < len(inCols); i++ {
			newTypes.Set(valCols[i].OutputName(), annT(inCols[i].Type()))
		}
	}

	out := newOMap[any]()
	for _, name := range PivotOutputColumns(pivot, srcTypes.Keys()).Keys() {
		t, _ := newTypes.Get(name)
		if t == nil {
			t, _ = srcTypes.Get(name)
		}
		if t != nil {
			out.Set(name, t)
		}
	}
	return out
}

func (a *TypeAnnotator) getPivotColumnTypes(pivot *Expr, srcTypes *omap[any]) *omap[any] {
	firstField := seqGet(pivot.ArgL("fields"), 0)
	if !firstField.IsA(KIn) {
		panic(&OptimizeError{Msg: fmt.Sprintf("Expected In expression for pivot field, got %s", annPyTypeRepr(firstField))})
	}

	pivotConstants := firstField.Expressions()

	// The first agg_cols_offset entries are source columns that pass through the PIVOT unchanged;
	// the rest are the aggregated columns, one per combination of IN value and aggregate function.
	outputToSrc := PivotOutputColumns(pivot, srcTypes.Keys())

	var aggTypes []any
	for _, agg := range pivot.Expressions() {
		if agg.IsA(KAlias) {
			aggTypes = append(aggTypes, annT(agg.This().Type()))
		} else {
			aggTypes = append(aggTypes, annT(agg.Type()))
		}
	}
	aggColsOffset := outputToSrc.Len() - len(pivotConstants)*len(aggTypes)
	if aggColsOffset < 0 {
		panic(&OptimizeError{Msg: fmt.Sprintf("Negative pivot column offset: %d", aggColsOffset)})
	}

	outputNames := outputToSrc.Keys()
	newTypes := newOMap[any]()

	for _, name := range outputNames[:aggColsOffset] {
		srcName, _ := outputToSrc.Get(name)
		if t, _ := srcTypes.Get(srcName); t != nil {
			newTypes.Set(name, t)
		}
	}

	rest := outputNames[aggColsOffset:]
	i := 0
zip:
	for range pivotConstants {
		for _, aggType := range aggTypes {
			if i >= len(rest) {
				break zip
			}
			if aggType != nil {
				newTypes.Set(rest[i], aggType)
			}
			i++
		}
	}

	return newTypes
}

func (a *TypeAnnotator) annotateBinary(expression *Expr) *Expr {
	left, right := expression.Left(), expression.Right()
	if left == nil || right == nil {
		// sqlglot logs "Failed to annotate badly formed binary expression" here.
		a.setType(expression, nil)
		return expression
	}

	leftType, lok := left.Type().Arg("this").(DType)
	rightType, rok := right.Type().Arg("this").(DType)

	if expression.IsA(KConnector, KPredicate) {
		a.setType(expression, DT_BOOLEAN)
	} else if fn, ok := a.binaryCoercions[[2]DType{leftType, rightType}]; ok && lok && rok {
		a.setType(expression, fn(left, right))
	} else {
		a.annotateByArgs(expression, left, right)
	}

	if expression.IsA(KIs) || (left.MetaGet("nonnull") == true && right.MetaGet("nonnull") == true) {
		expression.Meta()["nonnull"] = true
	}

	return expression
}

func (a *TypeAnnotator) annotateUnary(expression *Expr) *Expr {
	if expression.IsA(KNot) {
		a.setType(expression, DT_BOOLEAN)
	} else {
		a.setType(expression, annT(expression.This().Type()))
	}

	if expression.This().MetaGet("nonnull") == true {
		expression.Meta()["nonnull"] = true
	}

	return expression
}

func (a *TypeAnnotator) annotateLiteral(expression *Expr) *Expr {
	if expression.IsString() {
		a.setType(expression, DT_VARCHAR)
	} else if expression.IsInt() {
		a.setType(expression, DT_INT)
	} else {
		a.setType(expression, DT_DOUBLE)
	}

	expression.Meta()["nonnull"] = true

	return expression
}

// annotateByArgs mirrors TypeAnnotator._annotate_by_args(expression, *args) with
// promote=False and array=False. Each arg is an argument key (string) or an expression.
func (a *TypeAnnotator) annotateByArgs(expression *Expr, args ...any) *Expr {
	return a.annotateByArgsFull(expression, false, false, args...)
}

// annotateByArgsFull mirrors TypeAnnotator._annotate_by_args(expression, *args, promote, array).
func (a *TypeAnnotator) annotateByArgsFull(expression *Expr, promote bool, array bool, args ...any) *Expr {
	var literalType, nonLiteralType any
	var nestedType *Expr

	for _, arg := range args {
		var expressions []*Expr
		switch x := arg.(type) {
		case string:
			expressions = annEnsureList(expression.Arg(x))
		default:
			expressions = annEnsureList(arg)
		}

		for _, expr := range expressions {
			exprType := expr.Type()

			if exprType.IsTypeOf(DT_UNKNOWN) {
				a.setType(expression, DT_UNKNOWN)
				return expression
			}

			if nestedType != nil {
				continue
			}

			// Stop coercing at the first nested data type found
			if exprType.ArgB("nested") {
				nestedType = exprType
			} else if expr.IsA(KLiteral) {
				first := literalType
				if first == nil {
					first = annT(exprType)
				}
				literalType = a.maybeCoerce(first, annT(exprType))
			} else {
				first := nonLiteralType
				if first == nil {
					first = annT(exprType)
				}
				nonLiteralType = a.maybeCoerce(first, annT(exprType))
			}
		}
	}

	var resultType any

	if nestedType != nil {
		resultType = nestedType
	} else if literalType != nil && nonLiteralType != nil {
		if a.dialect.S.PRIORITIZE_NON_LITERAL_TYPES {
			literalThisType := annThisOfType(literalType)
			nonLiteralThisType := annThisOfType(nonLiteralType)
			if (annDTypeIn(literalThisType, DataType_INTEGER_TYPES) && annDTypeIn(nonLiteralThisType, DataType_INTEGER_TYPES)) ||
				(annDTypeIn(literalThisType, DataType_REAL_TYPES) && annDTypeIn(nonLiteralThisType, DataType_REAL_TYPES)) {
				resultType = nonLiteralType
			}
		}
	} else {
		switch {
		case literalType != nil:
			resultType = literalType
		case nonLiteralType != nil:
			resultType = nonLiteralType
		default:
			resultType = DT_UNKNOWN
		}
	}

	if resultType == nil {
		resultType = a.maybeCoerce(nonLiteralType, literalType)
	}
	a.setType(expression, resultType)

	if promote {
		if annDTypeIn(expression.Type().Arg("this"), DataType_INTEGER_TYPES) {
			a.setType(expression, DT_BIGINT)
		} else if annDTypeIn(expression.Type().Arg("this"), DataType_FLOAT_TYPES) {
			a.setType(expression, DT_DOUBLE)
		}
	}

	if array {
		a.setType(expression, New(KDataType, "this", DT_ARRAY, "expressions", []*Expr{expression.Type()}, "nested", true))
	}

	return expression
}

func (a *TypeAnnotator) annotateTimeunit(expression *Expr) *Expr {
	var datatype any
	thisType := expression.This().Type().Arg("this")
	if annDTypeIn(thisType, DataType_TEXT_TYPES) {
		datatype = annCoerceDateLiteral(expression.This(), expression.ArgE("unit"))
	} else if annDTypeIn(thisType, DataType_TEMPORAL_TYPES) {
		datatype = annCoerceDate(expression.This(), expression.ArgE("unit"))
	} else {
		datatype = DT_UNKNOWN
	}

	a.setType(expression, datatype)
	return expression
}

func (a *TypeAnnotator) annotateBracket(expression *Expr) *Expr {
	bracketArg := expression.Expressions()[0]
	this := expression.This()

	if bracketArg.IsA(KSlice) {
		a.setType(expression, annT(this.Type()))
	} else if this.Type().IsTypeOf(DT_ARRAY) {
		a.setType(expression, annT(seqGet(this.Type().Expressions(), 0)))
	} else if index := annIndexOf(annMapKeys(this), bracketArg); this.IsA(KMap, KVarMap) && index >= 0 {
		value := seqGet(annMapValues(this), index)
		if value != nil {
			a.setType(expression, annT(value.Type()))
		} else {
			a.setType(expression, nil)
		}
	} else {
		a.setType(expression, DT_UNKNOWN)
	}

	return expression
}

func (a *TypeAnnotator) annotateDiv(expression *Expr) *Expr {
	leftType, rightType := expression.Left().Type().Arg("this"), expression.Right().Type().Arg("this")

	if expression.ArgB("typed") &&
		annDTypeIn(leftType, DataType_INTEGER_TYPES) &&
		annDTypeIn(rightType, DataType_INTEGER_TYPES) {
		a.setType(expression, DT_BIGINT)
	} else {
		a.setType(expression, a.maybeCoerce(leftType, rightType))
		if t := expression.Type(); t != nil && !annDTypeIn(t.Arg("this"), DataType_REAL_TYPES) {
			a.setType(expression, a.maybeCoerce(t, DT_DOUBLE))
		}
	}

	return expression
}

func (a *TypeAnnotator) annotateDot(expression *Expr) *Expr {
	a.setType(expression, nil)

	// Propagate type from qualified UDF calls (e.g., db.my_udf(...))
	if expression.Expression().IsA(KAnonymous) {
		a.setType(expression, annT(expression.Expression().Type()))
		return expression
	}

	thisType := expression.argAttr("this", "type").Type()

	if thisType != nil && thisType.IsTypeOf(DT_STRUCT) {
		for _, e := range thisType.Expressions() {
			if e.Name() == expression.Expression().Name() {
				a.setType(expression, annT(annColumnDefKind(e)))
				break
			}
		}
	}

	return expression
}

func (a *TypeAnnotator) annotateExplode(expression *Expr) *Expr {
	a.setType(expression, annT(seqGet(expression.This().Type().Expressions(), 0)))
	return expression
}

func (a *TypeAnnotator) annotateUnnest(expression *Expr) *Expr {
	child := seqGet(expression.Expressions(), 0)

	var exprType any
	if child != nil && child.IsTypeOf(DT_ARRAY) {
		exprType = annT(seqGet(child.Type().Expressions(), 0))
	}

	a.setType(expression, exprType)
	return expression
}

func (a *TypeAnnotator) annotateSubquery(expression *Expr) *Expr {
	// For scalar subqueries (subqueries with a single projection), infer the type
	// from that single projection. This allows type propagation in cases like:
	// SELECT (SELECT 1 AS c) AS c
	query := expression.UnnestSubquery()

	if query.IsA(KQuery) {
		selects := query.Selects()
		if len(selects) == 1 {
			a.setType(expression, annT(selects[0].Type()))
			return expression
		}
	}

	a.setType(expression, DT_UNKNOWN)
	return expression
}

// annotateStructValue mirrors TypeAnnotator._annotate_struct_value; it returns a DataType, a
// ColumnDef or nil (None).
func (a *TypeAnnotator) annotateStructValue(expression *Expr) *Expr {
	// Case: STRUCT(key AS value)
	var this *Expr
	kind := expression.Type()

	if alias := expression.ArgE("alias"); alias != nil {
		this = alias.Copy()
	} else if expression.Expression() != nil {
		// Case: STRUCT(key = value) or STRUCT(key := value)
		this = expression.This().Copy()
		kind = expression.Expression().Type()
	} else if expression.IsA(KColumn) {
		// Case: STRUCT(c)
		this = expression.This().Copy()
	}

	if kind != nil && kind.IsTypeOf(DT_UNKNOWN) {
		return nil
	}

	if this != nil {
		return New(KColumnDef, "this", this, "kind", kind)
	}

	return kind
}

func (a *TypeAnnotator) annotateStruct(expression *Expr) *Expr {
	var expressions []*Expr
	for _, expr := range expression.Expressions() {
		structFieldType := a.annotateStructValue(expr)
		if structFieldType == nil {
			a.setType(expression, nil)
			return expression
		}

		expressions = append(expressions, structFieldType)
	}

	a.setType(expression, New(KDataType, "this", DT_STRUCT, "expressions", expressions, "nested", true))
	return expression
}

func (a *TypeAnnotator) annotateMap(expression *Expr) *Expr {
	keys, _ := expression.Arg("keys").(*Expr)
	values, _ := expression.Arg("values").(*Expr)

	mapType := New(KDataType, "this", DT_MAP)
	if keys.IsA(KArray) && values.IsA(KArray) {
		var keyType any = DT_UNKNOWN
		if t := seqGet(keys.Type().Expressions(), 0); t != nil {
			keyType = t
		}
		var valueType any = DT_UNKNOWN
		if t := seqGet(values.Type().Expressions(), 0); t != nil {
			valueType = t
		}

		// A DataType never compares equal to a DType, so this only checks that both types exist.
		if keyType != DT_UNKNOWN && valueType != DT_UNKNOWN {
			mapType.Set("expressions", []*Expr{keyType.(*Expr), valueType.(*Expr)})
			mapType.Set("nested", true)
		}
	}

	a.setType(expression, mapType)
	return expression
}

func (a *TypeAnnotator) annotateToMap(expression *Expr) *Expr {
	mapType := New(KDataType, "this", DT_MAP)
	arg := expression.This()
	if arg.IsTypeOf(DT_STRUCT) {
		for _, coldef := range arg.Type().Expressions() {
			kind := annColumnDefKind(coldef)
			// `kind != exp.DType.UNKNOWN` always holds: kind is a DataType or None, never a DType.
			mapType.Set("expressions", []*Expr{NewDataType(DT_VARCHAR), kind})
			mapType.Set("nested", true)
			break
		}
	}

	a.setType(expression, mapType)
	return expression
}

func (a *TypeAnnotator) annotateExtract(expression *Expr) *Expr {
	part := expression.Name()
	switch {
	case part == "TIME":
		a.setType(expression, DT_TIME)
	case part == "DATE":
		a.setType(expression, DT_DATE)
	case BIGINT_EXTRACT_DATE_PARTS.Has(part):
		a.setType(expression, DT_BIGINT)
	default:
		a.setType(expression, DT_INT)
	}
	return expression
}

func (a *TypeAnnotator) annotateWithinGroup(expression *Expr) *Expr {
	if expression.This().IsA(KPercentileDisc) {
		order := expression.ArgE("expression")
		var orderExpressions []*Expr
		if order != nil {
			orderExpressions = order.Expressions()
		}
		var sortType any = DT_UNKNOWN
		if len(orderExpressions) > 0 {
			sortType = annT(orderExpressions[0].This().Type())
		}
		a.setType(expression, sortType)
		return expression
	}

	return a.annotateByArgs(expression, "this")
}

func (a *TypeAnnotator) annotateByArrayElement(expression *Expr) *Expr {
	arrayArg := expression.This()
	if arrayArg.Type().IsTypeOf(DT_ARRAY) {
		var elementType any = DT_UNKNOWN
		if t := seqGet(arrayArg.Type().Expressions(), 0); t != nil {
			elementType = t
		}
		a.setType(expression, elementType)
	} else {
		a.setType(expression, DT_UNKNOWN)
	}

	return expression
}

// ---------------------------------------------------------------------------------------------
// Helpers (not direct ports).

// annT converts a possibly-nil DataType pointer into a `DataType | DType | None` value.
func annT(dt *Expr) any {
	if dt == nil {
		return nil
	}
	return dt
}

// annThisOfType mirrors `t.this if isinstance(t, exp.DataType) else t`.
func annThisOfType(t any) any {
	if dt, ok := t.(*Expr); ok && dt != nil {
		return dt.Arg("this")
	}
	return t
}

// annDTypeIn mirrors `value in SOME_DTYPE_SET` for a value that may not be a DType.
func annDTypeIn(v any, set DTypeSet) bool {
	d, ok := v.(DType)
	return ok && set.Has(d)
}

// annEnsureList mirrors sqlglot.helper.ensure_list for argument values.
func annEnsureList(v any) []*Expr {
	switch x := v.(type) {
	case nil:
		return nil
	case *Expr:
		if x == nil {
			return nil
		}
		return []*Expr{x}
	case []*Expr:
		out := make([]*Expr, 0, len(x))
		for _, e := range x {
			if e != nil {
				out = append(out, e)
			}
		}
		return out
	}
	return nil
}

// annColumnDefKind mirrors the `kind` property access on a struct field (ColumnDef.kind); other
// expressions (e.g. unnamed DataType fields) have no such attribute in Python.
func annColumnDefKind(e *Expr) *Expr {
	if e.IsA(KColumnDef) {
		return e.ArgE("kind")
	}
	panic(&annAttributeError{Msg: fmt.Sprintf("'%s' object has no attribute 'kind'", annPyTypeName(e))})
}

// annMapKeys mirrors Map.keys / VarMap.keys.
func annMapKeys(e *Expr) []*Expr {
	if !e.IsA(KMap, KVarMap) {
		return nil
	}
	if keys := e.ArgE("keys"); keys != nil {
		return keys.Expressions()
	}
	return nil
}

// annMapValues mirrors Map.values / VarMap.values.
func annMapValues(e *Expr) []*Expr {
	if values := e.ArgE("values"); values != nil {
		return values.Expressions()
	}
	return nil
}

// annIndexOf mirrors list.index (structural equality), returning -1 when absent.
func annIndexOf(list []*Expr, x *Expr) int {
	for i, e := range list {
		if e == x || (e != nil && e.Equal(x)) {
			return i
		}
	}
	return -1
}

func annPyTypeName(e *Expr) string {
	if e == nil {
		return "NoneType"
	}
	return e.Kind().Name()
}

// annPyTypeRepr mirrors repr(type(x)) for an expression (or None).
func annPyTypeRepr(e *Expr) string {
	if e == nil {
		return "<class 'NoneType'>"
	}
	return e.classRepr()
}

// annAttributeError mirrors a Python AttributeError raised by sqlglot on malformed input.
type annAttributeError struct{ Msg string }

func (e *annAttributeError) Error() string { return e.Msg }

// annExprSet is an insertion-ordered set of expressions keyed by identity (Python dict[id, expr]).
type annExprSet struct {
	index map[*Expr]int
	items []*Expr
}

func newAnnExprSet() *annExprSet { return &annExprSet{index: map[*Expr]int{}} }

func (s *annExprSet) add(e *Expr) {
	if _, ok := s.index[e]; ok {
		return
	}
	s.index[e] = len(s.items)
	s.items = append(s.items, e)
}

func (s *annExprSet) delete(e *Expr) {
	if i, ok := s.index[e]; ok {
		s.items[i] = nil
		delete(s.index, e)
	}
}

func (s *annExprSet) values() []*Expr {
	out := make([]*Expr, 0, len(s.index))
	for _, e := range s.items {
		if e != nil {
			out = append(out, e)
		}
	}
	return out
}

// ---------------------------------------------------------------------------------------------
// Ports of sqlglot.helper date helpers (Python's datetime.date/datetime.fromisoformat, which in
// CPython 3.13 are implemented in C: Modules/_datetimemodule.c).

// DATE_UNITS: interval units that operate on date components
var annDATE_UNITS = newStrSet("day", "week", "month", "quarter", "year", "year_month")

// annIsDateUnit mirrors sqlglot.helper.is_date_unit.
func annIsDateUnit(expression *Expr) bool {
	return expression != nil && annDATE_UNITS.Has(pyLower(expression.Name()))
}

// annIsISODate mirrors sqlglot.helper.is_iso_date (datetime.date.fromisoformat succeeds).
func annIsISODate(text string) bool {
	b := annCStr(text)
	n := len(text)
	if n != 7 && n != 8 && n != 10 {
		return false
	}
	year, month, day, rv := annParseISOFormatDate(b, n)
	return rv >= 0 && annValidYMD(year, month, day)
}

// annIsISODatetime mirrors sqlglot.helper.is_iso_datetime (datetime.datetime.fromisoformat succeeds).
func annIsISODatetime(text string) bool {
	// _sanitize_isoformat_str: all valid ISO 8601 strings are at least 7 characters long
	if utf8.RuneCountInString(text) < 7 {
		return false
	}
	b := annCStr(text)
	length := len(text)

	separatorLocation := annFindISOFormatDatetimeSeparator(b, length)

	// date runs up to separator_location
	year, month, day, rv := annParseISOFormatDate(b, separatorLocation)

	var hour, minute, second, microsecond, tzoffset, tzusec int
	if rv == 0 && length > separatorLocation {
		// In UTF-8, the length of multi-byte characters is encoded in the MSB
		p := separatorLocation
		c := b.at(p)
		if c&0x80 == 0 {
			p++
		} else {
			switch c & 0xf0 {
			case 0xe0:
				p += 3
			case 0xf0:
				p += 4
			default:
				p += 2
			}
		}
		rv = annParseISOFormatTime(b, p, length-p, &hour, &minute, &second, &microsecond, &tzoffset, &tzusec)
	}
	if rv < 0 {
		return false
	}

	// tzinfo_from_isoformat_results: the offset must be strictly within (-24h, 24h)
	if rv == 1 && tzoffset != 0 {
		total := int64(tzoffset)*1_000_000 + int64(tzusec)
		if total <= -86_400_000_000 || total >= 86_400_000_000 {
			return false
		}
	}

	return annValidYMD(year, month, day) &&
		hour >= 0 && hour <= 23 &&
		minute >= 0 && minute <= 59 &&
		second >= 0 && second <= 59 &&
		microsecond >= 0 && microsecond <= 999999
}

// annCStr is a NUL-terminated view over the UTF-8 bytes of a string (reads past the end yield 0).
type annCStr []byte

func (b annCStr) at(i int) byte {
	if i < 0 || i >= len(b) {
		return 0
	}
	return b[i]
}

func annIsDigit(c byte) bool { return c >= '0' && c <= '9' }

// annParseDigits mirrors parse_digits: returns the value, the new position and success.
func annParseDigits(b annCStr, p int, numDigits int) (int, int, bool) {
	v := 0
	for i := 0; i < numDigits; i++ {
		c := b.at(p)
		p++
		if !annIsDigit(c) {
			return 0, p, false
		}
		v = v*10 + int(c-'0')
	}
	return v, p, true
}

// annParseISOFormatDate mirrors parse_isoformat_date (the date runs over `length` bytes).
func annParseISOFormatDate(b annCStr, length int) (year, month, day, rv int) {
	p := 0
	var ok bool
	year, p, ok = annParseDigits(b, p, 4)
	if !ok {
		return 0, 0, 0, -1
	}

	usesSeparator := b.at(p) == '-'
	if usesSeparator {
		p++
	}

	if b.at(p) == 'W' {
		// This is an isocalendar-style date string
		p++
		var isoWeek, isoDay int

		isoWeek, p, ok = annParseDigits(b, p, 2)
		if !ok {
			return 0, 0, 0, -3
		}

		// `length` is a size_t in C: a negative separator location compares as a huge value.
		if length < 0 || p < length {
			if usesSeparator {
				c := b.at(p)
				p++
				if c != '-' {
					return 0, 0, 0, -2
				}
			}

			isoDay, _, ok = annParseDigits(b, p, 1)
			if !ok {
				return 0, 0, 0, -4
			}
		} else {
			isoDay = 1
		}

		y, m, d, r := annISOToYMD(year, isoWeek, isoDay)
		if r != 0 {
			return 0, 0, 0, -3 + r
		}
		return y, m, d, 0
	}

	month, p, ok = annParseDigits(b, p, 2)
	if !ok {
		return 0, 0, 0, -1
	}

	if usesSeparator {
		c := b.at(p)
		p++
		if c != '-' {
			return 0, 0, 0, -2
		}
	}
	day, _, ok = annParseDigits(b, p, 2)
	if !ok {
		return 0, 0, 0, -1
	}
	return year, month, day, 0
}

// annISOToYMD mirrors iso_to_ymd.
func annISOToYMD(isoYear, isoWeek, isoDay int) (int, int, int, int) {
	// Year is bounded to 0 < year < 10000 because 9999-12-31 is (9999, 52, 5)
	if isoYear < 1 || isoYear > 9999 {
		return 0, 0, 0, -4
	}
	if isoWeek <= 0 || isoWeek >= 53 {
		outOfRange := true
		if isoWeek == 53 {
			// ISO years have 53 weeks in it on years starting with a Thursday
			// and on leap years starting on Wednesday
			firstWeekday := annWeekdayMon0(isoYear, 1, 1)
			if firstWeekday == 3 || (firstWeekday == 2 && annIsLeap(isoYear)) {
				outOfRange = false
			}
		}
		if outOfRange {
			return 0, 0, 0, -2
		}
	}

	if isoDay <= 0 || isoDay >= 8 {
		return 0, 0, 0, -3
	}

	// iso_week1_monday
	jan1 := time.Date(isoYear, time.January, 1, 0, 0, 0, 0, time.UTC)
	firstWeekday := annWeekdayMon0(isoYear, 1, 1)
	week1Monday := jan1.AddDate(0, 0, -firstWeekday)
	if firstWeekday > 3 { // if 1/1 was Fri, Sat, Sun
		week1Monday = week1Monday.AddDate(0, 0, 7)
	}

	dayOffset := (isoWeek-1)*7 + isoDay - 1
	t := week1Monday.AddDate(0, 0, dayOffset)
	return t.Year(), int(t.Month()), t.Day(), 0
}

// annWeekdayMon0 returns the weekday with Monday == 0 (proleptic Gregorian calendar).
func annWeekdayMon0(y, m, d int) int {
	return (int(time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC).Weekday()) + 6) % 7
}

func annIsLeap(y int) bool { return y%4 == 0 && (y%100 != 0 || y%400 == 0) }

// annValidYMD mirrors check_date_args.
func annValidYMD(y, m, d int) bool {
	if y < 1 || y > 9999 || m < 1 || m > 12 || d < 1 {
		return false
	}
	dim := [13]int{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}[m]
	if m == 2 && annIsLeap(y) {
		dim = 29
	}
	return d <= dim
}

// annFindISOFormatDatetimeSeparator mirrors _find_isoformat_datetime_separator.
func annFindISOFormatDatetimeSeparator(b annCStr, length int) int {
	const dateSeparator = '-'
	const weekIndicator = 'W'

	if length == 7 {
		return 7
	}

	if b.at(4) == dateSeparator {
		// YYYY-???
		if b.at(5) == weekIndicator {
			// YYYY-W??
			if length < 8 {
				return -1
			}

			if length > 8 && b.at(8) == dateSeparator {
				// YYYY-Www-D (10) or YYYY-Www-HH (8)
				if length == 9 {
					return -1
				}
				if length > 10 && annIsDigit(b.at(10)) {
					return 8
				}
				return 10
			}
			// YYYY-Www (8)
			return 8
		}
		// YYYY-MM-DD (10)
		return 10
	}

	// YYYY???
	if b.at(4) == weekIndicator {
		// YYYYWww (7) or YYYYWwwd (8)
		idx := 7
		for ; idx < length; idx++ {
			// Keep going until we run out of digits.
			if !annIsDigit(b.at(idx)) {
				break
			}
		}

		if idx < 9 {
			return idx
		}

		if idx%2 == 0 {
			// If the index of the last number is even, it's YYYYWww
			return 7
		}
		return 8
	}
	// YYYYMMDD (8)
	return 8
}

// annParseHHMMSSFF mirrors parse_hh_mm_ss_ff over b[start:end].
func annParseHHMMSSFF(b annCStr, start, end int, hour, minute, second, microsecond *int) int {
	*hour, *minute, *second, *microsecond = 0, 0, 0, 0
	p := start
	vals := [3]*int{hour, minute, second}
	hasSeparator := true

	// Parse [HH[:?MM[:?SS]]]
	for i := 0; i < 3; i++ {
		v, np, ok := annParseDigits(b, p, 2)
		p = np
		if !ok {
			return -3
		}
		*vals[i] = v

		c := b.at(p)
		p++
		if i == 0 {
			hasSeparator = c == ':'
		}

		if p >= end {
			if c != 0 {
				return 1
			}
			return 0
		} else if hasSeparator && c == ':' {
			continue
		} else if c == '.' || c == ',' {
			break
		} else if !hasSeparator {
			p--
		} else {
			return -4 // Malformed time separator
		}
	}

	// Parse fractional components
	lenRemains := end - p
	toParse := lenRemains
	if lenRemains >= 6 {
		toParse = 6
	}

	us, np, ok := annParseDigits(b, p, toParse)
	p = np
	if !ok {
		return -3
	}

	correction := [5]int{100000, 10000, 1000, 100, 10}
	if toParse < 6 && toParse >= 1 {
		us *= correction[toParse-1]
	}
	*microsecond = us

	for annIsDigit(b.at(p)) {
		p++ // skip truncated digits
	}

	// Return 1 if it's not the end of the string
	if b.at(p) != 0 {
		return 1
	}
	return 0
}

// annParseISOFormatTime mirrors parse_isoformat_time over b[start:start+dtlen].
func annParseISOFormatTime(b annCStr, start, dtlen int, hour, minute, second, microsecond, tzoffset, tzmicrosecond *int) int {
	p := start
	pEnd := start + dtlen

	tzinfoPos := p
	for {
		c := b.at(tzinfoPos)
		if c == 'Z' || c == '+' || c == '-' {
			break
		}
		tzinfoPos++
		if tzinfoPos >= pEnd {
			break
		}
	}

	rv := annParseHHMMSSFF(b, start, tzinfoPos, hour, minute, second, microsecond)

	if rv < 0 {
		return rv
	} else if tzinfoPos == pEnd {
		// We know that there's no time zone, so if there's stuff at the
		// end of the string it's an error.
		if rv == 1 {
			return -5
		}
		return 0
	}

	// Special case UTC / Zulu time.
	if b.at(tzinfoPos) == 'Z' {
		*tzoffset = 0
		*tzmicrosecond = 0

		if b.at(tzinfoPos+1) != 0 {
			return -5
		}
		return 1
	}

	tzsign := 1
	if b.at(tzinfoPos) == '-' {
		tzsign = -1
	}
	tzinfoPos++
	var tzhour, tzminute, tzsecond int
	rv = annParseHHMMSSFF(b, tzinfoPos, pEnd, &tzhour, &tzminute, &tzsecond, tzmicrosecond)

	*tzoffset = tzsign * ((tzhour * 3600) + (tzminute * 60) + tzsecond)
	*tzmicrosecond *= tzsign

	if rv != 0 {
		return -5
	}
	return 1
}
