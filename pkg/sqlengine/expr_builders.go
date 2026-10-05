package sqlengine

import (
	"fmt"
)

// Expression builders: a port of the builder helpers of sqlglot/expressions/core.py and
// sqlglot/expressions/builders.py, plus the builder methods of Query, Select, SetOperation,
// Subquery, Join and Expr in sqlglot/expressions/query.py / core.py.
//
// Conventions:
//   - Inputs must be *Expr. Strings that sqlglot would run through maybe_parse are NOT accepted
//     by the builders; parse them first (MaybeParse does exactly what maybe_parse does for strings).
//     The Python `dialect` / `**opts` parse options are therefore dropped.
//   - `copy` keeps its Python meaning; `appendMode` is Python's `append`.
//   - Python `bool | None` arguments are typed `any` and take nil, true or false.
//   - Builders panic like sqlglot raises: ParseError via parsePanic, other errors as *ValueError.
//
// API (Python -> Go):
//
//	maybe_parse(sql: str, into, prefix)  MaybeParse(sql, into, prefix, d)        d == nil: default dialect
//	maybe_parse(expr, copy)              maybeParseExpr(e, copy)
//	maybe_copy                           maybeCopy(e, copy)
//	_is_wrong_expression                 isWrongExpression(e, into)
//	_apply_builder                       applyBuilder(expression, instance, arg, copy, into, intoArg)
//	_apply_child_list_builder            applyChildListBuilder(exprs, instance, arg, appendMode, copy, into, properties...)
//	_apply_list_builder                  applyListBuilder(exprs, instance, arg, appendMode, copy)
//	_apply_conjunction_builder           applyConjunctionBuilder(exprs, instance, arg, into, appendMode, copy)
//	_apply_set_operation                 applySetOperation(exprs, setOp, distinct, copy, opts...)
//	_apply_cte_builder                   applyCTEBuilder(instance, alias, as, recursive, materialized, appendMode, copy, scalar)
//	_combine                             combineConditions(exprs, operator, copy, wrap)
//	_wrap                                wrapIfKind(e, kind)
//	condition                            ConditionExpr(e, copy)
//	and_ / or_ / xor                     AndExpr(exprs...) / OrExpr(exprs...) / XorExpr(exprs...)   (copy=True, wrap=True)
//	                                     AndExprOpts(exprs, copy, wrap) / OrExprOpts / XorExprOpts
//	not_                                 NotExpr(e, copy)
//	paren                                ParenExpr(e, copy)
//	to_identifier                        ToIdentifierAny(name, quoted, copy)              name: nil | string | *Expr
//	alias_ (table=False)                 AliasExpr(e, alias, quoted, copy)                alias: nil | string | *Expr
//	alias_ (table=True | [columns])      AliasTableExpr(e, alias, columns, quoted, copy)
//	column                               ColumnExpr(col, table, db, catalog, fields, quoted, copy)
//	table_                               TableExpr(table, db, catalog, quoted, alias)
//	var                                  VarChecked(name) / VarOf(e)
//	cast                                 CastExpr(e, to, copy, d)                         to: DType | string | DataType
//	DataType.build                       DataTypeBuild(dtype, d, udt, copy)
//	DataType.is_type                     DataTypeIsType(dt, dtypes, checkNullable)
//	select / from_ / subquery            SelectExpr(exprs...) / FromExpr(e) / SubqueryExpr(e, alias, copy)
//	union / intersect / except_          UnionExpr(exprs, distinct, copy) / IntersectExpr / ExceptExpr
//	tuple_ / array / case                TupleExpr(exprs, copy) / ArrayExpr(exprs, copy) / CaseExpr(e, copy)
//	replace_children / replace_tree      ReplaceChildren(e, fn) / ReplaceTree(e, fn, prune)
//	column_table_names                   ColumnTableNames(e, exclude)
//	Expr.and_ / or_ / not_ / as_         e.ExprAnd(exprs, copy, wrap) / e.ExprOr(...) / e.ExprNot(copy) / e.ExprAs(alias, quoted, copy)
//	Query.select                         e.QuerySelect(exprs, appendMode, copy)  (Select / SetOperation / Subquery)
//	Query.subquery                       e.QuerySubquery(alias, copy)            alias: nil | string | *Expr
//	Query.limit / offset                 e.QueryLimit(expr, copy) / e.QueryOffset(expr, copy)
//	Query.order_by                       e.QueryOrderBy(exprs, appendMode, copy)
//	Query.where                          e.QueryWhere(exprs, appendMode, copy)
//	Query.with_                          e.QueryWith(alias, as, recursive, materialized, appendMode, copy, scalar)
//	Query.union / intersect / except_    e.QueryUnion(exprs, distinct, copy) / e.QueryIntersect(...) / e.QueryExcept(...)
//	Select.from_                         e.SelectFrom(expr, copy)
//	Select.group_by / sort_by / cluster_by
//	                                     e.SelectGroupBy(exprs, appendMode, copy) / e.SelectSortBy(...) / e.SelectClusterBy(...)
//	Select.lateral / window              e.SelectLateral(exprs, appendMode, copy) / e.SelectWindow(...)
//	Select.join                          e.SelectJoin(expr, on, using, appendMode, joinType, joinAlias, copy)
//	Select.having / qualify              e.SelectHaving(exprs, appendMode, copy) / e.SelectQualify(...)
//	Select.distinct                      e.SelectDistinct(ons, distinct, copy)
//	Select.lock / hint                   e.SelectLock(update, copy) / e.SelectHint(hints, copy)
//	Join.on / using                      e.JoinOn(exprs, appendMode, copy) / e.JoinUsing(exprs, appendMode, copy)

// ---------------------------------------------------------------------------------------------
// maybe_parse / maybe_copy
// ---------------------------------------------------------------------------------------------

// MaybeParse mirrors exp.maybe_parse for string inputs: sqlglot.parse_one(prefix + " " + sql,
// read=d, into=into). into == KNone parses a statement; d == nil uses the default dialect.
// Parse failures panic (ParseError via parsePanic), like the Python exception.
func MaybeParse(sql string, into Kind, prefix string, d *Dialect) *Expr {
	if prefix != "" {
		sql = prefix + " " + sql
	}
	if d == nil {
		d = MustDialect("")
	}
	var (
		res *Expr
		err error
	)
	if into != KNone {
		res, err = d.ParseOneInto(into, sql, nil)
	} else {
		res, err = d.ParseOne(sql, nil)
	}
	if err != nil {
		if pe, ok := err.(*ParseError); ok {
			panic(parsePanic{pe})
		}
		panic(err)
	}
	return res
}

// maybeParseExpr mirrors exp.maybe_parse for an expression input.
func maybeParseExpr(e *Expr, copy bool) *Expr {
	if e == nil {
		panic(parsePanic{&ParseError{Msg: "SQL cannot be None"}})
	}
	if copy {
		return e.Copy()
	}
	return e
}

// maybeCopy mirrors exp.maybe_copy.
func maybeCopy(e *Expr, copy bool) *Expr {
	if copy && e != nil {
		return e.Copy()
	}
	return e
}

// isWrongExpression mirrors exp._is_wrong_expression.
func isWrongExpression(e *Expr, into Kind) bool {
	return e != nil && into != KNone && !e.IsA(into)
}

// ---------------------------------------------------------------------------------------------
// Generic builders (core.py)
// ---------------------------------------------------------------------------------------------

// builderKwargs is an insertion-ordered mapping with Python dict update semantics.
type builderKwargs struct {
	keys []string
	vals map[string]any
}

func newBuilderKwargs(kv []any) *builderKwargs {
	b := &builderKwargs{vals: map[string]any{}}
	for i := 0; i+1 < len(kv); i += 2 {
		b.set(kv[i].(string), kv[i+1])
	}
	return b
}

func (b *builderKwargs) set(k string, v any) {
	if _, ok := b.vals[k]; !ok {
		b.keys = append(b.keys, k)
	}
	b.vals[k] = v
}

// applyBuilder mirrors exp._apply_builder for an expression input.
func applyBuilder(expression, instance *Expr, arg string, copy bool, into Kind, intoArg string) *Expr {
	if isWrongExpression(expression, into) {
		expression = New(into, intoArg, expression)
	}
	instance = maybeCopy(instance, copy)
	expression = maybeParseExpr(expression, false)
	instance.Set(arg, expression)
	return instance
}

// applyChildListBuilder mirrors exp._apply_child_list_builder. properties holds the optional
// `properties` mapping as alternating key/value pairs.
func applyChildListBuilder(expressions []*Expr, instance *Expr, arg string, appendMode bool, copy bool, into Kind, properties ...any) *Expr {
	instance = maybeCopy(instance, copy)
	parsed := []*Expr{}
	props := newBuilderKwargs(properties)

	for _, expression := range expressions {
		if expression != nil {
			if isWrongExpression(expression, into) {
				expression = New(into, "expressions", []*Expr{expression})
			}

			expression = maybeParseExpr(expression, false)
			for _, a := range expression.args {
				if a.key == "expressions" {
					if l, ok := a.val.([]*Expr); ok {
						parsed = append(parsed, l...)
					}
				} else {
					props.set(a.key, a.val)
				}
			}
		}
	}

	existing := instance.ArgE(arg)
	if appendMode && existing != nil {
		parsed = append(append([]*Expr{}, existing.Expressions()...), parsed...)
	}
	if into == KNone {
		panic(&ValueError{Msg: "`into` is required to use `_apply_child_list_builder`"})
	}
	child := New(into, "expressions", parsed)
	for _, k := range props.keys {
		child.Set(k, props.vals[k])
	}
	instance.Set(arg, child)

	return instance
}

// applyListBuilder mirrors exp._apply_list_builder for expression inputs (`into` and `prefix`
// only matter when parsing strings).
func applyListBuilder(expressions []*Expr, instance *Expr, arg string, appendMode bool, copy bool) *Expr {
	inst := maybeCopy(instance, copy)

	parsed := []*Expr{}
	for _, expression := range expressions {
		if expression != nil {
			parsed = append(parsed, maybeParseExpr(expression, false))
		}
	}

	existingExpressions := inst.ArgL(arg)
	if appendMode && len(existingExpressions) > 0 {
		parsed = append(append([]*Expr{}, existingExpressions...), parsed...)
	}

	inst.Set(arg, parsed)
	return inst
}

// applyConjunctionBuilder mirrors exp._apply_conjunction_builder. into == KNone means None.
func applyConjunctionBuilder(expressions []*Expr, instance *Expr, arg string, into Kind, appendMode bool, copy bool) *Expr {
	filtered := []*Expr{}
	for _, e := range expressions {
		if e != nil {
			filtered = append(filtered, e)
		}
	}
	if len(filtered) == 0 {
		return instance
	}

	inst := maybeCopy(instance, copy)

	if appendMode && inst.Arg(arg) != nil {
		existing := inst.ArgE(arg)
		first := existing
		if into != KNone {
			first = existing.This()
		}
		filtered = append([]*Expr{first}, filtered...)
	}

	node := AndExprOpts(filtered, copy, true)

	if into != KNone {
		inst.Set(arg, New(into, "this", node))
	} else {
		inst.Set(arg, node)
	}
	return inst
}

// combineConditions mirrors exp._combine.
func combineConditions(expressions []*Expr, operator Kind, copy bool, wrap bool) *Expr {
	conditions := []*Expr{}
	for _, expression := range expressions {
		if expression != nil {
			conditions = append(conditions, ConditionExpr(expression, copy))
		}
	}

	if len(conditions) == 0 {
		panic(&ValueError{Msg: "not enough values to unpack (expected at least 1, got 0)"})
	}
	this, rest := conditions[0], conditions[1:]
	if len(rest) > 0 && wrap {
		this = wrapIfKind(this, KConnector)
	}
	for _, expression := range rest {
		right := expression
		if wrap {
			right = wrapIfKind(expression, KConnector)
		}
		this = New(operator, "this", this, "expression", right)
	}

	return this
}

// wrapIfKind mirrors exp._wrap.
func wrapIfKind(expression *Expr, kind Kind) *Expr {
	if expression.IsA(kind) {
		return New(KParen, "this", expression)
	}
	return expression
}

// applySetOperation mirrors exp._apply_set_operation. opts are extra constructor kwargs
// (alternating key/value pairs) passed to every set operation node.
func applySetOperation(expressions []*Expr, setOperation Kind, distinct any, copy bool, opts ...any) *Expr {
	var acc *Expr
	for i, e := range expressions {
		x := maybeParseExpr(e, copy)
		if i == 0 {
			acc = x
			continue
		}
		kv := make([]any, 0, 6+len(opts))
		kv = append(kv, "this", acc, "expression", x, "distinct", distinct)
		kv = append(kv, opts...)
		acc = New(setOperation, kv...)
	}
	if acc == nil {
		panic(&ValueError{Msg: "reduce() of empty iterable with no initial value"})
	}
	return acc
}

// applyCTEBuilder mirrors query._apply_cte_builder for expression inputs.
func applyCTEBuilder(instance, alias, as *Expr, recursive, materialized any, appendMode bool, copy bool, scalar any) *Expr {
	aliasExpression := maybeParseExpr(alias, false)
	asExpression := maybeParseExpr(as, copy)
	if truthy(scalar) && !asExpression.IsA(KSubquery) {
		// scalar CTE must be wrapped in a subquery
		asExpression = New(KSubquery, "this", asExpression)
	}
	cte := New(KCTE, "this", asExpression, "alias", aliasExpression, "materialized", materialized, "scalar", scalar)
	var properties []any
	if truthy(recursive) {
		properties = []any{"recursive", recursive}
	}
	return applyChildListBuilder([]*Expr{cte}, instance, "with_", appendMode, copy, KWith, properties...)
}

// ---------------------------------------------------------------------------------------------
// Free builders (core.py / builders.py / query.py)
// ---------------------------------------------------------------------------------------------

// ConditionExpr mirrors exp.condition for an expression input.
func ConditionExpr(expression *Expr, copy bool) *Expr {
	return maybeParseExpr(expression, copy)
}

// AndExpr mirrors exp.and_(*expressions) (copy=True, wrap=True). nil inputs are skipped.
func AndExpr(expressions ...*Expr) *Expr { return combineConditions(expressions, KAnd, true, true) }

// AndExprOpts mirrors exp.and_(*expressions, copy=copy, wrap=wrap).
func AndExprOpts(expressions []*Expr, copy bool, wrap bool) *Expr {
	return combineConditions(expressions, KAnd, copy, wrap)
}

// OrExpr mirrors exp.or_(*expressions) (copy=True, wrap=True).
func OrExpr(expressions ...*Expr) *Expr { return combineConditions(expressions, KOr, true, true) }

// OrExprOpts mirrors exp.or_(*expressions, copy=copy, wrap=wrap).
func OrExprOpts(expressions []*Expr, copy bool, wrap bool) *Expr {
	return combineConditions(expressions, KOr, copy, wrap)
}

// XorExpr mirrors exp.xor(*expressions) (copy=True, wrap=True).
func XorExpr(expressions ...*Expr) *Expr { return combineConditions(expressions, KXor, true, true) }

// XorExprOpts mirrors exp.xor(*expressions, copy=copy, wrap=wrap).
func XorExprOpts(expressions []*Expr, copy bool, wrap bool) *Expr {
	return combineConditions(expressions, KXor, copy, wrap)
}

// NotExpr mirrors exp.not_(expression, copy=copy).
func NotExpr(expression *Expr, copy bool) *Expr {
	this := ConditionExpr(expression, copy)
	return New(KNot, "this", wrapIfKind(this, KConnector))
}

// ParenExpr mirrors exp.paren(expression, copy=copy).
func ParenExpr(expression *Expr, copy bool) *Expr {
	return New(KParen, "this", maybeParseExpr(expression, copy))
}

// ToIdentifierAny mirrors exp.to_identifier(name, quoted, copy) for nil, string and
// Identifier inputs.
func ToIdentifierAny(name any, quoted *bool, copy bool) *Expr {
	switch n := name.(type) {
	case nil:
		return nil
	case *Expr:
		if n == nil {
			return nil
		}
		if n.IsA(KIdentifier) {
			return maybeCopy(n, copy)
		}
		panic(&ValueError{Msg: "Name needs to be a string or an Identifier, got: " + n.classRepr()})
	case string:
		return ToIdentifier(n, quoted)
	}
	panic(&ValueError{Msg: fmt.Sprintf("Name needs to be a string or an Identifier, got: %T", name)})
}

// pyTruthyName mirrors the truthiness of a `str | Identifier | None` argument.
func pyTruthyName(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case string:
		return x != ""
	case *Expr:
		return x != nil
	}
	return true
}

// AliasExpr mirrors exp.alias_(expression, alias, table=False, quoted=quoted, copy=copy).
func AliasExpr(expression *Expr, alias any, quoted *bool, copy bool) *Expr {
	return aliasImpl(expression, alias, false, nil, quoted, copy)
}

// AliasTableExpr mirrors exp.alias_(expression, alias, table=True or columns, ...). columns
// holds the optional column names (string or Identifier).
func AliasTableExpr(expression *Expr, alias any, columns []any, quoted *bool, copy bool) *Expr {
	return aliasImpl(expression, alias, true, columns, quoted, copy)
}

func aliasImpl(expression *Expr, alias any, table bool, columns []any, quoted *bool, copy bool) *Expr {
	exp := maybeParseExpr(expression, copy)
	aliasID := ToIdentifierAny(alias, quoted, true)

	if table {
		tableAlias := New(KTableAlias, "this", aliasID)
		exp.Set("alias", tableAlias)

		for _, column := range columns {
			tableAlias.Append("columns", ToIdentifierAny(column, quoted, true))
		}

		return exp
	}

	// We don't set the "alias" arg for Window expressions, because that would add an IDENTIFIER node in
	// the AST, representing a "named_window" construct (eg. bigquery). What we want is an ALIAS node
	// for the complete Window expression.
	if exp.kind.hasArgType("alias") && exp.kind.Name() != "Window" {
		exp.Set("alias", aliasID)
		return exp
	}
	return New(KAlias, "this", exp, "alias", aliasID)
}

// ColumnExpr mirrors exp.column(col, table, db, catalog, fields=fields, quoted=quoted, copy=copy).
// col is a string, Identifier or Star; table/db/catalog/fields entries are nil, strings or Identifiers.
func ColumnExpr(col any, table, db, catalog any, fields []any, quoted *bool, copy bool) *Expr {
	var colV any = col
	if c, ok := col.(*Expr); !ok || !c.IsA(KStar) {
		colV = ToIdentifierAny(col, quoted, copy)
	}

	this := New(
		KColumn,
		"this", colV,
		"table", ToIdentifierAny(table, quoted, copy),
		"db", ToIdentifierAny(db, quoted, copy),
		"catalog", ToIdentifierAny(catalog, quoted, copy),
	)

	if len(fields) > 0 {
		parts := []*Expr{this}
		for _, field := range fields {
			parts = append(parts, ToIdentifierAny(field, quoted, copy))
		}
		this = DotBuild(parts)
	}
	return this
}

// TableExpr mirrors exp.table_(table, db, catalog, quoted, alias).
func TableExpr(table, db, catalog any, quoted *bool, alias any) *Expr {
	var this, dbV, catalogV, aliasV *Expr
	if pyTruthyName(table) {
		this = ToIdentifierAny(table, quoted, true)
	}
	if pyTruthyName(db) {
		dbV = ToIdentifierAny(db, quoted, true)
	}
	if pyTruthyName(catalog) {
		catalogV = ToIdentifierAny(catalog, quoted, true)
	}
	if pyTruthyName(alias) {
		aliasV = New(KTableAlias, "this", ToIdentifierAny(alias, nil, true))
	}
	return New(KTable, "this", this, "db", dbV, "catalog", catalogV, "alias", aliasV)
}

// VarChecked mirrors exp.var(name) for a string, including its empty-name ValueError.
func VarChecked(name string) *Expr {
	if name == "" {
		panic(&ValueError{Msg: "Cannot convert empty name into var."})
	}
	return New(KVar, "this", name)
}

// VarOf mirrors exp.var(expression): the expression's name becomes the var.
func VarOf(e *Expr) *Expr {
	if e == nil {
		panic(&ValueError{Msg: "Cannot convert empty name into var."})
	}
	return New(KVar, "this", e.Name())
}

// DataTypeBuild mirrors exp.DataType.build(dtype, dialect=d, udt=udt, copy=copy) for DType,
// string and expression inputs.
func DataTypeBuild(dtype any, d *Dialect, udt bool, copy bool) *Expr {
	switch x := dtype.(type) {
	case string:
		return dataTypeFromStr(x, d, udt)
	case DType:
		return New(KDataType, "this", x)
	case *Expr:
		if x.IsA(KIdentifier, KDot) && udt {
			return New(KDataType, "this", DT_USERDEFINED, "kind", x)
		}
		if x.IsA(KDataType) {
			return maybeCopy(x, copy)
		}
		if x != nil {
			panic(&ValueError{Msg: "Invalid data type: " + x.classRepr() + ". Expected str or DType"})
		}
	}
	panic(&ValueError{Msg: fmt.Sprintf("Invalid data type: %T. Expected str or DType", dtype)})
}

// dataTypeFromStr mirrors exp.DataType.from_str.
func dataTypeFromStr(dtype string, d *Dialect, udt bool) *Expr {
	if pyUpper(dtype) == "UNKNOWN" {
		return New(KDataType, "this", DT_UNKNOWN)
	}
	if d == nil {
		d = MustDialect("")
	}
	level := ErrorLevelIgnore
	res, err := d.ParseOneInto(KDataType, dtype, &ParseOptions{ErrorLevel: &level})
	if err != nil {
		if pe, ok := err.(*ParseError); ok {
			if udt {
				return New(KDataType, "this", DT_USERDEFINED, "kind", dtype)
			}
			panic(parsePanic{pe})
		}
		panic(err)
	}
	return res
}

// DataTypeIsType mirrors exp.DataType.is_type(*dtypes, check_nullable=checkNullable).
func DataTypeIsType(dt *Expr, dtypes []any, checkNullable bool) bool {
	selfIsNullable := dt.Arg("nullable")
	for _, dtype := range dtypes {
		otherType := DataTypeBuild(dtype, nil, true, false)
		otherIsNullable := otherType.Arg("nullable")
		var matches bool
		if len(otherType.Expressions()) > 0 ||
			(checkNullable && (truthy(selfIsNullable) || truthy(otherIsNullable))) ||
			dt.DTypeOf() == DT_USERDEFINED ||
			otherType.DTypeOf() == DT_USERDEFINED {
			matches = dt.Equal(otherType)
		} else {
			matches = dt.Arg("this") == otherType.Arg("this")
		}

		if matches {
			return true
		}
	}
	return false
}

// CastExpr mirrors exp.cast(expression, to, copy=copy, dialect=d). `to` is a DType, a type
// string or a DataType expression; d == nil uses the default dialect.
func CastExpr(expression *Expr, to any, copy bool, d *Dialect) *Expr {
	expr := maybeParseExpr(expression, copy)
	dataType := DataTypeBuild(to, d, false, copy)

	// dont re-cast if the expression is already a cast to the correct type
	if expr.IsA(KCast) {
		targetDialect := d
		if targetDialect == nil {
			targetDialect = MustDialect("")
		}
		var typeMapping map[DType]string
		if targetDialect.G != nil && targetDialect.G.GeneratorData != nil {
			typeMapping = targetDialect.G.TYPE_MAPPING
		} else {
			typeMapping = generatorSettings_base().TYPE_MAPPING
		}
		mapped := func(t DType) string {
			if v, ok := typeMapping[t]; ok {
				return v
			}
			return dtypeValues[t]
		}

		existingCastType := expr.ArgE("to").DTypeOf()
		newCastType := dataType.DTypeOf()
		typesAreEquivalent := mapped(existingCastType) == mapped(newCastType)

		if DataTypeIsType(expr.ArgE("to"), []any{dataType}, false) || typesAreEquivalent {
			return expr
		}
	}

	expr = New(KCast, "this", expr, "to", dataType)
	expr.SetType(dataType)

	return expr
}

// SelectExpr mirrors exp.select(*expressions).
func SelectExpr(expressions ...*Expr) *Expr {
	return New(KSelect).QuerySelect(expressions, true, true)
}

// FromExpr mirrors exp.from_(expression).
func FromExpr(expression *Expr) *Expr {
	return New(KSelect).SelectFrom(expression, true)
}

// SubqueryExpr mirrors exp.subquery(expression, alias, copy=copy).
func SubqueryExpr(expression *Expr, alias any, copy bool) *Expr {
	expr := chunkFAssertIs(maybeParseExpr(expression, false), KQuery).QuerySubquery(alias, copy)
	return New(KSelect).SelectFrom(expr, true)
}

// UnionExpr mirrors exp.union(*expressions, distinct=distinct, copy=copy).
func UnionExpr(expressions []*Expr, distinct bool, copy bool) *Expr {
	if len(expressions) < 2 {
		panic(&ValueError{Msg: "At least two expressions are required by `union`."})
	}
	return applySetOperation(expressions, KUnion, distinct, copy)
}

// IntersectExpr mirrors exp.intersect(*expressions, distinct=distinct, copy=copy).
func IntersectExpr(expressions []*Expr, distinct bool, copy bool) *Expr {
	if len(expressions) < 2 {
		panic(&ValueError{Msg: "At least two expressions are required by `intersect`."})
	}
	return applySetOperation(expressions, KIntersect, distinct, copy)
}

// ExceptExpr mirrors exp.except_(*expressions, distinct=distinct, copy=copy).
func ExceptExpr(expressions []*Expr, distinct bool, copy bool) *Expr {
	if len(expressions) < 2 {
		panic(&ValueError{Msg: "At least two expressions are required by `except_`."})
	}
	return applySetOperation(expressions, KExcept, distinct, copy)
}

// TupleExpr mirrors exp.tuple_(*expressions, copy=copy).
func TupleExpr(expressions []*Expr, copy bool) *Expr {
	out := make([]*Expr, 0, len(expressions))
	for _, e := range expressions {
		out = append(out, maybeParseExpr(e, copy))
	}
	return New(KTuple, "expressions", out)
}

// ArrayExpr mirrors exp.array(*expressions, copy=copy).
func ArrayExpr(expressions []*Expr, copy bool) *Expr {
	out := make([]*Expr, 0, len(expressions))
	for _, e := range expressions {
		out = append(out, maybeParseExpr(e, copy))
	}
	return New(KArray, "expressions", out)
}

// CaseExpr mirrors exp.case(expression, copy=copy); expression may be nil.
func CaseExpr(expression *Expr, copy bool) *Expr {
	var this *Expr
	if expression != nil {
		this = maybeParseExpr(expression, copy)
	}
	return New(KCase, "this", this, "ifs", []*Expr{})
}

// ReplaceChildren mirrors exp.replace_children: fn returns nil, an *Expr or a []*Expr.
func ReplaceChildren(expression *Expr, fn func(*Expr) any) {
	type kv struct {
		key string
		val any
	}
	items := make([]kv, len(expression.args))
	for i, a := range expression.args {
		items[i] = kv{a.key, a.val}
	}

	ensureCollection := func(v any) []*Expr {
		switch x := v.(type) {
		case nil:
			return nil
		case *Expr:
			if x == nil {
				return nil
			}
			return []*Expr{x}
		case []*Expr:
			return x
		}
		panic(&ValueError{Msg: fmt.Sprintf("replace_children: unexpected value %T", v)})
	}

	for _, it := range items {
		switch v := it.val.(type) {
		case []*Expr:
			newChildNodes := []*Expr{}
			for _, cn := range v {
				if cn != nil {
					newChildNodes = append(newChildNodes, ensureCollection(fn(cn))...)
				} else {
					newChildNodes = append(newChildNodes, cn)
				}
			}
			expression.Set(it.key, newChildNodes)
		case *Expr:
			newChildNodes := ensureCollection(fn(v))
			expression.Set(it.key, seqGet(newChildNodes, 0))
		default:
			expression.Set(it.key, it.val)
		}
	}
}

// ReplaceTree mirrors exp.replace_tree (reverse DFS, leaves first; new nodes are traversed too).
func ReplaceTree(expression *Expr, fn func(*Expr) *Expr, prune func(*Expr) bool) *Expr {
	var stack []*Expr
	for n := range expression.DFS(prune) {
		stack = append(stack, n)
	}

	var newNode *Expr
	for len(stack) > 0 {
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		newNode = fn(node)

		if newNode != node {
			node.Replace(newNode)

			if newNode != nil {
				stack = append(stack, newNode)
			}
		}
	}

	return newNode
}

// ColumnTableNames mirrors exp.column_table_names.
func ColumnTableNames(expression *Expr, exclude string) StrSet {
	out := StrSet{}
	for column := range expression.FindAll(KColumn) {
		table := column.TableName()
		if table != "" && table != exclude {
			out[table] = struct{}{}
		}
	}
	return out
}

// chunkFAssertIs mirrors Expr.assert_is(type_) (AssertionError is reported as a ValueError).
func chunkFAssertIs(e *Expr, kind Kind) *Expr {
	if !e.IsA(kind) {
		panic(&ValueError{Msg: chunkFPyStr(e) + " is not " + kindClassRepr(kind) + "."})
	}
	return e
}

// unnestMethod mirrors a dynamic `.unnest()` call: Subquery.unnest (first non-subquery) for
// subqueries, Expr.unnest (strip parentheses) otherwise.
func unnestMethod(e *Expr) *Expr {
	if e.IsA(KSubquery) {
		return e.UnnestSubquery()
	}
	return e.Unnest()
}

// ---------------------------------------------------------------------------------------------
// Expr methods (core.py)
// ---------------------------------------------------------------------------------------------

// ExprAnd mirrors Expr.and_(*expressions, copy=copy, wrap=wrap).
func (e *Expr) ExprAnd(expressions []*Expr, copy bool, wrap bool) *Expr {
	return AndExprOpts(append([]*Expr{e}, expressions...), copy, wrap)
}

// ExprOr mirrors Expr.or_(*expressions, copy=copy, wrap=wrap).
func (e *Expr) ExprOr(expressions []*Expr, copy bool, wrap bool) *Expr {
	return OrExprOpts(append([]*Expr{e}, expressions...), copy, wrap)
}

// ExprNot mirrors Expr.not_(copy=copy).
func (e *Expr) ExprNot(copy bool) *Expr { return NotExpr(e, copy) }

// ExprAs mirrors Expr.as_(alias, quoted=quoted, copy=copy) (table=False).
func (e *Expr) ExprAs(alias any, quoted *bool, copy bool) *Expr {
	return AliasExpr(e, alias, quoted, copy)
}

// ---------------------------------------------------------------------------------------------
// Query methods (query.py)
// ---------------------------------------------------------------------------------------------

// QuerySelect mirrors Select.select / SetOperation.select / Subquery.select.
func (e *Expr) QuerySelect(expressions []*Expr, appendMode bool, copy bool) *Expr {
	switch {
	case e.IsA(KSelect):
		return applyListBuilder(expressions, e, "expressions", appendMode, copy)
	case e.IsA(KSetOperation):
		this := maybeCopy(e, copy)
		unnestMethod(this.This()).QuerySelect(expressions, appendMode, false)
		unnestMethod(this.Expression()).QuerySelect(expressions, appendMode, false)
		return this
	case e.IsA(KSubquery):
		this := maybeCopy(e, copy)
		inner := unnestMethod(this)
		if inner.IsA(KQuery) {
			inner.QuerySelect(expressions, appendMode, false)
		}
		return this
	}
	panic(&ValueError{Msg: "Query objects must implement `select`"})
}

// QuerySubquery mirrors Query.subquery(alias, copy=copy). alias: nil, string or *Expr.
func (e *Expr) QuerySubquery(alias any, copy bool) *Expr {
	instance := maybeCopy(e, copy)
	var aliasV *Expr
	switch a := alias.(type) {
	case *Expr:
		aliasV = a
	case string:
		if a != "" {
			aliasV = New(KTableAlias, "this", ToIdentifier(a, nil))
		}
	}

	return New(KSubquery, "this", instance, "alias", aliasV)
}

// QueryLimit mirrors Query.limit(expression, copy=copy).
func (e *Expr) QueryLimit(expression *Expr, copy bool) *Expr {
	return applyBuilder(expression, e, "limit", copy, KLimit, "expression")
}

// QueryOffset mirrors Query.offset(expression, copy=copy).
func (e *Expr) QueryOffset(expression *Expr, copy bool) *Expr {
	return applyBuilder(expression, e, "offset", copy, KOffset, "expression")
}

// QueryOrderBy mirrors Query.order_by(*expressions, append=appendMode, copy=copy).
func (e *Expr) QueryOrderBy(expressions []*Expr, appendMode bool, copy bool) *Expr {
	return applyChildListBuilder(expressions, e, "order", appendMode, copy, KOrder)
}

// QueryWhere mirrors Query.where(*expressions, append=appendMode, copy=copy).
func (e *Expr) QueryWhere(expressions []*Expr, appendMode bool, copy bool) *Expr {
	unwrapped := make([]*Expr, len(expressions))
	for i, expr := range expressions {
		if expr.IsA(KWhere) {
			unwrapped[i] = expr.This()
		} else {
			unwrapped[i] = expr
		}
	}
	return applyConjunctionBuilder(unwrapped, e, "where", KWhere, appendMode, copy)
}

// QueryWith mirrors Query.with_(alias, as_, recursive, materialized, append, copy=copy, scalar).
// recursive, materialized and scalar take nil, true or false.
func (e *Expr) QueryWith(alias, as *Expr, recursive, materialized any, appendMode bool, copy bool, scalar any) *Expr {
	return applyCTEBuilder(e, alias, as, recursive, materialized, appendMode, copy, scalar)
}

// QueryUnion mirrors Query.union(*expressions, distinct=distinct, copy=copy).
func (e *Expr) QueryUnion(expressions []*Expr, distinct bool, copy bool) *Expr {
	return UnionExpr(append([]*Expr{e}, expressions...), distinct, copy)
}

// QueryIntersect mirrors Query.intersect(*expressions, distinct=distinct, copy=copy).
func (e *Expr) QueryIntersect(expressions []*Expr, distinct bool, copy bool) *Expr {
	return IntersectExpr(append([]*Expr{e}, expressions...), distinct, copy)
}

// QueryExcept mirrors Query.except_(*expressions, distinct=distinct, copy=copy).
func (e *Expr) QueryExcept(expressions []*Expr, distinct bool, copy bool) *Expr {
	return ExceptExpr(append([]*Expr{e}, expressions...), distinct, copy)
}

// ---------------------------------------------------------------------------------------------
// Select methods (query.py)
// ---------------------------------------------------------------------------------------------

// SelectFrom mirrors Select.from_(expression, copy=copy).
func (e *Expr) SelectFrom(expression *Expr, copy bool) *Expr {
	return applyBuilder(expression, e, "from_", copy, KFrom, "this")
}

// SelectGroupBy mirrors Select.group_by(*expressions, append=appendMode, copy=copy).
func (e *Expr) SelectGroupBy(expressions []*Expr, appendMode bool, copy bool) *Expr {
	if len(expressions) == 0 {
		if !copy {
			return e
		}
		return e.Copy()
	}

	return applyChildListBuilder(expressions, e, "group", appendMode, copy, KGroup)
}

// SelectSortBy mirrors Select.sort_by(*expressions, append=appendMode, copy=copy).
func (e *Expr) SelectSortBy(expressions []*Expr, appendMode bool, copy bool) *Expr {
	return applyChildListBuilder(expressions, e, "sort", appendMode, copy, KSort)
}

// SelectClusterBy mirrors Select.cluster_by(*expressions, append=appendMode, copy=copy).
func (e *Expr) SelectClusterBy(expressions []*Expr, appendMode bool, copy bool) *Expr {
	return applyChildListBuilder(expressions, e, "cluster", appendMode, copy, KCluster)
}

// SelectLateral mirrors Select.lateral(*expressions, append=appendMode, copy=copy).
func (e *Expr) SelectLateral(expressions []*Expr, appendMode bool, copy bool) *Expr {
	return applyListBuilder(expressions, e, "laterals", appendMode, copy)
}

// SelectJoin mirrors Select.join(expression, on=on, using=using, append=appendMode,
// join_type=joinType, join_alias=joinAlias, copy=copy). joinType "" means None (when set it is
// parsed with the default dialect, like sqlglot does); joinAlias is nil, a string or an Identifier.
func (e *Expr) SelectJoin(expression *Expr, on []*Expr, using []*Expr, appendMode bool, joinType string, joinAlias any, copy bool) *Expr {
	expression = maybeParseExpr(expression, false)

	join := expression
	if !expression.IsA(KJoin) {
		join = New(KJoin, "this", expression)
	}

	if join.This().IsA(KSelect) {
		join.This().Replace(join.This().QuerySubquery(nil, true))
	}

	if joinType != "" {
		newJoin := MaybeParse("FROM _ "+joinType+" JOIN _", KNone, "", nil).Find(KJoin)
		method := newJoin.MethodText()
		side := newJoin.SideText()
		kind := newJoin.KindText()

		if method != "" {
			join.Set("method", method)
		}
		if side != "" {
			join.Set("side", side)
		}
		if kind != "" {
			join.Set("kind", kind)
		}
	}

	if len(on) > 0 {
		join.Set("on", AndExprOpts(on, copy, true))
	}

	if len(using) > 0 {
		join = applyListBuilder(using, join, "using", appendMode, copy)
	}

	if pyTruthyName(joinAlias) {
		join.Set("this", AliasTableExpr(join.This(), joinAlias, nil, nil, true))
	}

	return applyListBuilder([]*Expr{join}, e, "joins", appendMode, copy)
}

// SelectHaving mirrors Select.having(*expressions, append=appendMode, copy=copy).
func (e *Expr) SelectHaving(expressions []*Expr, appendMode bool, copy bool) *Expr {
	return applyConjunctionBuilder(expressions, e, "having", KHaving, appendMode, copy)
}

// SelectWindow mirrors Select.window(*expressions, append=appendMode, copy=copy).
func (e *Expr) SelectWindow(expressions []*Expr, appendMode bool, copy bool) *Expr {
	return applyListBuilder(expressions, e, "windows", appendMode, copy)
}

// SelectQualify mirrors Select.qualify(*expressions, append=appendMode, copy=copy).
func (e *Expr) SelectQualify(expressions []*Expr, appendMode bool, copy bool) *Expr {
	return applyConjunctionBuilder(expressions, e, "qualify", KQualify, appendMode, copy)
}

// SelectDistinct mirrors Select.distinct(*ons, distinct=distinct, copy=copy).
func (e *Expr) SelectDistinct(ons []*Expr, distinct bool, copy bool) *Expr {
	instance := maybeCopy(e, copy)
	var on *Expr
	if len(ons) > 0 {
		exprs := []*Expr{}
		for _, o := range ons {
			if o != nil {
				exprs = append(exprs, maybeParseExpr(o, copy))
			}
		}
		on = New(KTuple, "expressions", exprs)
	}
	if distinct {
		instance.Set("distinct", New(KDistinct, "on", on))
	} else {
		instance.Set("distinct", nil)
	}
	return instance
}

// SelectLock mirrors Select.lock(update=update, copy=copy).
func (e *Expr) SelectLock(update bool, copy bool) *Expr {
	inst := maybeCopy(e, copy)
	inst.Set("locks", []*Expr{New(KLock, "update", update)})

	return inst
}

// SelectHint mirrors Select.hint(*hints, copy=copy).
func (e *Expr) SelectHint(hints []*Expr, copy bool) *Expr {
	inst := maybeCopy(e, copy)
	exprs := make([]*Expr, 0, len(hints))
	for _, h := range hints {
		exprs = append(exprs, maybeParseExpr(h, copy))
	}
	inst.Set("hint", New(KHint, "expressions", exprs))

	return inst
}

// ---------------------------------------------------------------------------------------------
// Join methods (query.py)
// ---------------------------------------------------------------------------------------------

// JoinOn mirrors Join.on(*expressions, append=appendMode, copy=copy).
func (e *Expr) JoinOn(expressions []*Expr, appendMode bool, copy bool) *Expr {
	join := applyConjunctionBuilder(expressions, e, "on", KNone, appendMode, copy)

	if join.KindText() == "CROSS" {
		join.Set("kind", nil)
	}

	return join
}

// JoinUsing mirrors Join.using(*expressions, append=appendMode, copy=copy).
func (e *Expr) JoinUsing(expressions []*Expr, appendMode bool, copy bool) *Expr {
	join := applyListBuilder(expressions, e, "using", appendMode, copy)

	if join.KindText() == "CROSS" {
		join.Set("kind", nil)
	}

	return join
}
