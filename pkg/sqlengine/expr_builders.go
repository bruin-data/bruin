package sqlengine

import (
	"fmt"
)

// Builder conventions:
//   - Inputs must be *Expr. The builders do not parse strings; parse them first with MaybeParse.
//   - copy == true makes a builder work on a copy of the expression it changes; false changes it
//     in place. appendMode adds to existing children instead of replacing them.
//   - Tri-state arguments (unset, true, false) are typed any and take nil, true or false.
//   - Builders panic on bad input: parse errors as parsePanic, other errors as *ValueError.

// MaybeParse parses prefix + " " + sql (just sql when prefix is empty) with dialect d; d == nil
// uses the default dialect. into == KNone parses a statement, any other kind parses sql as that
// kind. Parse failures panic with a parsePanic wrapping the ParseError.
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

// maybeParseExpr returns e, copied when copy is set. A nil e panics with the parse error
// "SQL cannot be None".
func maybeParseExpr(e *Expr, copy bool) *Expr {
	if e == nil {
		panic(parsePanic{&ParseError{Msg: "SQL cannot be None"}})
	}
	if copy {
		return e.Copy()
	}
	return e
}

func maybeCopy(e *Expr, copy bool) *Expr {
	if copy && e != nil {
		return e.Copy()
	}
	return e
}

func isWrongExpression(e *Expr, into Kind) bool {
	return e != nil && into != KNone && !e.IsA(into)
}

// builderKwargs is an insertion-ordered key/value map; setting an existing key updates the value
// and keeps the key's position.
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

// applyBuilder sets instance's arg to expression, wrapped in an into node (as intoArg) when it is
// of another kind. instance is copied first when copy is set.
func applyBuilder(expression, instance *Expr, arg string, copy bool, into Kind, intoArg string) *Expr {
	if isWrongExpression(expression, into) {
		expression = New(into, intoArg, expression)
	}
	instance = maybeCopy(instance, copy)
	expression = maybeParseExpr(expression, false)
	instance.Set(arg, expression)
	return instance
}

// applyChildListBuilder sets instance's arg to an into node holding the expressions, after the
// existing ones in appendMode. Inputs of another kind are wrapped in into first. properties holds
// extra args for the new node as alternating key/value pairs; the inputs' args other than
// expressions are set on it too, after properties.
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

// applyListBuilder sets instance's arg to the non-nil expressions, after the existing ones in
// appendMode.
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

// applyConjunctionBuilder ANDs the non-nil expressions (after the existing condition in
// appendMode) and sets the result as instance's arg, wrapped in an into node unless into is KNone.
// Without expressions it returns instance unchanged.
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

// combineConditions folds the non-nil expressions left to right into operator nodes, putting
// connector operands in parentheses when wrap is set. It panics when there is no expression;
// the message keeps its historical wording.
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

func wrapIfKind(expression *Expr, kind Kind) *Expr {
	if expression.IsA(kind) {
		return New(KParen, "this", expression)
	}
	return expression
}

// applySetOperation folds the expressions left to right into setOperation nodes. opts are extra
// args (alternating key/value pairs) set on every node. It panics on an empty list.
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

// applyCTEBuilder adds a CTE named alias for the query as to instance's WITH clause. A scalar CTE
// is wrapped in a subquery; a truthy recursive marks the WITH clause recursive.
func applyCTEBuilder(instance, alias, as *Expr, recursive, materialized any, appendMode bool, copy bool, scalar any) *Expr {
	aliasExpression := maybeParseExpr(alias, false)
	asExpression := maybeParseExpr(as, copy)
	if truthy(scalar) && !asExpression.IsA(KSubquery) {
		asExpression = New(KSubquery, "this", asExpression)
	}
	cte := New(KCTE, "this", asExpression, "alias", aliasExpression, "materialized", materialized, "scalar", scalar)
	var properties []any
	if truthy(recursive) {
		properties = []any{"recursive", recursive}
	}
	return applyChildListBuilder([]*Expr{cte}, instance, "with_", appendMode, copy, KWith, properties...)
}

func ConditionExpr(expression *Expr, copy bool) *Expr {
	return maybeParseExpr(expression, copy)
}

// AndExpr combines the expressions with AND, copying them and parenthesizing connector operands.
// nil inputs are skipped.
func AndExpr(expressions ...*Expr) *Expr { return combineConditions(expressions, KAnd, true, true) }

func AndExprOpts(expressions []*Expr, copy bool, wrap bool) *Expr {
	return combineConditions(expressions, KAnd, copy, wrap)
}

// OrExpr combines the expressions with OR, copying them and parenthesizing connector operands.
func OrExpr(expressions ...*Expr) *Expr { return combineConditions(expressions, KOr, true, true) }

func OrExprOpts(expressions []*Expr, copy bool, wrap bool) *Expr {
	return combineConditions(expressions, KOr, copy, wrap)
}

// XorExpr combines the expressions with XOR, copying them and parenthesizing connector operands.
func XorExpr(expressions ...*Expr) *Expr { return combineConditions(expressions, KXor, true, true) }

func XorExprOpts(expressions []*Expr, copy bool, wrap bool) *Expr {
	return combineConditions(expressions, KXor, copy, wrap)
}

// NotExpr negates expression, putting a connector (AND, OR, ...) in parentheses.
func NotExpr(expression *Expr, copy bool) *Expr {
	this := ConditionExpr(expression, copy)
	return New(KNot, "this", wrapIfKind(this, KConnector))
}

func ParenExpr(expression *Expr, copy bool) *Expr {
	return New(KParen, "this", maybeParseExpr(expression, copy))
}

// ToIdentifierAny converts a string or an Identifier to an Identifier; nil stays nil and any other
// expression panics.
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
		panic(&ValueError{Msg: "Name needs to be a string or an Identifier, got: " + n.Kind().Name()})
	case string:
		return ToIdentifier(n, quoted)
	}
	panic(&ValueError{Msg: fmt.Sprintf("Name needs to be a string or an Identifier, got: %T", name)})
}

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

// AliasExpr aliases expression: it sets the alias arg when the kind has one (except Window) and
// wraps the expression in an Alias otherwise.
func AliasExpr(expression *Expr, alias any, quoted *bool, copy bool) *Expr {
	return aliasImpl(expression, alias, false, nil, quoted, copy)
}

// AliasTableExpr sets a TableAlias on expression. columns holds the optional column names
// (string or Identifier).
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

// ColumnExpr builds a column reference, extended into a Dot chain by fields. col is a string,
// Identifier or Star; table/db/catalog/fields entries are nil, strings or Identifiers.
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

// TableExpr builds a table reference from its name parts and an optional alias; unset parts (nil
// or empty) are left out.
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

// VarChecked builds a Var named name; an empty name panics with a ValueError.
func VarChecked(name string) *Expr {
	if name == "" {
		panic(&ValueError{Msg: "Cannot convert empty name into var."})
	}
	return New(KVar, "this", name)
}

// VarOf builds a Var from an expression's name; a nil expression panics.
func VarOf(e *Expr) *Expr {
	if e == nil {
		panic(&ValueError{Msg: "Cannot convert empty name into var."})
	}
	return New(KVar, "this", e.Name())
}

// DataTypeBuild builds a DataType from a DType, a type string (parsed with d) or an expression.
// With udt, unparsable strings and Identifier or Dot expressions become user-defined types.
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
			panic(&ValueError{Msg: "Invalid data type: " + x.Kind().Name() + ". Expected str or DType"})
		}
	}
	panic(&ValueError{Msg: fmt.Sprintf("Invalid data type: %T. Expected str or DType", dtype)})
}

// dataTypeFromStr parses a type string with d (nil: default dialect). With udt, a string that
// does not parse becomes a user-defined type instead of a parse error.
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

// DataTypeIsType reports whether dt matches any of dtypes. Parameterized and user-defined types,
// and nullable ones when checkNullable is set, must match exactly; otherwise only the base type
// is compared.
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

// CastExpr casts expression to `to`, a DType, a type string or a DataType expression; d == nil
// uses the default dialect. A cast that already targets an equivalent type is returned as is.
func CastExpr(expression *Expr, to any, copy bool, d *Dialect) *Expr {
	expr := maybeParseExpr(expression, copy)
	dataType := DataTypeBuild(to, d, false, copy)

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

func SelectExpr(expressions ...*Expr) *Expr {
	return New(KSelect).QuerySelect(expressions, true, true)
}

func FromExpr(expression *Expr) *Expr {
	return New(KSelect).SelectFrom(expression, true)
}

// SubqueryExpr builds a Select from the query expression wrapped in a subquery with the given
// alias; expression must be a query.
func SubqueryExpr(expression *Expr, alias any, copy bool) *Expr {
	expr := chunkFAssertIs(maybeParseExpr(expression, false), KQuery).QuerySubquery(alias, copy)
	return New(KSelect).SelectFrom(expr, true)
}

// UnionExpr combines at least two queries with UNION.
func UnionExpr(expressions []*Expr, distinct bool, copy bool) *Expr {
	if len(expressions) < 2 {
		panic(&ValueError{Msg: "At least two expressions are required by `union`."})
	}
	return applySetOperation(expressions, KUnion, distinct, copy)
}

// IntersectExpr combines at least two queries with INTERSECT.
func IntersectExpr(expressions []*Expr, distinct bool, copy bool) *Expr {
	if len(expressions) < 2 {
		panic(&ValueError{Msg: "At least two expressions are required by `intersect`."})
	}
	return applySetOperation(expressions, KIntersect, distinct, copy)
}

// ExceptExpr combines at least two queries with EXCEPT.
func ExceptExpr(expressions []*Expr, distinct bool, copy bool) *Expr {
	if len(expressions) < 2 {
		panic(&ValueError{Msg: "At least two expressions are required by `except_`."})
	}
	return applySetOperation(expressions, KExcept, distinct, copy)
}

func TupleExpr(expressions []*Expr, copy bool) *Expr {
	out := make([]*Expr, 0, len(expressions))
	for _, e := range expressions {
		out = append(out, maybeParseExpr(e, copy))
	}
	return New(KTuple, "expressions", out)
}

func ArrayExpr(expressions []*Expr, copy bool) *Expr {
	out := make([]*Expr, 0, len(expressions))
	for _, e := range expressions {
		out = append(out, maybeParseExpr(e, copy))
	}
	return New(KArray, "expressions", out)
}

// CaseExpr builds a CASE without WHEN branches; expression, the CASE operand, may be nil.
func CaseExpr(expression *Expr, copy bool) *Expr {
	var this *Expr
	if expression != nil {
		this = maybeParseExpr(expression, copy)
	}
	return New(KCase, "this", this, "ifs", []*Expr{})
}

// ReplaceChildren replaces every child of expression with fn(child); fn returns nil, an *Expr or
// a []*Expr.
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

// ReplaceTree replaces nodes with fn(node) in reverse DFS order, leaves first; replacement nodes
// are visited too. It returns the result of the last fn call.
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

// ColumnTableNames returns the table names that columns in expression refer to, except exclude.
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

func chunkFAssertIs(e *Expr, kind Kind) *Expr {
	if !e.IsA(kind) {
		panic(&ValueError{Msg: chunkFPyStr(e) + " is not " + kind.Name() + "."})
	}
	return e
}

func unnestMethod(e *Expr) *Expr {
	if e.IsA(KSubquery) {
		return e.UnnestSubquery()
	}
	return e.Unnest()
}

func (e *Expr) ExprAnd(expressions []*Expr, copy bool, wrap bool) *Expr {
	return AndExprOpts(append([]*Expr{e}, expressions...), copy, wrap)
}

func (e *Expr) ExprOr(expressions []*Expr, copy bool, wrap bool) *Expr {
	return OrExprOpts(append([]*Expr{e}, expressions...), copy, wrap)
}

func (e *Expr) ExprNot(copy bool) *Expr { return NotExpr(e, copy) }

func (e *Expr) ExprAs(alias any, quoted *bool, copy bool) *Expr {
	return AliasExpr(e, alias, quoted, copy)
}

// QuerySelect adds projections to a Select, to both sides of a set operation, or to the query
// inside a subquery.
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

// QuerySubquery wraps e in a subquery. alias: nil, string or *Expr.
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

// QueryLimit sets LIMIT; an expression that is not a Limit is wrapped in one.
func (e *Expr) QueryLimit(expression *Expr, copy bool) *Expr {
	return applyBuilder(expression, e, "limit", copy, KLimit, "expression")
}

// QueryOffset sets OFFSET; an expression that is not an Offset is wrapped in one.
func (e *Expr) QueryOffset(expression *Expr, copy bool) *Expr {
	return applyBuilder(expression, e, "offset", copy, KOffset, "expression")
}

func (e *Expr) QueryOrderBy(expressions []*Expr, appendMode bool, copy bool) *Expr {
	return applyChildListBuilder(expressions, e, "order", appendMode, copy, KOrder)
}

// QueryWhere ANDs the conditions into WHERE; Where nodes among them are unwrapped first.
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

// QueryWith adds a CTE named alias for the query as (see applyCTEBuilder). recursive,
// materialized and scalar take nil, true or false.
func (e *Expr) QueryWith(alias, as *Expr, recursive, materialized any, appendMode bool, copy bool, scalar any) *Expr {
	return applyCTEBuilder(e, alias, as, recursive, materialized, appendMode, copy, scalar)
}

func (e *Expr) QueryUnion(expressions []*Expr, distinct bool, copy bool) *Expr {
	return UnionExpr(append([]*Expr{e}, expressions...), distinct, copy)
}

func (e *Expr) QueryIntersect(expressions []*Expr, distinct bool, copy bool) *Expr {
	return IntersectExpr(append([]*Expr{e}, expressions...), distinct, copy)
}

func (e *Expr) QueryExcept(expressions []*Expr, distinct bool, copy bool) *Expr {
	return ExceptExpr(append([]*Expr{e}, expressions...), distinct, copy)
}

// SelectFrom sets FROM; an expression that is not a From is wrapped in one.
func (e *Expr) SelectFrom(expression *Expr, copy bool) *Expr {
	return applyBuilder(expression, e, "from_", copy, KFrom, "this")
}

// SelectGroupBy sets or extends GROUP BY. Without expressions it returns e, copied when copy is
// set.
func (e *Expr) SelectGroupBy(expressions []*Expr, appendMode bool, copy bool) *Expr {
	if len(expressions) == 0 {
		if !copy {
			return e
		}
		return e.Copy()
	}

	return applyChildListBuilder(expressions, e, "group", appendMode, copy, KGroup)
}

func (e *Expr) SelectSortBy(expressions []*Expr, appendMode bool, copy bool) *Expr {
	return applyChildListBuilder(expressions, e, "sort", appendMode, copy, KSort)
}

func (e *Expr) SelectClusterBy(expressions []*Expr, appendMode bool, copy bool) *Expr {
	return applyChildListBuilder(expressions, e, "cluster", appendMode, copy, KCluster)
}

func (e *Expr) SelectLateral(expressions []*Expr, appendMode bool, copy bool) *Expr {
	return applyListBuilder(expressions, e, "laterals", appendMode, copy)
}

// SelectJoin adds a join with expression, wrapping a SELECT in a subquery. joinType, such as
// "LEFT OUTER", sets the join's method, side and kind ("" for none; it is parsed with the default
// dialect); joinAlias is nil, a string or an Identifier.
func (e *Expr) SelectJoin(expression *Expr, on []*Expr, using []*Expr, appendMode bool, joinType string, joinAlias any, copy bool) *Expr {
	expression = maybeParseExpr(expression, false)

	// A bare table is cross joined unless a condition or join type says otherwise
	join := expression
	if !expression.IsA(KJoin) {
		join = New(KJoin, "this", expression, "kind", "CROSS")
	}

	if join.KindText() == "CROSS" && (len(on) > 0 || len(using) > 0 || joinType != "") {
		join.Set("kind", nil)
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

// SelectHaving ANDs the conditions into HAVING.
func (e *Expr) SelectHaving(expressions []*Expr, appendMode bool, copy bool) *Expr {
	return applyConjunctionBuilder(expressions, e, "having", KHaving, appendMode, copy)
}

func (e *Expr) SelectWindow(expressions []*Expr, appendMode bool, copy bool) *Expr {
	return applyListBuilder(expressions, e, "windows", appendMode, copy)
}

// SelectQualify ANDs the conditions into QUALIFY.
func (e *Expr) SelectQualify(expressions []*Expr, appendMode bool, copy bool) *Expr {
	return applyConjunctionBuilder(expressions, e, "qualify", KQualify, appendMode, copy)
}

// SelectDistinct sets DISTINCT, as DISTINCT ON (ons) when ons are given, or removes it when
// distinct is false.
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

// SelectLock sets a single row lock: FOR UPDATE when update is set, FOR SHARE otherwise.
func (e *Expr) SelectLock(update bool, copy bool) *Expr {
	inst := maybeCopy(e, copy)
	inst.Set("locks", []*Expr{New(KLock, "update", update)})

	return inst
}

func (e *Expr) SelectHint(hints []*Expr, copy bool) *Expr {
	inst := maybeCopy(e, copy)
	exprs := make([]*Expr, 0, len(hints))
	for _, h := range hints {
		exprs = append(exprs, maybeParseExpr(h, copy))
	}
	inst.Set("hint", New(KHint, "expressions", exprs))

	return inst
}

// JoinOn ANDs the conditions into ON; a CROSS join loses its CROSS kind.
func (e *Expr) JoinOn(expressions []*Expr, appendMode bool, copy bool) *Expr {
	join := applyConjunctionBuilder(expressions, e, "on", KNone, appendMode, copy)

	if join.KindText() == "CROSS" {
		join.Set("kind", nil)
	}

	return join
}

// JoinUsing sets or extends USING; a CROSS join loses its CROSS kind.
func (e *Expr) JoinUsing(expressions []*Expr, appendMode bool, copy bool) *Expr {
	join := applyListBuilder(expressions, e, "using", appendMode, copy)

	if join.KindText() == "CROSS" {
		join.Set("kind", nil)
	}

	return join
}
