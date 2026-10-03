package sqlengine

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// Generator chunk C (part 2): sqlglot/generator.py check_sql .. parsedatetime_sql, plus helpers.

// check_sql (generator.py L3701).
func (g *Generator) checkSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	return "CHECK (" + this + ")"
}

// foreignkey_sql (generator.py L3705).
func (g *Generator) foreignkeySQL(expression *Expr) string {
	expressions := g.expressions(expression, exprsOpts{flat: true})
	if expressions != "" {
		expressions = " (" + expressions + ")"
	}
	reference := g.sqlKey(expression, "reference")
	if reference != "" {
		reference = " " + reference
	}
	del := g.sqlKey(expression, "delete")
	if del != "" {
		del = " ON DELETE " + del
	}
	update := g.sqlKey(expression, "update")
	if update != "" {
		update = " ON UPDATE " + update
	}
	options := g.expressions(expression, exprsOpts{key: "options", flat: true, sep: strp2(" ")})
	if options != "" {
		options = " " + options
	}
	return "FOREIGN KEY" + expressions + reference + del + update + options
}

// primarykey_sql (generator.py L3718).
func (g *Generator) primarykeySQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	if this != "" {
		this = " " + this
	}
	expressions := g.expressions(expression, exprsOpts{flat: true})
	include := g.sqlKey(expression, "include")
	options := g.expressions(expression, exprsOpts{key: "options", flat: true, sep: strp2(" ")})
	if options != "" {
		options = " " + options
	}
	return "PRIMARY KEY" + this + " (" + expressions + ")" + include + options
}

// timeserieskey_sql (generator.py L3727).
func (g *Generator) baseTimeserieskeySQL(expression *Expr) string {
	g.unsupported("TIMESERIES primary key columns are not supported")
	return g.sqlKey(expression, "this")
}

// if_sql (generator.py L3731).
func (g *Generator) ifSQL(expression *Expr) string {
	return g.caseSQL(New(KCase, "ifs", []*Expr{expression}, "default", expression.Arg("false")))
}

// matchagainst_sql (generator.py L3734).
func (g *Generator) baseMatchagainstSQL(expression *Expr) string {
	var expressions []any
	if truthy(g.s.MATCH_AGAINST_TABLE_PREFIX) {
		for _, expr := range expression.Expressions() {
			if expr.IsA(KTable) {
				expressions = append(expressions, "TABLE "+g.sql(expr))
			} else {
				expressions = append(expressions, expr)
			}
		}
	} else {
		expressions = gchunkCExprsToAny(expression.Expressions())
	}

	modifier := ""
	if m := expression.Arg("modifier"); truthy(m) {
		modifier = " " + gchunkCPyStr(m)
	}
	return g.fn("MATCH", expressions...) + " AGAINST(" + g.sqlKey(expression, "this") + modifier + ")"
}

// jsonkeyvalue_sql (generator.py L3751).
func (g *Generator) jsonkeyvalueSQL(expression *Expr) string {
	return g.sqlKey(expression, "this") + g.s.JSON_KEY_VALUE_PAIR_SEP + " " + g.sqlKey(expression, "expression")
}

// jsonpath_sql (generator.py L3754).
func (g *Generator) baseJsonpathSQL(expression *Expr) string {
	path := strings.TrimLeft(g.expressions(expression, exprsOpts{sep: strp2(""), flat: true}), ".")

	if g.s.QUOTE_JSON_PATH {
		path = g.d.S.QUOTE_START + path + g.d.S.QUOTE_END
	}

	return path
}

// json_path_part (generator.py L3762). expression is an int, a string or a JSONPathPart *Expr.
func (g *Generator) jsonPathPart(expression any) string {
	switch x := expression.(type) {
	case *Expr:
		if x.IsA(KJSONPathPart) {
			transform := g.s.TRANSFORMS[x.Kind()]
			if transform == nil {
				g.unsupported("Unsupported JSONPathPart type " + x.Kind().Name())
				return ""
			}

			return transform(g, x)
		}
	case bool:
		// Python bools are ints: str(True) == "True"
		if x {
			return "True"
		}
		return "False"
	case int:
		return strconv.Itoa(x)
	}

	s, _ := expression.(string)
	var escaped string
	if g.quoteJSONPathKeyUsingBrackets && g.s.JSON_PATH_SINGLE_QUOTE_ESCAPE {
		// sqlglot computes an escaped value and then discards it (uses the raw key).
		escaped = "\\'" + s + "\\'"
	} else {
		escaped = strings.ReplaceAll(s, `"`, `\"`)
		escaped = `"` + escaped + `"`
	}

	return escaped
}

// formatjson_sql (generator.py L3783).
func (g *Generator) formatjsonSQL(expression *Expr) string {
	return g.sqlKey(expression, "this") + " FORMAT JSON"
}

// formatphrase_sql (generator.py L3786).
func (g *Generator) formatphraseSQL(expression *Expr) string {
	// Output the Teradata column FORMAT override.
	// https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Types-and-Literals/Data-Type-Formats-and-Format-Phrases/FORMAT
	this := g.sqlKey(expression, "this")
	fmt := g.sqlKey(expression, "format")
	return this + " (FORMAT " + fmt + ")"
}

// _jsonobject_sql (generator.py L3793).
func (g *Generator) jsonobjectSQL(expression *Expr, name string) string {
	nullHandling := ""
	if v := expression.Arg("null_handling"); truthy(v) {
		nullHandling = " " + gchunkCPyStr(v)
	}

	uniqueKeys := ""
	if v := expression.Arg("unique_keys"); v != nil {
		if truthy(v) {
			uniqueKeys = " WITH UNIQUE KEYS"
		} else {
			uniqueKeys = " WITHOUT UNIQUE KEYS"
		}
	}

	returnType := g.sqlKey(expression, "return_type")
	if returnType != "" {
		returnType = " RETURNING " + returnType
	}
	encoding := g.sqlKey(expression, "encoding")
	if encoding != "" {
		encoding = " ENCODING " + encoding
	}

	if name == "" {
		if expression.IsA(KJSONObject) {
			name = "JSON_OBJECT"
		} else {
			name = "JSON_OBJECTAGG"
		}
	}

	return g.funcFull(
		name,
		"(",
		nullHandling+uniqueKeys+returnType+encoding+")",
		true,
		gchunkCExprsToAny(expression.Expressions())...,
	)
}

// jsonarray_sql (generator.py L3819).
func (g *Generator) jsonarraySQL(expression *Expr) string {
	nullHandling := ""
	if v := expression.Arg("null_handling"); truthy(v) {
		nullHandling = " " + gchunkCPyStr(v)
	}
	returnType := g.sqlKey(expression, "return_type")
	if returnType != "" {
		returnType = " RETURNING " + returnType
	}
	strict := ""
	if expression.ArgB("strict") {
		strict = " STRICT"
	}
	return g.funcFull(
		"JSON_ARRAY", "(", nullHandling+returnType+strict+")", true, gchunkCExprsToAny(expression.Expressions())...,
	)
}

// jsonarrayagg_sql (generator.py L3829).
func (g *Generator) jsonarrayaggSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	order := g.sqlKey(expression, "order")
	nullHandling := ""
	if v := expression.Arg("null_handling"); truthy(v) {
		nullHandling = " " + gchunkCPyStr(v)
	}
	returnType := g.sqlKey(expression, "return_type")
	if returnType != "" {
		returnType = " RETURNING " + returnType
	}
	strict := ""
	if expression.ArgB("strict") {
		strict = " STRICT"
	}
	return g.funcFull(
		"JSON_ARRAYAGG",
		"(",
		order+nullHandling+returnType+strict+")",
		true,
		this,
	)
}

// jsoncolumndef_sql (generator.py L3843).
func (g *Generator) jsoncolumndefSQL(expression *Expr) string {
	path := g.sqlKey(expression, "path")
	if path != "" {
		path = " PATH " + path
	}
	nestedSchema := g.sqlKey(expression, "nested_schema")

	if nestedSchema != "" {
		return "NESTED" + path + " " + nestedSchema
	}

	this := g.sqlKey(expression, "this")
	kind := g.sqlKey(expression, "kind")
	if kind != "" {
		kind = " " + kind
	}
	formatJSON := ""
	if expression.ArgB("format_json") {
		formatJSON = " FORMAT JSON"
	}

	ordinality := ""
	if expression.ArgB("ordinality") {
		ordinality = " FOR ORDINALITY"
	}
	return this + kind + formatJSON + path + ordinality
}

// jsonschema_sql (generator.py L3859).
func (g *Generator) jsonschemaSQL(expression *Expr) string {
	return g.fn("COLUMNS", gchunkCExprsToAny(expression.Expressions())...)
}

// jsontable_sql (generator.py L3862).
func (g *Generator) jsontableSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	path := g.sqlKey(expression, "path")
	if path != "" {
		path = ", " + path
	}
	errorHandling := ""
	if v := expression.Arg("error_handling"); truthy(v) {
		errorHandling = " " + gchunkCPyStr(v)
	}
	emptyHandling := ""
	if v := expression.Arg("empty_handling"); truthy(v) {
		emptyHandling = " " + gchunkCPyStr(v)
	}
	schema := g.sqlKey(expression, "schema")
	return g.funcFull(
		"JSON_TABLE", "(", path+errorHandling+emptyHandling+" "+schema+")", true, this,
	)
}

// openjsoncolumndef_sql (generator.py L3875).
func (g *Generator) openjsoncolumndefSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	kind := g.sqlKey(expression, "kind")
	path := g.sqlKey(expression, "path")
	if path != "" {
		path = " " + path
	}
	asJSON := ""
	if expression.ArgB("as_json") {
		asJSON = " AS JSON"
	}
	return this + " " + kind + path + asJSON
}

// openjson_sql (generator.py L3883).
func (g *Generator) openjsonSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	path := g.sqlKey(expression, "path")
	if path != "" {
		path = ", " + path
	}
	expressions := g.expressions(expression, exprsOpts{})
	with := ""
	if expressions != "" {
		with = " WITH (" + g.segSep(g.indentDefault(expressions), "") + g.segSep(")", "")
	}
	return "OPENJSON(" + this + path + ")" + with
}

// in_sql (generator.py L3895).
func (g *Generator) baseInSQL(expression *Expr) string {
	query := expression.ArgE("query")
	unnest := expression.ArgE("unnest")
	field := expression.ArgE("field")
	isGlobal := ""
	if expression.ArgB("is_global") {
		isGlobal = " GLOBAL"
	}

	var inSQL string
	if query != nil {
		inSQL = g.sql(query)
	} else if unnest != nil {
		inSQL = g.inUnnestOp(unnest)
	} else if field != nil {
		inSQL = g.sql(field)
	} else {
		inSQL = "(" + g.expressions(expression, exprsOpts{dynamic: true, newLine: true, skipFirst: true, skipLast: true}) + ")"
	}

	return g.sqlKey(expression, "this") + isGlobal + " IN " + inSQL
}

// in_unnest_op (generator.py L3912).
func (g *Generator) baseInUnnestOp(unnest *Expr) string {
	return "(SELECT " + g.sql(unnest) + ")"
}

// interval_sql (generator.py L3915).
func (g *Generator) baseIntervalSQL(expression *Expr) string {
	unitExpression := expression.ArgE("unit")
	unit := ""
	if unitExpression != nil {
		unit = g.sql(unitExpression)
	}
	if !g.s.INTERVAL_ALLOWS_PLURAL_FORM {
		if v, ok := g.s.TIME_PART_SINGULARS[unit]; ok {
			unit = v
		}
	}
	if unit != "" {
		unit = " " + unit
	}

	if g.s.SINGLE_STRING_INTERVAL {
		this := ""
		if expression.This() != nil {
			this = expression.This().Name()
		}
		if this != "" {
			if unitExpression != nil && unitExpression.IsA(KIntervalSpan) {
				return "INTERVAL '" + this + "'" + unit
			}
			return "INTERVAL '" + this + unit + "'"
		}
		return "INTERVAL" + unit
	}

	this := g.sqlKey(expression, "this")
	if this != "" {
		unwrapped := expression.This().IsA(g.s.UNWRAPPED_INTERVAL_VALUES...)
		if unwrapped {
			this = " " + this
		} else {
			this = " (" + this + ")"
		}
	}

	return "INTERVAL" + this + unit
}

// return_sql (generator.py L3937).
func (g *Generator) returnSQL(expression *Expr) string {
	return "RETURN " + g.sqlKey(expression, "this")
}

// reference_sql (generator.py L3940).
func (g *Generator) referenceSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	expressions := g.expressions(expression, exprsOpts{flat: true})
	if expressions != "" {
		expressions = "(" + expressions + ")"
	}
	options := g.expressions(expression, exprsOpts{key: "options", flat: true, sep: strp2(" ")})
	if options != "" {
		options = " " + options
	}
	return "REFERENCES " + this + expressions + options
}

// anonymous_sql (generator.py L3948).
func (g *Generator) anonymousSQL(expression *Expr) string {
	// We don't normalize qualified functions such as a.b.foo(), because they can be case-sensitive
	parent := expression.Parent()
	isQualified := parent.IsA(KDot) && expression == parent.Expression()

	return g.funcFull(
		g.sqlKey(expression, "this"), "(", ")", !isQualified, gchunkCExprsToAny(expression.Expressions())...,
	)
}

// paren_sql (generator.py L3957).
func (g *Generator) parenSQL(expression *Expr) string {
	sql := g.segSep(g.indentDefault(g.sqlKey(expression, "this")), "")
	return "(" + sql + g.segSep(")", "")
}

// neg_sql (generator.py L3961).
func (g *Generator) negSQL(expression *Expr) string {
	// This makes sure we don't convert "- - 5" to "--5", which is a comment
	thisSQL := g.sqlKey(expression, "this")
	sep := ""
	if thisSQL[0] == '-' { // panics on "" like Python's IndexError
		sep = " "
	}
	return "-" + sep + thisSQL
}

// not_sql (generator.py L3967).
func (g *Generator) baseNotSQL(expression *Expr) string {
	return "NOT " + g.sqlKey(expression, "this")
}

// alias_sql (generator.py L3970).
func (g *Generator) aliasSQL(expression *Expr) string {
	alias := g.sqlKey(expression, "alias")
	if alias != "" {
		alias = " AS " + alias
	}
	return g.sqlKey(expression, "this") + alias
}

// pivotalias_sql (generator.py L3975).
func (g *Generator) pivotaliasSQL(expression *Expr) string {
	alias := expression.ArgE("alias")

	parent := expression.Parent()
	var pivot *Expr
	if parent != nil {
		pivot = parent.Parent()
	}

	if pivot.IsA(KPivot) && pivot.ArgB("unpivot") {
		identifierAlias := alias.IsA(KIdentifier)
		literalAlias := alias.IsA(KLiteral)

		if identifierAlias && !g.s.UNPIVOT_ALIASES_ARE_IDENTIFIERS {
			alias.Replace(LiteralString(alias.OutputName()))
		} else if !identifierAlias && literalAlias && g.s.UNPIVOT_ALIASES_ARE_IDENTIFIERS {
			alias.Replace(ToIdentifier(alias.OutputName(), nil))
		}
	}

	return g.aliasSQL(expression)
}

// aliases_sql (generator.py L3992).
func (g *Generator) baseAliasesSQL(expression *Expr) string {
	return g.sqlKey(expression, "this") + " AS (" + g.expressions(expression, exprsOpts{flat: true}) + ")"
}

// atindex_sql (generator.py L3995).
func (g *Generator) atindexSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	index := g.sqlKey(expression, "expression")
	return this + " AT " + index
}

// attimezone_sql (generator.py L4000).
func (g *Generator) baseAttimezoneSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	zone := g.sqlKey(expression, "zone")
	return this + " AT TIME ZONE " + zone
}

// fromtimezone_sql (generator.py L4005).
func (g *Generator) fromtimezoneSQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	zone := g.sqlKey(expression, "zone")
	return this + " AT TIME ZONE " + zone + " AT TIME ZONE 'UTC'"
}

// fromiso8601date_sql (generator.py L4010).
func (g *Generator) fromiso8601dateSQL(expression *Expr) string {
	return g.sql(gchunkCCast(expression.This(), DT_DATE))
}

// fromiso8601timestamp_sql (generator.py L4013).
func (g *Generator) fromiso8601timestampSQL(expression *Expr) string {
	return g.sql(gchunkCCast(expression.This(), DT_TIMESTAMPTZ))
}

// fromiso8601timestampnanos_sql (generator.py L4016).
func (g *Generator) fromiso8601timestampnanosSQL(expression *Expr) string {
	return g.sql(gchunkCCast(expression.This(), DT_TIMESTAMPTZ))
}

// add_sql (generator.py L4019).
func (g *Generator) addSQL(expression *Expr) string {
	return g.binary(expression, "+")
}

// and_sql (generator.py L4022).
func (g *Generator) andSQL(expression *Expr, stack []any) string {
	return g.connectorSQL(expression, "AND", stack)
}

// or_sql (generator.py L4025).
func (g *Generator) orSQL(expression *Expr, stack []any) string {
	return g.connectorSQL(expression, "OR", stack)
}

// xor_sql (generator.py L4028).
func (g *Generator) xorSQL(expression *Expr, stack []any) string {
	return g.connectorSQL(expression, "XOR", stack)
}

// connector_sql (generator.py L4031)
//
// Python passes the stack list by reference and the callee appends to it. A Go slice cannot grow
// in place, so the loop below calls connectorPush (the `stack is not None` branch) with a pointer.
// A non-nil stack passed from outside only mirrors the return value.
func (g *Generator) connectorSQL(expression *Expr, op string, stack []any) string {
	if stack != nil {
		return g.connectorPush(expression, op, &stack)
	}

	stack = []any{expression}
	var sqls []string
	ops := map[string]bool{}

	for len(stack) > 0 {
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if n, ok := node.(*Expr); ok && n.IsA(KConnector) {
			// getattr(self, f"{node.key}_sql")(node, stack): and_sql / or_sql / xor_sql
			ops[g.connectorPush(n, gchunkCConnectorOp(n), &stack)] = true
		} else {
			sql := g.sql(node)
			if len(sqls) > 0 && ops[sqls[len(sqls)-1]] {
				sqls[len(sqls)-1] += " " + sql
			} else {
				sqls = append(sqls, sql)
			}
		}
	}

	sep := " "
	if g.pretty && g.tooWide(sqls) {
		sep = "\n"
	}
	return strings.Join(sqls, sep)
}

// connectorPush is the `stack is not None` branch of connector_sql.
func (g *Generator) connectorPush(expression *Expr, op string, stack *[]any) string {
	if len(expression.Expressions()) > 0 {
		*stack = append(*stack, g.expressions(expression, exprsOpts{sep: strp2(" " + op + " ")}))
	} else {
		*stack = append(*stack, expression.Right())
		if len(expression.Comments()) > 0 && g.comments {
			op = g.maybeCommentC(op, nil, expression.Comments())
		}
		*stack = append(*stack, op, expression.Left())
	}
	return op
}

// gchunkCConnectorOp maps a Connector node to the op its <key>_sql method passes to connector_sql.
func gchunkCConnectorOp(e *Expr) string {
	switch e.Kind() {
	case KAnd:
		return "AND"
	case KOr:
		return "OR"
	case KXor:
		return "XOR"
	}
	panic(&ValueError{Msg: "'Generator' object has no attribute '" + e.Key() + "_sql'"})
}

// bitwiseand_sql (generator.py L4065).
func (g *Generator) bitwiseandSQL(expression *Expr) string {
	return g.binary(expression, "&")
}

// bitwiseleftshift_sql (generator.py L4068).
func (g *Generator) bitwiseleftshiftSQL(expression *Expr) string {
	return g.binary(expression, "<<")
}

// bitwisenot_sql (generator.py L4071).
func (g *Generator) baseBitwisenotSQL(expression *Expr) string {
	return "~" + g.sqlKey(expression, "this")
}

// bitwiseor_sql (generator.py L4074).
func (g *Generator) bitwiseorSQL(expression *Expr) string {
	return g.binary(expression, "|")
}

// bitwiserightshift_sql (generator.py L4077).
func (g *Generator) bitwiserightshiftSQL(expression *Expr) string {
	return g.binary(expression, ">>")
}

// bitwisexor_sql (generator.py L4080).
func (g *Generator) baseBitwisexorSQL(expression *Expr) string {
	return g.binary(expression, "^")
}

// cast_sql (generator.py L4083). safePrefix "" stands for None.
func (g *Generator) baseCastSQL(expression *Expr, safePrefix string) string {
	formatSQL := g.sqlKey(expression, "format")
	if formatSQL != "" {
		formatSQL = " FORMAT " + formatSQL
	}
	toSQL := g.sqlKey(expression, "to")
	if toSQL != "" {
		toSQL = " " + toSQL
	}
	action := g.sqlKey(expression, "action")
	if action != "" {
		action = " " + action
	}
	def := g.sqlKey(expression, "default")
	if def != "" {
		def = " DEFAULT " + def + " ON CONVERSION ERROR"
	}
	return safePrefix + "CAST(" + g.sqlKey(expression, "this") + " AS" + toSQL + def + formatSQL + action + ")"
}

// strtotime_sql (generator.py L4095)
// Base implementation that excludes safe, zone, and target_type metadata args.
func (g *Generator) baseStrtotimeSQL(expression *Expr) string {
	return g.fn("STR_TO_TIME", expression.Arg("this"), expression.Arg("format"))
}

// strtodate_sql (generator.py L4099)
// Base implementation that excludes the safe and default_year metadata args.
func (g *Generator) baseStrtodateSQL(expression *Expr) string {
	return g.fn("STR_TO_DATE", expression.Arg("this"), expression.Arg("format"))
}

// parsedatetime_sql (generator.py L4102).
func (g *Generator) baseParsedatetimeSQL(expression *Expr) string {
	return g.fn(
		"PARSE_DATETIME",
		expression.Arg("this"),
		expression.Arg("format"),
		expression.Arg("zone"),
	)
}

// ---------------------------------------------------------------------------------------------
// Helpers used by generator chunk C (prefixed gchunkC so they can be unified later).

// gchunkCCsv mirrors sqlglot.helper.csv(*args, sep=sep).
func gchunkCCsv(sep string, args ...string) string {
	var parts []string
	for _, a := range args {
		if a != "" {
			parts = append(parts, a)
		}
	}
	return strings.Join(parts, sep)
}

// gchunkCExprsToAny converts an expression list into variadic args for g.fn / g.funcFull.
func gchunkCExprsToAny(list []*Expr) []any {
	out := make([]any, len(list))
	for i, e := range list {
		out[i] = e
	}
	return out
}

// gchunkCPyStr mirrors Python str(value) for raw arg values interpolated in f-strings.
func gchunkCPyStr(v any) string {
	switch x := v.(type) {
	case nil:
		return "None"
	case string:
		return x
	case *Expr:
		if x == nil {
			return "None"
		}
		return exprSQL(x)
	case bool:
		if x {
			return "True"
		}
		return "False"
	case int:
		return strconv.Itoa(x)
	}
	return fmt.Sprint(v)
}

// gchunkCWrap mirrors expressions.core._wrap(expression, kind).
func gchunkCWrap(e *Expr, kind Kind) *Expr {
	if e.IsA(kind) {
		return Paren(e)
	}
	return e
}

var (
	gchunkCBaseTypeMappingOnce sync.Once
	gchunkCBaseTypeMappingVal  map[DType]string
)

// gchunkCBaseTypeMapping is Dialect.get_or_raise(None).generator_class.TYPE_MAPPING.
func gchunkCBaseTypeMapping() map[DType]string {
	gchunkCBaseTypeMappingOnce.Do(func() {
		gchunkCBaseTypeMappingVal = generatorSettings_base().TYPE_MAPPING
	})
	return gchunkCBaseTypeMappingVal
}

// gchunkCDataTypeIsType mirrors DataType.is_type(dtype) for a single DType (check_nullable=False).
func gchunkCDataTypeIsType(dt *Expr, dtype DType) bool {
	if dt == nil {
		return false
	}
	other := NewDataType(dtype)
	if dt.DTypeOf() == DT_USERDEFINED || dtype == DT_USERDEFINED {
		return dt.Equal(other)
	}
	return dt.DTypeOf() == dtype
}

// gchunkCIsType mirrors Expr.is_type(dtype), including the DataType and Cast overrides.
func gchunkCIsType(e *Expr, dtype DType) bool {
	if e == nil {
		return false
	}
	if e.Kind().isDataType() {
		return gchunkCDataTypeIsType(e, dtype)
	}
	if e.IsA(KCast) {
		return gchunkCDataTypeIsType(e.ArgE("to"), dtype)
	}
	t := e.RawType()
	return t != nil && gchunkCDataTypeIsType(t, dtype)
}

// gchunkCCast mirrors exp.cast(expression, to) for a DType target (copy=True, dialect=None).
func gchunkCCast(expression *Expr, to DType) *Expr {
	if expression == nil {
		panic(&ParseError{Msg: "SQL cannot be None"})
	}
	expr := expression.Copy()
	dataType := NewDataType(to)

	// dont re-cast if the expression is already a cast to the correct type
	if expr.IsA(KCast) {
		typeMapping := gchunkCBaseTypeMapping()

		existingCastType := expr.ArgE("to").DTypeOf()
		newCastType := dataType.DTypeOf()
		existing, ok := typeMapping[existingCastType]
		if !ok {
			existing = dtypeValues[existingCastType]
		}
		newMapped, ok := typeMapping[newCastType]
		if !ok {
			newMapped = dtypeValues[newCastType]
		}
		typesAreEquivalent := existing == newMapped

		if gchunkCIsType(expr, to) || typesAreEquivalent {
			return expr
		}
	}

	c := New(KCast, "this", expr, "to", dataType)
	c.SetType(dataType)

	return c
}

// gchunkCIs mirrors Expr.is_(other) (Expr._binop(Is, other)).
func gchunkCIs(self, other *Expr) *Expr {
	this := self.Copy()
	other = other.Copy()
	if !this.IsA(KIs) && !other.IsA(KIs) {
		this = gchunkCWrap(this, KBinary)
		other = gchunkCWrap(other, KBinary)
	}
	return New(KIs, "this", this, "expression", other)
}

// gchunkCOr mirrors exp.or_(*expressions) with copy=True, wrap=True.
func gchunkCOr(expressions ...*Expr) *Expr {
	var conditions []*Expr
	for _, e := range expressions {
		if e != nil {
			conditions = append(conditions, e.Copy())
		}
	}
	if len(conditions) == 0 {
		panic(&ValueError{Msg: "not enough values to unpack (expected at least 1, got 0)"})
	}

	this, rest := conditions[0], conditions[1:]
	if len(rest) > 0 {
		this = gchunkCWrap(this, KConnector)
	}
	for _, e := range rest {
		this = New(KOr, "this", this, "expression", gchunkCWrap(e, KConnector))
	}
	return this
}

// gchunkCCaseWhen mirrors Case.when(condition, then) with copy=True.
func gchunkCCaseWhen(c *Expr, condition, then *Expr) *Expr {
	instance := c.Copy()
	instance.Append("ifs", New(KIf, "this", condition.Copy(), "true", then.Copy()))
	return instance
}

// gchunkCCaseElse mirrors Case.else_(condition) with copy=True.
func gchunkCCaseElse(c *Expr, condition *Expr) *Expr {
	instance := c.Copy()
	instance.Set("default", condition.Copy())
	return instance
}

// gchunkCMapDatePart mirrors sqlglot.dialects.dialect.map_date_part(part, dialect).
func gchunkCMapDatePart(part *Expr, d *Dialect) *Expr {
	mapped := ""
	if part != nil && !(part.IsA(KColumn) && len(part.Parts()) != 1) {
		mapped = d.S.DATE_PART_MAPPING[pyUpper(part.Name())]
	}
	if mapped != "" {
		if part.IsString() {
			return LiteralString(mapped)
		}
		return VarExpr(mapped)
	}

	return part
}

// gchunkCConcatToDpipeSQL mirrors sqlglot.dialects.dialect.concat_to_dpipe_sql.
func gchunkCConcatToDpipeSQL(g *Generator, expression *Expr) string {
	exprs := expression.Expressions()
	if len(exprs) == 0 {
		panic(&ValueError{Msg: "reduce() of empty iterable with no initial value"})
	}
	acc := exprs[0]
	for _, y := range exprs[1:] {
		acc = New(KDPipe, "this", acc, "expression", y)
	}
	return g.sql(acc)
}

// gchunkCAnnotateTypes mirrors sqlglot.optimizer.annotate_types.annotate_types(e, dialect=d).
func gchunkCAnnotateTypes(e *Expr, d *Dialect) *Expr { return annotateTypes(e, d) }

// gchunkCApplyIndexOffset mirrors sqlglot.expressions.apply_index_offset.
func gchunkCApplyIndexOffset(this *Expr, expressions []*Expr, offset int, d *Dialect) []*Expr {
	return dhApplyIndexOffset(this, expressions, offset, d)
}
