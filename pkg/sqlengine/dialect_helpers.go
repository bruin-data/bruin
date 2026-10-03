package sqlengine

// Ports of the module-level helper functions of sqlglot/dialects/dialect.py (rename_func and
// friends), plus `_with_strict_time_inverse` and the `Dialect.format_time` classmethod.
//
// Conventions:
//   - generator helpers `def x(self: Generator, expression)` -> `func x(g *Generator, e *Expr) string` (GenFunc);
//     extra parameters become extra Go parameters (Python defaults are noted in /* */).
//   - generator factories returning a callable -> return GenFunc.
//   - parser builders -> `func x(args []*Expr, d *Dialect) *Expr` (FuncBuilder); builder factories return FuncBuilder.
//   - Python `str | None` parameters are Go strings where "" means None (documented per function).
//   - helpers prefixed `dh` are local ports of sqlglot builders / helpers not (yet) available elsewhere.

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// STRICT_TIME_FORMATS mirrors dialect.STRICT_TIME_FORMATS (ordered).
// "Strict" dialects (e.g. modern Hive, Spark 3+) map their zero-padded MM/dd to these in TIME_MAPPING so they
// roundtrip, since a lax %m/%d renders non-padded there for parse expressions (see HiveGenerator.format_time).
var STRICT_TIME_FORMATS = [][2]string{{"%mstrict", "%m"}, {"%dstrict", "%d"}}

// withStrictTimeInverse mirrors dialect._with_strict_time_inverse (mutates and returns the mapping).
func withStrictTimeInverse(inverseMapping map[string]string) map[string]string {
	for _, pair := range STRICT_TIME_FORMATS {
		strictFormat, laxFormat := pair[0], pair[1]
		if v, ok := inverseMapping[strictFormat]; ok {
			// In strict dialects, a foreign lax %m formats the same padded way as %mstrict (MM)
			if _, has := inverseMapping[laxFormat]; !has {
				inverseMapping[laxFormat] = v
			}
		} else {
			// Elsewhere, the strict format degrades to its lax counterpart so it never leaks
			v, has := inverseMapping[laxFormat]
			if !has {
				v = laxFormat
			}
			inverseMapping[strictFormat] = v
		}
	}
	return inverseMapping
}

// dhFormatTimeLiteral mirrors exp.Literal.string(format_time(...)); a None result becomes 'None'
// because Literal.string calls str() on its argument.
func dhFormatTimeLiteral(s string, ok bool) *Expr {
	if !ok {
		return LiteralString("None")
	}
	return LiteralString(s)
}

// formatTimeStr mirrors Dialect.format_time(expression) for a (quoted) string argument.
func (d *Dialect) formatTimeStr(s string) *Expr {
	// the time formats are quoted
	r := []rune(s)
	inner := ""
	if len(r) >= 2 {
		inner = string(r[1 : len(r)-1])
	}
	return dhFormatTimeLiteral(formatTime(inner, d.S.TIME_MAPPING, d.timeTrie))
}

// formatTimeExpr mirrors Dialect.format_time(expression) for an expression (or None) argument.
func (d *Dialect) formatTimeExpr(e *Expr) *Expr {
	if e != nil && e.IsString() {
		return dhFormatTimeLiteral(formatTime(e.ThisS(), d.S.TIME_MAPPING, d.timeTrie))
	}
	return e
}

// dhFlattenArgs mirrors helper.flatten(expression.args.values()) as arguments for Generator.func.
func dhFlattenArgs(e *Expr) []any {
	out := make([]any, 0, len(e.args))
	for _, a := range e.args {
		switch v := a.val.(type) {
		case []*Expr:
			for _, x := range v {
				out = append(out, x)
			}
		case []string:
			for _, x := range v {
				out = append(out, x)
			}
		case int:
			// Generator.sql(0) returns "" (falsy); other ints raise like Python.
			if v == 0 {
				out = append(out, "")
			} else {
				out = append(out, v)
			}
		default:
			out = append(out, v)
		}
	}
	return out
}

// dhUnsupportedArgs mirrors the @unsupported_args(...) decorator for plain argument names.
func dhUnsupportedArgs(g *Generator, e *Expr, args ...string) {
	for _, arg := range args {
		if e.ArgB(arg) {
			g.unsupported(fmt.Sprintf("Argument '%s' is not supported for expression '%s' when targeting %s.", arg, e.Kind().Name(), g.d.ClassName))
		}
	}
}

// renameFunc mirrors dialect.rename_func.
func renameFunc(name string) GenFunc {
	return func(g *Generator, e *Expr) string {
		return g.fn(name, dhFlattenArgs(e)...)
	}
}

// bracketToElementAtSQL mirrors dialect.bracket_to_element_at_sql.
func bracketToElementAtSQL(g *Generator, e *Expr) string {
	// Python: `1 - expression.args.get("offset", 0)`; offset may be an int or a bool (True == 1).
	offset := 0
	switch v := e.Arg("offset").(type) {
	case int:
		offset = v
	case bool:
		if v {
			offset = 1
		}
	}
	index := seqGet(dhApplyIndexOffset(e.This(), e.Expressions(), 1-offset, g.d), 0)
	return g.fn("ELEMENT_AT", e.Arg("this"), index)
}

// approxCountDistinctSQL mirrors dialect.approx_count_distinct_sql.
func approxCountDistinctSQL(g *Generator, e *Expr) string {
	dhUnsupportedArgs(g, e, "accuracy")
	return g.fn("APPROX_COUNT_DISTINCT", e.Arg("this"))
}

// ifSQL mirrors dialect.if_sql(name="IF", false_value=None). falseValue may be nil, a string or an *Expr.
func ifSQL(name string /*="IF"*/, falseValue any /*=None*/) GenFunc {
	return func(g *Generator, e *Expr) string {
		f := e.Arg("false")
		if !truthy(f) {
			f = falseValue
		}
		return g.fn(name, e.Arg("this"), e.Arg("true"), f)
	}
}

// arrowJSONExtractSQL mirrors dialect.arrow_json_extract_sql.
func arrowJSONExtractSQL(g *Generator, e *Expr) string {
	this := e.This()
	if g.s.JSON_TYPE_REQUIRED_FOR_EXTRACTION && this.IsA(KLiteral) && this.IsString() {
		this.Replace(CastExpr(this, DT_JSON, true, nil))
	}
	op := "->>"
	if e.IsA(KJSONExtract) {
		op = "->"
	}
	return g.binary(e, op)
}

// inlineArraySQL mirrors dialect.inline_array_sql.
func inlineArraySQL(g *Generator, e *Expr) string {
	return "[" + g.expressions(e, exprsOpts{dynamic: true, newLine: true, skipFirst: true, skipLast: true}) + "]"
}

// inlineArrayUnlessQuery mirrors dialect.inline_array_unless_query.
func inlineArrayUnlessQuery(g *Generator, e *Expr) string {
	elem := seqGet(e.Expressions(), 0)
	if elem != nil && elem.Find(KQuery) != nil {
		return g.fn("ARRAY", elem)
	}
	return inlineArraySQL(g, e)
}

// noIlikeSQL mirrors dialect.no_ilike_sql.
func noIlikeSQL(g *Generator, e *Expr) string {
	return g.likeSQL_(New(
		KLike,
		"this", New(KLower, "this", e.This()),
		"expression", New(KLower, "this", e.Expression()),
		"negate", e.Arg("negate"),
	))
}

// noParenCurrentDateSQL mirrors dialect.no_paren_current_date_sql.
func noParenCurrentDateSQL(g *Generator, e *Expr) string {
	zone := g.sqlKey(e, "this")
	if zone != "" {
		return "CURRENT_DATE AT TIME ZONE " + zone
	}
	return "CURRENT_DATE"
}

// noRecursiveCteSQL mirrors dialect.no_recursive_cte_sql.
func noRecursiveCteSQL(g *Generator, e *Expr) string {
	if e.ArgB("recursive") {
		g.unsupported("Recursive CTEs are unsupported")
		e.Set("recursive", false)
	}
	return g.withSQL(e)
}

// noTablesampleSQL mirrors dialect.no_tablesample_sql.
func noTablesampleSQL(g *Generator, e *Expr) string {
	g.unsupported("TABLESAMPLE unsupported")
	return g.sql(e.Arg("this"))
}

// noPivotSQL mirrors dialect.no_pivot_sql.
func noPivotSQL(g *Generator, e *Expr) string {
	g.unsupported("PIVOT unsupported")
	return ""
}

// noTrycastSQL mirrors dialect.no_trycast_sql.
func noTrycastSQL(g *Generator, e *Expr) string {
	return g.castSQL(e, "")
}

// noCommentColumnConstraintSQL mirrors dialect.no_comment_column_constraint_sql.
func noCommentColumnConstraintSQL(g *Generator, e *Expr) string {
	g.unsupported("CommentColumnConstraint unsupported")
	return ""
}

// noMapFromEntriesSQL mirrors dialect.no_map_from_entries_sql.
func noMapFromEntriesSQL(g *Generator, e *Expr) string {
	g.unsupported("MAP_FROM_ENTRIES unsupported")
	return ""
}

// propertySQL mirrors dialect.property_sql.
func propertySQL(g *Generator, e *Expr) string {
	return g.propertyName(e, true) + "=" + g.sqlKey(e, "value")
}

// strpositionSQL mirrors dialect.strposition_sql.
func strpositionSQL(
	g *Generator,
	e *Expr,
	funcName string, /*="STRPOS"*/
	supportsPosition bool, /*=False*/
	supportsOccurrence bool, /*=False*/
	useAnsiPosition bool, /*=True*/
) string {
	str := e.This()
	substr := e.ArgE("substr")
	position := e.ArgE("position")
	occurrence := e.ArgE("occurrence")
	zero := LiteralInt(0)
	one := LiteralInt(1)

	if supportsOccurrence && occurrence != nil && supportsPosition && position == nil {
		position = one
	}

	transpilePosition := position != nil && !supportsPosition
	if transpilePosition {
		str = New(KSubstring, "this", str, "start", position)
	}

	var fn *Expr
	if funcName == "POSITION" && useAnsiPosition {
		fn = New(KAnonymous, "this", funcName, "expressions", []*Expr{New(KIn, "this", substr, "field", str)})
	} else {
		var args []*Expr
		if funcName == "LOCATE" || funcName == "CHARINDEX" {
			args = []*Expr{substr, str}
		} else {
			args = []*Expr{str, substr}
		}
		if supportsPosition {
			args = append(args, position)
		}
		if occurrence != nil {
			if supportsOccurrence {
				args = append(args, occurrence)
			} else {
				g.unsupported(funcName + " does not support the occurrence parameter.")
			}
		}
		fn = New(KAnonymous, "this", funcName, "expressions", args)
	}

	if transpilePosition {
		funcWithOffset := New(KSub, "this", dhBinop(KAdd, fn, position), "expression", one)
		funcWrapped := New(KIf, "this", dhBinop(KEQ, fn, zero), "true", zero, "false", funcWithOffset)
		return g.sql(funcWrapped)
	}

	return g.sql(fn)
}

// structExtractSQL mirrors dialect.struct_extract_sql.
func structExtractSQL(g *Generator, e *Expr) string {
	return g.sqlKey(e, "this") + "." + g.sql(ToIdentifier(e.Expression().Name(), nil))
}

// arrayAppendSQL mirrors dialect.array_append_sql(name, swap_params=False).
//
// Transpile ARRAY_APPEND/ARRAY_PREPEND between dialects with different NULL propagation semantics.
// Dialects that propagate NULLs need to set `ARRAY_FUNCS_PROPAGATES_NULLS` to True.
func arrayAppendSQL(name string, swapParams bool /*=False*/) GenFunc {
	return func(g *Generator, e *Expr) string {
		this := e.This()
		element := e.Expression()
		args := []any{this, element}
		if swapParams {
			args = []any{element, this}
		}
		funcSQL := g.fn(name, args...)

		sourceNullPropagation := e.ArgB("null_propagation")
		targetNullPropagation := g.d.S.ARRAY_FUNCS_PROPAGATES_NULLS

		// No transpilation needed when source and target have matching NULL semantics
		if sourceNullPropagation == targetNullPropagation {
			return funcSQL
		}

		// Source propagates NULLs, target doesn't: wrap in conditional to return NULL explicitly
		if sourceNullPropagation {
			return g.sql(New(
				KIf,
				"this", New(KIs, "this", this, "expression", New(KNull)),
				"true", New(KNull),
				"false", funcSQL,
			))
		}

		// Source doesn't propagate NULLs, target does: use COALESCE to convert NULL to empty array
		this = New(KCoalesce, "expressions", []*Expr{this, New(KArray, "expressions", []*Expr{})})
		args = []any{this, element}
		if swapParams {
			args = []any{element, this}
		}
		return g.fn(name, args...)
	}
}

// generateSeriesSQL mirrors dialect.generate_series_sql(func_name, exclusive_func_name=None).
// exclusiveFuncName "" means None.
func generateSeriesSQL(funcName string, exclusiveFuncName string /*=None*/) GenFunc {
	return func(g *Generator, e *Expr) string {
		start := e.Arg("start")
		end := e.Arg("end")
		step := e.Arg("step")

		if e.ArgB("is_end_exclusive") {
			if exclusiveFuncName != "" {
				return g.fn(exclusiveFuncName, start, end, step)
			}
			adjustedEnd := New(KSub, "this", end, "expression", LiteralInt(1))
			return g.fn(funcName, start, adjustedEnd, step)
		}

		return g.fn(funcName, start, end, step)
	}
}

// arrayConcatSQL mirrors dialect.array_concat_sql(name).
//
// Transpile ARRAY_CONCAT/ARRAY_CAT between dialects with different NULL propagation semantics.
// Dialects that propagate NULLs need to set `ARRAY_FUNCS_PROPAGATES_NULLS` to True.
func arrayConcatSQL(name string) GenFunc {
	// Build ARRAY_CONCAT call from a list of arguments, handling variadic vs binary nesting.
	buildFuncCall := func(g *Generator, funcName string, args []*Expr) string {
		if g.s.ARRAY_CONCAT_IS_VAR_LEN {
			return g.fn(funcName, dhExprsToAny(args)...)
		} else if len(args) == 1 {
			// Single arg gets empty array to preserve semantics
			return g.fn(funcName, args[0], New(KArray, "expressions", []*Expr{}))
		}
		// Snowflake/PostgreSQL/Redshift require binary nesting: ARRAY_CAT(a, ARRAY_CAT(b, c))
		// Build right-deep tree recursively to avoid creating new ArrayConcat expressions
		result := g.fn(funcName, args[len(args)-2], args[len(args)-1])
		rest := args[:len(args)-2]
		for i := len(rest) - 1; i >= 0; i-- {
			result = funcName + "(" + g.sql(rest[i]) + ", " + result + ")"
		}
		return result
	}

	return func(g *Generator, e *Expr) string {
		this := e.This()
		if x, ok := e.Arg("expressions").(*Expr); ok && x != nil {
			// A single node in `expressions` (ClickHouse arrayConcat): `[this] + exprs` falls back to
			// Expr.__radd__ and builds an Add, which later fails as a list.
			if e.ArgB("null_propagation") == g.d.S.ARRAY_FUNCS_PROPAGATES_NULLS || this.IsA(KArray) {
				if g.s.ARRAY_CONCAT_IS_VAR_LEN {
					panic(&ValueError{Msg: "'Add' object is not iterable"})
				}
				panic(&ValueError{Msg: "object of type 'Add' has no len()"})
			}
			panic(&ValueError{Msg: fmt.Sprintf("object of type '%s' has no len()", x.Kind().Name())})
		}
		exprs := e.Expressions()
		allArgs := append([]*Expr{this}, exprs...)

		sourceNullPropagation := e.ArgB("null_propagation")
		targetNullPropagation := g.d.S.ARRAY_FUNCS_PROPAGATES_NULLS

		// Skip wrapper when source and target have matching NULL semantics,
		// or when the first argument is an array literal (which can never be NULL),
		// or when it's a single-argument call (empty array is added, preserving NULL semantics)
		if sourceNullPropagation == targetNullPropagation || this.IsA(KArray) || len(exprs) == 0 {
			return buildFuncCall(g, name, allArgs)
		}

		// Case 1: Source propagates NULLs, target doesn't (Snowflake → DuckDB)
		// Check if ANY argument is NULL and return NULL explicitly
		if sourceNullPropagation {
			// Build OR-chain: a IS NULL OR b IS NULL OR c IS NULL
			nullChecks := make([]*Expr, 0, len(allArgs))
			for _, arg := range allArgs {
				nullChecks = append(nullChecks, New(KIs, "this", arg.Copy(), "expression", New(KNull)))
			}
			combinedCheck := nullChecks[0]
			for _, b := range nullChecks[1:] {
				combinedCheck = New(KOr, "this", combinedCheck, "expression", b)
			}

			funcSQL := buildFuncCall(g, name, allArgs)

			return g.sql(New(KIf, "this", combinedCheck, "true", New(KNull), "false", funcSQL))
		}

		// Case 2: Source doesn't propagate NULLs, target does (DuckDB → Snowflake)
		// Wrap ALL arguments in COALESCE to convert NULL → empty array
		wrappedArgs := make([]*Expr, 0, len(allArgs))
		for _, arg := range allArgs {
			wrappedArgs = append(wrappedArgs, New(KCoalesce, "expressions", []*Expr{arg.Copy(), New(KArray, "expressions", []*Expr{})}))
		}

		return buildFuncCall(g, name, wrappedArgs)
	}
}

// dhExprsToAny converts an expression list into Generator.func arguments.
func dhExprsToAny(list []*Expr) []any {
	out := make([]any, len(list))
	for i, x := range list {
		out[i] = x
	}
	return out
}

// varMapSQL mirrors dialect.var_map_sql(self, expression, map_func_name="MAP").
func varMapSQL(g *Generator, e *Expr, mapFuncName string /*="MAP"*/) string {
	keys := e.Arg("keys")
	values := e.Arg("values")
	keysE, _ := keys.(*Expr)
	valuesE, _ := values.(*Expr)

	if !keysE.IsA(KArray) || !valuesE.IsA(KArray) {
		g.unsupported("Cannot convert array columns into map.")
		return g.fn(mapFuncName, keys, values)
	}

	var args []any
	ks, vs := keysE.Expressions(), valuesE.Expressions()
	for i := 0; i < len(ks) && i < len(vs); i++ {
		args = append(args, g.sql(ks[i]))
		args = append(args, g.sql(vs[i]))
	}

	return g.fn(mapFuncName, args...)
}

// monthsBetweenSQL mirrors dialect.months_between_sql.
//
// Transpile MONTHS_BETWEEN to dialects that don't have native support.
// Formula: DATEDIFF('month', date2, date1) + (DAY(date1) - DAY(date2)) / 31.0.
func monthsBetweenSQL(g *Generator, e *Expr) string {
	date1 := e.This()
	date2 := e.Expression()

	// Cast to DATE to ensure consistent behavior
	date1Cast := CastExpr(date1, DT_DATE, false, nil)
	date2Cast := CastExpr(date2, DT_DATE, false, nil)

	// Whole months: DATEDIFF('month', date2, date1)
	wholeMonths := New(KDateDiff, "this", date1Cast, "expression", date2Cast, "unit", VarChecked("month"))

	// Day components
	day1 := New(KDay, "this", date1Cast.Copy())
	day2 := New(KDay, "this", date2Cast.Copy())

	// Last day of month components
	lastDayOfMonth1 := New(KLastDay, "this", date1Cast.Copy())
	lastDayOfMonth2 := New(KLastDay, "this", date2Cast.Copy())

	dayOfLastDay1 := New(KDay, "this", lastDayOfMonth1)
	dayOfLastDay2 := New(KDay, "this", lastDayOfMonth2)

	// Check if both are last day of month
	lastDay1 := New(KEQ, "this", day1.Copy(), "expression", dayOfLastDay1)
	lastDay2 := New(KEQ, "this", day2.Copy(), "expression", dayOfLastDay2)
	bothLastDay := New(KAnd, "this", lastDay1, "expression", lastDay2)

	// Fractional part: (DAY(date1) - DAY(date2)) / 31.0
	fractional := New(
		KDiv,
		"this", New(KParen, "this", New(KSub, "this", day1.Copy(), "expression", day2.Copy())),
		"expression", LiteralNumber("31.0"),
	)

	// If both are last day of month, fractional = 0, else calculate fractional
	fractionalWithCheck := New(KIf, "this", bothLastDay, "true", LiteralNumber("0"), "false", fractional)

	// Final result: whole_months + fractional
	result := New(KAdd, "this", wholeMonths, "expression", fractionalWithCheck)

	return g.sql(result)
}

// buildFormattedTime mirrors dialect.build_formatted_time(exp_class, dialect_override=None, default=None).
// dialectOverride "" means None; def is nil (None), true, false or a string.
func buildFormattedTime(kind Kind, dialectOverride string /*=None*/, def any /*=None*/) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		targetDialect := d
		if dialectOverride != "" {
			targetDialect = MustDialect(dialectOverride)
		}

		var format *Expr
		if fmtArg := seqGet(args, 1); fmtArg != nil {
			format = targetDialect.formatTimeExpr(fmtArg)
		} else {
			// fmt = target_dialect.TIME_FORMAT if default is True else default or None
			if b, ok := def.(bool); ok && b {
				format = targetDialect.formatTimeStr(targetDialect.S.TIME_FORMAT)
			} else if s, ok := def.(string); ok && s != "" {
				format = targetDialect.formatTimeStr(s)
			}
		}

		return New(kind, "this", seqGet(args, 0), "format", format)
	}
}

// timeFormat mirrors dialect.time_format(dialect=None). The returned callable yields either a string
// or nil (Python None) so that it can be passed straight to g.fn. dialect "" means None.
func timeFormat(dialect string /*=None*/) func(g *Generator, e *Expr) any {
	return func(g *Generator, e *Expr) any {
		// Returns the time format for a given expression, unless it's equivalent
		// to the default time format of the dialect of interest.
		tf := g.formatTime(e, nil, nil)
		if tf == "" || tf == MustDialect(dialect).S.TIME_FORMAT {
			return nil
		}
		return tf
	}
}

// buildDateDelta mirrors dialect.build_date_delta(exp_class, unit_mapping=None, default_unit="DAY",
// supports_timezone=False). defaultUnit "" means None.
func buildDateDelta(
	kind Kind,
	unitMapping map[string]string, /*=None*/
	defaultUnit string, /*="DAY"*/
	supportsTimezone bool, /*=False*/
) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		unitBased := len(args) >= 3
		hasTimezone := len(args) == 4
		var this *Expr
		if unitBased {
			this = args[2]
		} else {
			this = seqGet(args, 0)
		}
		var unit *Expr
		if unitBased || defaultUnit != "" {
			if unitBased {
				unit = args[0]
			} else {
				unit = LiteralString(defaultUnit)
			}
			if len(unitMapping) > 0 {
				name := unit.Name()
				if mapped, ok := unitMapping[pyLower(name)]; ok {
					name = mapped
				}
				unit = VarChecked(name)
			}
		}
		expression := New(kind, "this", this, "expression", seqGet(args, 1), "unit", unit)
		if supportsTimezone && hasTimezone {
			expression.Set("zone", args[len(args)-1])
		}
		return expression
	}
}

// buildDateDeltaWithInterval mirrors dialect.build_date_delta_with_interval(expression_class, default_unit=None).
// defaultUnit "" means None. Returns nil (None) when fewer than 2 args are given.
func buildDateDeltaWithInterval(kind Kind, defaultUnit string /*=None*/) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		if len(args) < 2 {
			return nil
		}

		interval := args[1]

		if !interval.IsA(KInterval) {
			if defaultUnit == "" {
				panic(parsePanic{&ParseError{Msg: fmt.Sprintf("INTERVAL expression expected but got '%s'", dhPyStr(interval))}})
			}
			return New(kind, "this", args[0], "expression", interval, "unit", LiteralString(defaultUnit))
		}

		return New(kind, "this", args[0], "expression", interval.This(), "unit", unitToStr(interval, "DAY"))
	}
}

// dateTruncToTime mirrors dialect.date_trunc_to_time.
func dateTruncToTime(args []*Expr, d *Dialect) *Expr {
	unit := seqGet(args, 0)
	this := seqGet(args, 1)

	if this.IsA(KCast) && dhIsType(this, DT_DATE) {
		return New(KDateTrunc, "unit", unit, "this", this)
	}
	return New(KTimestampTrunc, "this", this, "unit", unit)
}

// dateAddIntervalSQL mirrors dialect.date_add_interval_sql(data_type, kind).
func dateAddIntervalSQL(dataType string, kind string) GenFunc {
	return func(g *Generator, e *Expr) string {
		this := g.sqlKey(e, "this")
		interval := New(KInterval, "this", e.Expression(), "unit", unitToVar(e, "DAY"))
		return dataType + "_" + kind + "(" + this + ", " + g.sql(interval) + ")"
	}
}

// timestamptruncSQL mirrors dialect.timestamptrunc_sql(func="DATE_TRUNC", zone=False).
func timestamptruncSQL(fn string /*="DATE_TRUNC"*/, zone bool /*=False*/) GenFunc {
	return func(g *Generator, e *Expr) string {
		args := []any{unitToStr(e, "DAY"), e.This()}
		if zone {
			args = append(args, e.Arg("zone"))
		}
		return g.fn(fn, args...)
	}
}

// noTimestampSQL mirrors dialect.no_timestamp_sql.
func noTimestampSQL(g *Generator, e *Expr) string {
	zone := e.ArgE("zone")
	if zone == nil {
		var targetType any = DT_TIMESTAMP
		if t := annotateTypes(e, g.d).Type(); t != nil {
			targetType = t
		}
		return g.sql(CastExpr(e.This(), targetType, true, nil))
	}
	if dhTIMEZONES.Has(pyLower(zone.Name())) {
		return g.sql(New(
			KAtTimeZone,
			"this", CastExpr(e.This(), DT_TIMESTAMP, true, nil),
			"zone", zone,
		))
	}
	return g.fn("TIMESTAMP", e.Arg("this"), zone)
}

// noTimeSQL mirrors dialect.no_time_sql.
func noTimeSQL(g *Generator, e *Expr) string {
	// Transpile BQ's TIME(timestamp, zone) to CAST(TIMESTAMPTZ <timestamp> AT TIME ZONE <zone> AS TIME)
	this := CastExpr(e.This(), DT_TIMESTAMPTZ, true, nil)
	expr := CastExpr(New(KAtTimeZone, "this", this, "zone", e.Arg("zone")), DT_TIME, true, nil)
	return g.sql(expr)
}

// noDatetimeSQL mirrors dialect.no_datetime_sql.
func noDatetimeSQL(g *Generator, e *Expr) string {
	this := e.This()
	expr := e.Expression()

	if dhTIMEZONES.Has(pyLower(expr.Name())) {
		// Transpile BQ's DATETIME(timestamp, zone) to CAST(TIMESTAMPTZ <timestamp> AT TIME ZONE <zone> AS TIMESTAMP)
		this = CastExpr(this, DT_TIMESTAMPTZ, true, nil)
		this = CastExpr(New(KAtTimeZone, "this", this, "zone", expr), DT_TIMESTAMP, true, nil)
		return g.sql(this)
	}

	this = CastExpr(this, DT_DATE, true, nil)
	expr = CastExpr(expr, DT_TIME, true, nil)

	return g.sql(CastExpr(New(KAdd, "this", this, "expression", expr), DT_TIMESTAMP, true, nil))
}

// leftToSubstringSQL mirrors dialect.left_to_substring_sql.
func leftToSubstringSQL(g *Generator, e *Expr) string {
	return g.sql(New(KSubstring, "this", e.This(), "start", LiteralInt(1), "length", e.Expression()))
}

// rightToSubstringSQL mirrors dialect.right_to_substring_sql.
func rightToSubstringSQL(g *Generator, e *Expr) string {
	this := e.This()
	start := dhBinop(
		KSub,
		New(KLength, "this", this),
		ParenExpr(dhBinop(KSub, e.Expression(), 1), true),
	)
	return g.sql(New(KSubstring, "this", this, "start", start))
}

// timestrtotimeSQL mirrors dialect.timestrtotime_sql(self, expression, include_precision=False).
func timestrtotimeSQL(g *Generator, e *Expr, includePrecision bool /*=False*/) string {
	builder := DT_TIMESTAMP
	if e.ArgB("zone") {
		builder = DT_TIMESTAMPTZ
	}
	datatype := NewDataType(builder)

	if e.This().IsA(KLiteral) && includePrecision {
		precision := dhSubsecondPrecision(e.This().Name())
		if precision > 0 {
			datatype = NewDataType(datatype.DTypeOf())
			datatype.Set("expressions", []*Expr{New(KDataTypeParam, "this", LiteralInt(precision))})
		}
	}

	return g.sql(CastExpr(e.This(), datatype, true, g.d))
}

// datestrtodateSQL mirrors dialect.datestrtodate_sql.
func datestrtodateSQL(g *Generator, e *Expr) string {
	return g.sql(CastExpr(e.This(), DT_DATE, true, nil))
}

// encodeDecodeSQL mirrors dialect.encode_decode_sql(self, expression, name, replace=True).
// Used for Presto and Duckdb which use functions that don't support charset, and assume utf-8.
func encodeDecodeSQL(g *Generator, e *Expr, name string, replace bool /*=True*/) string {
	charset := e.ArgE("charset")
	if charset != nil {
		if n := pyLower(charset.Name()); n != "utf-8" && n != "utf8" {
			g.unsupported(fmt.Sprintf("Expected utf-8 character set, got %s.", dhPyStr(charset)))
		}
	}

	var r any
	if replace {
		r = e.Arg("replace")
	}
	return g.fn(name, e.Arg("this"), r)
}

// minOrLeast mirrors dialect.min_or_least.
func minOrLeast(g *Generator, e *Expr) string {
	name := "MIN"
	if len(e.Expressions()) > 0 {
		name = "LEAST"
	}
	return renameFunc(name)(g, e)
}

// maxOrGreatest mirrors dialect.max_or_greatest.
func maxOrGreatest(g *Generator, e *Expr) string {
	name := "MAX"
	if len(e.Expressions()) > 0 {
		name = "GREATEST"
	}
	return renameFunc(name)(g, e)
}

// countIfToSum mirrors dialect.count_if_to_sum.
func countIfToSum(g *Generator, e *Expr) string {
	cond := e.This()

	if e.This().IsA(KDistinct) {
		cond = e.This().Expressions()[0]
		g.unsupported("DISTINCT is not supported when converting COUNT_IF to SUM")
	}

	return g.fn("sum", dhFunc("if", cond, 1, 0))
}

// trimSQL mirrors dialect.trim_sql(self, expression, default_trim_type="").
func trimSQL(g *Generator, e *Expr, defaultTrimType string /*=""*/) string {
	removeChars := g.sqlKey(e, "expression")

	// Use TRIM/LTRIM/RTRIM syntax if the expression isn't database-specific
	if removeChars == "" {
		return g.trimSQL(e)
	}

	target := g.sqlKey(e, "this")
	trimType := g.sqlKey(e, "position")
	if trimType == "" {
		trimType = defaultTrimType
	}
	collation := g.sqlKey(e, "collation")

	if trimType != "" {
		trimType = trimType + " "
	}
	if removeChars != "" {
		removeChars = removeChars + " "
	}
	fromPart := ""
	if trimType != "" || removeChars != "" {
		fromPart = "FROM "
	}
	if collation != "" {
		collation = " COLLATE " + collation
	}
	return "TRIM(" + trimType + removeChars + fromPart + target + collation + ")"
}

// concatToDpipeSQL mirrors dialect.concat_to_dpipe_sql.
func concatToDpipeSQL(g *Generator, e *Expr) string {
	exprs := e.Expressions()
	// reduce() without an initializer raises on an empty sequence
	acc := exprs[0]
	for _, y := range exprs[1:] {
		acc = New(KDPipe, "this", acc, "expression", y)
	}
	return g.sql(acc)
}

// concatWsToDpipeSQL mirrors dialect.concat_ws_to_dpipe_sql.
func concatWsToDpipeSQL(g *Generator, e *Expr) string {
	exprs := e.Expressions()
	delim, restArgs := exprs[0], exprs[1:]
	// reduce() without an initializer raises on an empty sequence
	acc := restArgs[0]
	for _, y := range restArgs[1:] {
		acc = New(KDPipe, "this", acc, "expression", New(KDPipe, "this", delim, "expression", y))
	}
	return g.sql(acc)
}

// regexpExtractSQL mirrors dialect.regexp_extract_sql.
func regexpExtractSQL(g *Generator, e *Expr) string {
	dhUnsupportedArgs(g, e, "position", "occurrence", "parameters")
	group := e.ArgE("group")

	// Do not render group if it's the default value for this dialect
	if group != nil && group.Name() == strconv.Itoa(g.d.S.REGEXP_EXTRACT_DEFAULT_GROUP) {
		group = nil
	}

	return g.fn(e.Kind().SQLName(), e.Arg("this"), e.Arg("expression"), group)
}

// regexpReplaceSQL mirrors dialect.regexp_replace_sql.
func regexpReplaceSQL(g *Generator, e *Expr) string {
	dhUnsupportedArgs(g, e, "position", "occurrence", "modifiers")
	if !e.HasArgKey("replacement") {
		// expression.args["replacement"] raises KeyError
		panic(&ValueError{Msg: "'replacement'"})
	}
	return g.fn("REGEXP_REPLACE", e.Arg("this"), e.Arg("expression"), e.Arg("replacement"))
}

// pivotColumnNames mirrors dialect.pivot_column_names(aggregations, dialect). d nil means None.
func pivotColumnNames(aggregations []*Expr, d *Dialect) []string {
	names := []string{}
	for _, agg := range aggregations {
		if agg.IsA(KAlias) {
			names = append(names, agg.Alias())
		} else {
			// This case corresponds to aggregations without aliases being used as suffixes
			// (e.g. col_avg(foo)). We need to unquote identifiers because they're going to
			// be quoted in the base parser's `_parse_pivot` method, due to `to_identifier`.
			// Otherwise, we'd end up with `col_avg(`foo`)` (notice the double quotes).
			aggAllUnquoted := agg.Transform(func(node *Expr) *Expr {
				if node.IsA(KIdentifier) {
					return New(KIdentifier, "this", node.Name(), "quoted", false)
				}
				return node
			}, true)
			target := d
			if target == nil {
				target = MustDialect("")
			}
			sql, err := target.NewGenerator(&GenerateOptions{NormalizeFunctions: strp("lower")}).Generate(aggAllUnquoted, true)
			if err != nil {
				panic(err)
			}
			names = append(names, sql)
		}
	}
	return names
}

// binaryFromFunction mirrors dialect.binary_from_function(expr_type).
func binaryFromFunction(kind Kind) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		return New(kind, "this", seqGet(args, 0), "expression", seqGet(args, 1))
	}
}

// buildTimestampTrunc mirrors dialect.build_timestamp_trunc.
// Used to represent DATE_TRUNC in Doris, Postgres and Starrocks dialects.
func buildTimestampTrunc(args []*Expr, d *Dialect) *Expr {
	return New(KTimestampTrunc, "this", seqGet(args, 1), "unit", seqGet(args, 0))
}

// buildTrunc mirrors dialect.build_trunc(args, dialect, date_trunc_unabbreviate=True,
// default_date_trunc_unit=None, date_trunc_requires_part=True, fractions_supported=False).
// defaultDateTruncUnit "" means None.
//
// Builder for dialects with overloaded TRUNC (Oracle, Snowflake, etc).
// Uses type annotation to distinguish date vs numeric truncation.
// Returns Anonymous if type cannot be determined.
func buildTrunc(
	args []*Expr,
	d *Dialect,
	dateTruncUnabbreviate bool, /*=True*/
	defaultDateTruncUnit string, /*=None*/
	dateTruncRequiresPart bool, /*=True*/
	fractionsSupported bool, /*=False*/
) *Expr {
	this := seqGet(args, 0)
	second := seqGet(args, 1)

	if this != nil && this.Type() == nil {
		this = annotateTypes(this, d)
	}
	if second != nil && second.Type() == nil {
		second = annotateTypes(second, d)
	}

	// Date truncation
	if (this != nil && dhIsType(this, DataType_TEMPORAL_TYPES.Items()...) && (second != nil || defaultDateTruncUnit != "")) ||
		(second != nil && dhIsType(second, DataType_TEXT_TYPES.Items()...)) {
		unit := second
		if unit == nil {
			unit = LiteralString(defaultDateTruncUnit)
		}
		// DateTrunc.__init__ pops `unabbreviate` (see report: New() must implement DateTrunc/TimeUnit init).
		return New(KDateTrunc, "this", this, "unit", unit, "unabbreviate", dateTruncUnabbreviate)
	}

	// Numeric truncation
	if (this != nil && dhIsType(this, DataType_NUMERIC_TYPES.Items()...)) ||
		(second != nil && dhIsType(second, DataType_NUMERIC_TYPES.Items()...)) ||
		(!dateTruncRequiresPart && second == nil) {
		return New(KTrunc, "this", this, "decimals", second, "fractions_supported", fractionsSupported)
	}

	return New(KAnonymous, "this", "TRUNC", "expressions", args)
}

// anyValueToMaxSQL mirrors dialect.any_value_to_max_sql.
func anyValueToMaxSQL(g *Generator, e *Expr) string {
	return g.fn("MAX", e.Arg("this"))
}

// boolXorSQL mirrors dialect.bool_xor_sql.
func boolXorSQL(g *Generator, e *Expr) string {
	a := g.sql(e.Left())
	b := g.sql(e.Right())
	return "(" + a + " AND (NOT " + b + ")) OR ((NOT " + a + ") AND " + b + ")"
}

// isParseJSON mirrors dialect.is_parse_json.
func isParseJSON(e *Expr) bool {
	return e.IsA(KParseJSON) || (e.IsA(KCast) && dhIsType(e, DT_JSON))
}

// isnullToIsNull mirrors dialect.isnull_to_is_null.
func isnullToIsNull(args []*Expr, d *Dialect) *Expr {
	return New(KParen, "this", New(KIs, "this", seqGet(args, 0), "expression", Null()))
}

// generatedasidentitycolumnconstraintSQL mirrors dialect.generatedasidentitycolumnconstraint_sql.
func generatedasidentitycolumnconstraintSQL(g *Generator, e *Expr) string {
	start := g.sqlKey(e, "start")
	if start == "" {
		start = "1"
	}
	increment := g.sqlKey(e, "increment")
	if increment == "" {
		increment = "1"
	}
	return "IDENTITY(" + start + ", " + increment + ")"
}

// argMaxOrMinNoCount mirrors dialect.arg_max_or_min_no_count(name).
func argMaxOrMinNoCount(name string) GenFunc {
	return func(g *Generator, e *Expr) string {
		dhUnsupportedArgs(g, e, "count")
		return g.fn(name, e.Arg("this"), e.Arg("expression"))
	}
}

// dhTsOrDsReturnType mirrors the TsOrDsAdd.return_type property:
// DataType.build(self.args.get("return_type") or DType.DATE).
func dhTsOrDsReturnType(e *Expr) *Expr {
	rt := e.Arg("return_type")
	if !truthy(rt) {
		return NewDataType(DT_DATE)
	}
	return DataTypeBuild(rt, nil, false, true)
}

// tsOrDsAddCast mirrors dialect.ts_or_ds_add_cast.
func tsOrDsAddCast(e *Expr) *Expr {
	this := e.This().Copy()

	returnType := dhTsOrDsReturnType(e)
	if dhIsType(returnType, DT_DATE) {
		// If we need to cast to a DATE, we cast to TIMESTAMP first to make sure we
		// can truncate timestamp strings, because some dialects can't cast them to DATE
		this = CastExpr(this, DT_TIMESTAMP, true, nil)
	}

	e.This().Replace(CastExpr(this, returnType, true, nil))
	return e
}

// dateDeltaSQL mirrors dialect.date_delta_sql(name, cast=False).
func dateDeltaSQL(name string, cast bool /*=False*/) GenFunc {
	return func(g *Generator, e *Expr) string {
		if cast && e.IsA(KTsOrDsAdd) {
			e = tsOrDsAddCast(e)
		}

		return g.fn(name, unitToVar(e, "DAY"), e.Arg("expression"), e.Arg("this"))
	}
}

// dateDeltaToBinaryIntervalOp mirrors dialect.date_delta_to_binary_interval_op(cast=True).
func dateDeltaToBinaryIntervalOp(cast bool /*=True*/) GenFunc {
	return func(g *Generator, e *Expr) string {
		this := e.This()
		unit := unitToVar(e, "DAY")
		op := "-"
		if e.IsA(KDateAdd, KTimeAdd, KDatetimeAdd, KTsOrDsAdd, KTimestampAdd) {
			op = "+"
		}

		var toType any
		if cast {
			if e.IsA(KTsOrDsAdd) {
				toType = dhTsOrDsReturnType(e)
			} else if this.IsString() {
				// Cast string literals (i.e function parameters) to the appropriate type for +/- interval to work
				if e.IsA(KDatetimeAdd, KDatetimeSub) {
					toType = DT_DATETIME
				} else {
					toType = DT_DATE
				}
			}
		}

		if toType != nil {
			this = CastExpr(this, toType, true, nil)
		}

		expr := e.Expression()
		interval := expr
		if !expr.IsA(KInterval) {
			interval = New(KInterval, "this", expr, "unit", unit)
		}

		return g.sql(this) + " " + op + " " + g.sql(interval)
	}
}

// unitToStr mirrors dialect.unit_to_str(expression, default="DAY"). def "" means None.
func unitToStr(e *Expr, def string /*="DAY"*/) *Expr {
	unit := e.ArgE("unit")
	if unit == nil {
		if def != "" {
			return LiteralString(def)
		}
		return nil
	}

	if unit.IsA(KPlaceholder) || (!unit.Is(KVar) && !unit.Is(KLiteral)) {
		return unit
	}

	return LiteralString(unit.Name())
}

// unitToVar mirrors dialect.unit_to_var(expression, default="DAY"). def "" means None.
func unitToVar(e *Expr, def string /*="DAY"*/) *Expr {
	unit := e.ArgE("unit")

	if unit.IsA(KVar, KPlaceholder, KWeekStart, KColumn) {
		return unit
	}

	value := def
	if unit != nil {
		value = unit.Name()
	}
	if value != "" {
		return New(KVar, "this", value)
	}
	return nil
}

// WEEK_START_DAY_TO_DOW mirrors dialect.WEEK_START_DAY_TO_DOW.
// Days of week to ISO 8601 day-of-week numbers
// ISO 8601 standard: Monday=1, Tuesday=2, Wednesday=3, Thursday=4, Friday=5, Saturday=6, Sunday=7.
var WEEK_START_DAY_TO_DOW = map[string]int{
	"MONDAY":    1,
	"TUESDAY":   2,
	"WEDNESDAY": 3,
	"THURSDAY":  4,
	"FRIDAY":    5,
	"SATURDAY":  6,
	"SUNDAY":    7,
}

// weekUnitToDow mirrors dialect.week_unit_to_dow. ok=false means None.
//
// Compute the week start day for a week-ish diff unit, e.g BigQuery's WEEK(<day>) or ISOWEEK unit parts.
func weekUnitToDow(unit *Expr) (int, bool) {
	if unit.IsA(KVar) {
		if n := pyUpper(unit.Name()); n == "WEEK" || n == "ISOWEEK" {
			return 1, true
		}
	}

	// Handle WeekStart expressions with explicit day
	if unit.IsA(KWeekStart) {
		v, ok := WEEK_START_DAY_TO_DOW[pyUpper(unit.Name())]
		return v, ok
	}

	return 0, false
}

// mapDatePart mirrors dialect.map_date_part(part, dialect=Dialect). d nil means the base Dialect.
func mapDatePart(part *Expr, d *Dialect) *Expr {
	mapped := ""
	if part != nil && !(part.IsA(KColumn) && len(part.Parts()) != 1) {
		if d == nil {
			d = MustDialect("")
		}
		mapped = d.S.DATE_PART_MAPPING[pyUpper(part.Name())]
	}
	if mapped != "" {
		if part.IsString() {
			return LiteralString(mapped)
		}
		return VarChecked(mapped)
	}

	return part
}

// noLastDaySQL mirrors dialect.no_last_day_sql.
func noLastDaySQL(g *Generator, e *Expr) string {
	truncCurrDate := dhFunc("date_trunc", "month", e.This())
	plusOneMonth := dhFunc("date_add", truncCurrDate, 1, "month")
	minusOneDay := dhFunc("date_sub", plusOneMonth, 1, "day")

	return g.sql(CastExpr(minusOneDay, DT_DATE, true, nil))
}

// mergeWithoutTargetSQL mirrors dialect.merge_without_target_sql.
// Remove table refs from columns in when statements.
func mergeWithoutTargetSQL(g *Generator, e *Expr) string {
	alias := e.This().argForAttr("alias", "this")

	// normalize returns (name, false) for None
	normalize := func(identifier *Expr) (string, bool) {
		if identifier == nil {
			return "", false
		}
		return g.d.NormalizeIdentifier(identifier).Name(), true
	}

	targets := map[string]bool{}
	targetsHasNone := false
	add := func(s string, ok bool) {
		if ok {
			targets[s] = true
		} else {
			targetsHasNone = true
		}
	}
	in := func(s string, ok bool) bool {
		if ok {
			return targets[s]
		}
		return targetsHasNone
	}

	add(normalize(e.This().This()))

	if alias != nil {
		add(normalize(alias.This()))
	}

	for _, when := range e.ArgE("whens").Expressions() {
		// only remove the target table names from certain parts of WHEN MATCHED / WHEN NOT MATCHED
		// they are still valid in the <condition>, the right hand side of each UPDATE and the VALUES part
		// (not the column list) of the INSERT
		then := when.ArgE("then")
		if then != nil {
			if then.IsA(KUpdate) {
				for equals := range then.FindAll(KEQ) {
					equalLHS := equals.This()
					if equalLHS.IsA(KColumn) && in(normalize(equalLHS.ArgE("table"))) {
						equalLHS.Replace(ColumnExpr(equalLHS.This(), nil, nil, nil, nil, nil, true))
					}
				}
			}
			if then.IsA(KInsert) {
				columnList := then.This()
				if columnList.IsA(KTuple) {
					for _, column := range columnList.Expressions() {
						if in(normalize(column.ArgE("table"))) {
							column.Replace(ColumnExpr(column.This(), nil, nil, nil, nil, nil, true))
						}
					}
				}
			}
		}
	}

	return g.mergeSQL(e)
}

// buildJSONExtractPath mirrors dialect.build_json_extract_path(expr_type, zero_based_indexing=True,
// arrow_req_json_type=False, json_type=None). jsonType "" means None.
//
// Python's `del args[2:]` truncates the caller's list (affecting the parser's argument-count
// validation); this is reported to the caller through WithValidateArgs.
func buildJSONExtractPath(
	kind Kind,
	zeroBasedIndexing bool, /*=True*/
	arrowReqJSONType bool, /*=False*/
	jsonType string, /*=None*/
) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		segments := []*Expr{New(KJSONPathRoot)}
		if len(args) > 1 {
			for _, arg := range args[1:] {
				if !arg.IsA(KLiteral) {
					// We use the fallback parser because we can't really transpile non-literals safely
					return FromArgList(kind, args)
				}

				text := arg.Name()
				if isPyInt(text) && (!arrowReqJSONType || !arg.IsString()) {
					index := dhPyIntValue(text)
					if !zeroBasedIndexing {
						index--
					}
					segments = append(segments, New(KJSONPathSubscript, "this", index))
				} else {
					segments = append(segments, New(KJSONPathKey, "this", text))
				}
			}
		}

		// This is done to avoid failing in the expression validator due to the arg count
		if len(args) > 2 {
			args = args[:2]
		}
		kv := []any{
			"this", seqGet(args, 0),
			"expression", New(KJSONPath, "expressions", segments),
		}

		isJSONB := kind.IsA(KJSONBExtract, KJSONBExtractScalar)
		if !isJSONB {
			kv = append(kv, "only_json_types", arrowReqJSONType)
		}

		if jsonType != "" {
			kv = append(kv, "json_type", jsonType)
		}

		return WithValidateArgs(New(kind, kv...), args)
	}
}

// jsonExtractSegments mirrors dialect.json_extract_segments(name, quoted_index=True, op=None). op "" means None.
func jsonExtractSegments(name string, quotedIndex bool /*=True*/, op string /*=None*/) GenFunc {
	return func(g *Generator, e *Expr) string {
		path := e.Expression()
		if !path.IsA(KJSONPath) {
			return renameFunc(name)(g, e)
		}

		var segments []string
		for _, segment := range path.Expressions() {
			escape := segment.ArgB("quoted")
			p := g.sql(segment)
			if p != "" {
				if segment.IsA(KJSONPathPart) && (quotedIndex || !segment.IsA(KJSONPathSubscript)) {
					if escape {
						p = g.escapeStr(p, true, "", "", false)
					}

					p = g.d.S.QUOTE_START + p + g.d.S.QUOTE_END
				}

				segments = append(segments, p)
			}
		}

		if op != "" {
			return strings.Join(append([]string{g.sql(e.Arg("this"))}, segments...), " "+op+" ")
		}
		args := []any{e.This()}
		for _, s := range segments {
			args = append(args, s)
		}
		return g.fn(name, args...)
	}
}

// jsonPathKeyOnlyName mirrors dialect.json_path_key_only_name.
func jsonPathKeyOnlyName(g *Generator, e *Expr) string {
	if e.This().IsA(KJSONPathWildcard) {
		g.unsupported("Unsupported wildcard in JSONPathKey expression")
	}

	return e.Name()
}

// filterArrayUsingUnnest mirrors dialect.filter_array_using_unnest.
func filterArrayUsingUnnest(g *Generator, e *Expr) string {
	cond := e.Expression()
	var alias any
	if cond.IsA(KLambda) && len(cond.Expressions()) == 1 {
		alias = cond.Expressions()[0]
		cond = cond.This()
	} else if cond.IsA(KPredicate) {
		alias = "_u"
	} else if e.IsA(KArrayRemove) {
		alias = "_u"
		cond = New(KNEQ, "this", alias, "expression", e.Expression())
	} else {
		g.unsupported("Unsupported filter condition")
		return ""
	}

	unnest := New(KUnnest, "expressions", []*Expr{e.This()})
	var selectAlias *Expr
	if a, ok := alias.(*Expr); ok {
		selectAlias = a
	} else {
		// exp.select("_u") parses the string (into=exp.Expr)
		selectAlias = MaybeParse(alias.(string), KExpr, "", nil)
	}
	filtered := SelectExpr(selectAlias).
		SelectFrom(AliasTableExpr(unnest, nil, []any{alias}, nil, true), true).
		QueryWhere([]*Expr{cond}, true, true)
	return g.sql(New(KArray, "expressions", []*Expr{filtered}))
}

// arrayCompactSQL mirrors dialect.array_compact_sql.
func arrayCompactSQL(g *Generator, e *Expr) string {
	lambdaID := ToIdentifier("_u", nil)
	cond := NotExpr(New(KIs, "this", lambdaID, "expression", Null()), true)
	return g.sql(New(
		KArrayFilter,
		"this", e.This(),
		"expression", New(KLambda, "this", cond, "expressions", []*Expr{lambdaID}),
	))
}

// removeFromArrayUsingFilter mirrors dialect.remove_from_array_using_filter.
func removeFromArrayUsingFilter(g *Generator, e *Expr) string {
	lambdaID := ToIdentifier("_u", nil)
	cond := New(KNEQ, "this", lambdaID, "expression", e.Expression())

	filterSQL := g.sql(New(
		KArrayFilter,
		"this", e.This(),
		"expression", New(KLambda, "this", cond, "expressions", []*Expr{lambdaID}),
	))

	// Handle NULL propagation for ArrayRemove
	sourceNullPropagation := e.ArgB("null_propagation")
	targetNullPropagation := g.d.S.ARRAY_FUNCS_PROPAGATES_NULLS

	// Source propagates NULLs (Snowflake), target doesn't (DuckDB):
	// When removal value is NULL, return NULL instead of applying filter
	if sourceNullPropagation && !targetNullPropagation {
		removalValue := e.Expression()

		// Optimization: skip wrapper if removal value is a non-NULL literal
		// (e.g., 5, 'a', TRUE) or an array literal (e.g., [1, 2])
		if (removalValue.IsA(KLiteral) && !removalValue.IsA(KNull)) || removalValue.IsA(KArray) {
			return filterSQL
		}

		return g.sql(New(
			KIf,
			"this", New(KIs, "this", removalValue, "expression", New(KNull)),
			"true", New(KNull),
			"false", filterSQL,
		))
	}

	return filterSQL
}

// toNumberWithNlsParam mirrors dialect.to_number_with_nls_param.
func toNumberWithNlsParam(g *Generator, e *Expr) string {
	return g.fn("TO_NUMBER", e.Arg("this"), e.Arg("format"), e.Arg("nlsparam"))
}

// buildDefaultDecimalType mirrors dialect.build_default_decimal_type(precision=None, scale=None).
// Negative precision / scale mean None.
func buildDefaultDecimalType(precision int /*=None (<0)*/, scale int /*=None (<0)*/) typeConverterFn {
	return func(dtype *Expr) *Expr {
		if len(dtype.Expressions()) > 0 || precision < 0 {
			return dtype
		}

		params := strconv.Itoa(precision)
		if scale >= 0 {
			params += ", " + strconv.Itoa(scale)
		}
		return DataTypeFromStr("DECIMAL("+params+")", MustDialect(""), false)
	}
}

// buildTimestampFromParts mirrors dialect.build_timestamp_from_parts.
func buildTimestampFromParts(args []*Expr, d *Dialect) *Expr {
	if len(args) == 2 {
		// Other dialects don't have the TIMESTAMP_FROM_PARTS(date, time) concept,
		// so we parse this into Anonymous for now instead of introducing complexity
		return New(KAnonymous, "this", "TIMESTAMP_FROM_PARTS", "expressions", args)
	}

	return FromArgList(KTimestampFromParts, args)
}

// sha256SQL mirrors dialect.sha256_sql.
func sha256SQL(g *Generator, e *Expr) string {
	length := e.Text("length")
	if length == "" {
		length = "256"
	}
	return g.fn("SHA"+length, e.Arg("this"))
}

// sha2DigestSQL mirrors dialect.sha2_digest_sql.
func sha2DigestSQL(g *Generator, e *Expr) string {
	length := e.Text("length")
	if length == "" {
		length = "256"
	}
	return g.fn("SHA"+length, e.Arg("this"))
}

// sequenceSQL mirrors dialect.sequence_sql.
func sequenceSQL(g *Generator, e *Expr) string {
	start := e.ArgE("start")
	end := e.ArgE("end")
	step := e.ArgE("step")

	var targetType *Expr
	if start.IsA(KCast) {
		targetType = start.ArgE("to")
	} else if end.IsA(KCast) {
		targetType = end.ArgE("to")
	}

	if start != nil && end != nil {
		if targetType != nil && dhIsType(targetType, DT_DATE, DT_TIMESTAMP) {
			if start.IsA(KCast) && targetType == start.ArgE("to") {
				end = CastExpr(end, targetType, true, nil)
			} else {
				start = CastExpr(start, targetType, true, nil)
			}
		}

		if e.ArgB("is_end_exclusive") {
			stepValue := step
			if stepValue == nil {
				stepValue = LiteralInt(1)
			}
			end = ParenExpr(New(KSub, "this", end, "expression", stepValue), false)

			var seqArgs []*Expr
			for _, x := range []*Expr{start, end, step} {
				if x != nil {
					seqArgs = append(seqArgs, x)
				}
			}
			sequenceCall := New(KAnonymous, "this", "SEQUENCE", "expressions", seqArgs)
			zero := LiteralInt(0)
			shouldReturnEmpty := OrExpr(
				New(KEQ, "this", stepValue.Copy(), "expression", zero.Copy()),
				AndExpr(
					New(KGT, "this", stepValue.Copy(), "expression", zero.Copy()),
					New(KGT, "this", start.Copy(), "expression", end.Copy()),
				),
				AndExpr(
					New(KLT, "this", stepValue.Copy(), "expression", zero.Copy()),
					New(KLT, "this", start.Copy(), "expression", end.Copy()),
				),
			)
			emptyArrayOrSequence := New(
				KIf,
				"this", shouldReturnEmpty,
				"true", New(KArray, "expressions", []*Expr{}),
				"false", sequenceCall,
			)
			return g.sql(g.simplifyUnlessLiteral(emptyArrayOrSequence))
		}
	}

	return g.fn("SEQUENCE", start, end, step)
}

// dialectBuildLike mirrors dialect.build_like(expr_type, not_like=False).
// (Named with a `dialect` prefix because parser.py's module-level build_like already owns `buildLike`.)
func dialectBuildLike(kind Kind, notLike bool /*=False*/) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		likeExpr := New(kind, "this", seqGet(args, 0), "expression", seqGet(args, 1))

		if escape := seqGet(args, 2); escape != nil {
			likeExpr = New(KEscape, "this", likeExpr, "expression", escape)
		}

		if notLike {
			likeExpr = New(KNot, "this", likeExpr)
		}

		return likeExpr
	}
}

// buildRegexpExtract mirrors dialect.build_regexp_extract(expr_type).
func buildRegexpExtract(kind Kind) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		// The "position" argument specifies the index of the string character to start matching from.
		// `null_if_pos_overflow` reflects the dialect's behavior when position is greater than the string
		// length. If true, returns NULL. If false, returns an empty string. `null_if_pos_overflow` is
		// only needed for exp.RegexpExtract - exp.RegexpExtractAll always returns an empty array if
		// position overflows.
		group := seqGet(args, 2)
		if group == nil {
			group = LiteralInt(d.S.REGEXP_EXTRACT_DEFAULT_GROUP)
		}
		kv := []any{
			"this", seqGet(args, 0),
			"expression", seqGet(args, 1),
			"group", group,
			"parameters", seqGet(args, 3),
		}
		if kind == KRegexpExtract {
			kv = append(kv, "null_if_pos_overflow", d.S.REGEXP_EXTRACT_POSITION_OVERFLOW_RETURNS_NULL)
		}
		return New(kind, kv...)
	}
}

// explodeToUnnestSQL mirrors dialect.explode_to_unnest_sql.
func explodeToUnnestSQL(g *Generator, e *Expr) string {
	this := e.This()
	alias := e.ArgE("alias")

	var crossJoinExpr *Expr
	if this.IsA(KPosexplode) && alias != nil {
		// Spark's `FROM x LATERAL VIEW POSEXPLODE(y) t AS pos, col` has the following semantics:
		// - The first column is the position and the rest (1 for array, 2 for maps) are the exploded values
		// - The position is 0-based whereas WITH ORDINALITY is 1-based
		// For that matter, we must (1) subtract 1 from the ORDINALITY position and (2) rearrange the columns accordingly, returning:
		// `FROM x CROSS JOIN LATERAL (SELECT pos - 1 AS pos, col FROM UNNEST(y) WITH ORDINALITY AS t(col, pos))
		columns := alias.ArgL("columns")
		pos, cols := columns[0], append([]*Expr{}, columns[1:]...)

		selects := append([]*Expr{AliasExpr(dhBinop(KSub, pos, 1), pos, nil, true)}, cols...)
		lateralSubquery := SelectExpr(selects...).SelectFrom(New(
			KUnnest,
			"expressions", []*Expr{this.This()},
			"offset", true,
			"alias", New(KTableAlias, "this", alias.This(), "columns", append(append([]*Expr{}, cols...), pos)),
		), true)

		crossJoinExpr = New(KLateral, "this", lateralSubquery.QuerySubquery(nil, true))
	} else if this.IsA(KExplode) {
		crossJoinExpr = New(KUnnest, "expressions", []*Expr{this.This()}, "alias", alias)
	}

	if crossJoinExpr != nil {
		return g.sql(New(KJoin, "this", crossJoinExpr, "kind", "cross"))
	}

	return g.lateralSQL(e)
}

// timestampdiffSQL mirrors dialect.timestampdiff_sql.
func timestampdiffSQL(g *Generator, e *Expr) string {
	return g.fn("TIMESTAMPDIFF", e.Arg("unit"), e.Arg("expression"), e.Arg("this"))
}

// noMakeIntervalSQL mirrors dialect.no_make_interval_sql(self, expression, sep=", ").
func noMakeIntervalSQL(g *Generator, e *Expr, sep string /*=", "*/) string {
	var args []any
	for _, a := range e.args {
		value := a.val
		if v, ok := value.(*Expr); ok && v.IsA(KKwarg) {
			value = v.Expression()
		}

		args = append(args, dhPyStr(value)+" "+a.key)
	}

	return "INTERVAL '" + g.formatArgs(sep, args...) + "'"
}

// lengthOrCharLengthSQL mirrors dialect.length_or_char_length_sql.
func lengthOrCharLengthSQL(g *Generator, e *Expr) string {
	lengthFunc := "CHAR_LENGTH"
	if e.ArgB("binary") {
		lengthFunc = "LENGTH"
	}
	return g.fn(lengthFunc, e.Arg("this"))
}

// groupconcatSQL mirrors dialect.groupconcat_sql(self, expression, func_name="LISTAGG", sep=",",
// within_group=True, on_overflow=False). sep "" means None.
func groupconcatSQL(
	g *Generator,
	e *Expr,
	funcName string, /*="LISTAGG"*/
	sep string, /*=","*/
	withinGroup bool, /*=True*/
	onOverflow bool, /*=False*/
) string {
	this := e.This()
	var sepExpr any = e.Arg("separator")
	if !truthy(sepExpr) {
		sepExpr = nil
		if sep != "" {
			sepExpr = LiteralString(sep)
		}
	}
	separator := g.sql(sepExpr)

	onOverflowSQL := g.sqlKey(e, "on_overflow")
	if onOverflow && onOverflowSQL != "" {
		onOverflowSQL = " ON OVERFLOW " + onOverflowSQL
	} else {
		onOverflowSQL = ""
	}

	var limit *Expr
	if this.IsA(KLimit) && this.This() != nil {
		limit = this
		this = limit.This().Pop()
	}

	order := this.Find(KOrder)

	if order != nil && order.This() != nil {
		this = order.This().Pop()
	}

	var sepArg any
	if separator != "" || onOverflowSQL != "" {
		sepArg = separator + onOverflowSQL
	}
	args := g.formatArgs(", ", this, sepArg)

	// Python stores raw SQL strings in Anonymous.expressions; a Var renders its text verbatim.
	listagg := New(KAnonymous, "this", funcName, "expressions", []*Expr{dhRawSQL(args)})

	modifiers := g.sql(limit)

	if order != nil {
		if withinGroup {
			listagg = New(KWithinGroup, "this", listagg, "expression", order)
		} else {
			modifiers = g.sql(order) + modifiers
		}
	}

	if modifiers != "" {
		listagg.Set("expressions", []*Expr{dhRawSQL(args + modifiers)})
	}

	return g.sql(listagg)
}

// dhRawSQL stands in for a raw SQL string stored inside an expression list (Python allows str
// elements there); Var renders its `this` verbatim.
func dhRawSQL(s string) *Expr { return New(KVar, "this", s) }

// buildTimetostrOrTochar mirrors dialect.build_timetostr_or_tochar.
func buildTimetostrOrTochar(args []*Expr, d *Dialect) *Expr {
	if len(args) == 2 {
		this := args[0]
		if this.Type() == nil {
			annotateTypes(this, d)
		}

		if dhIsType(this, DataType_TEMPORAL_TYPES.Items()...) {
			return buildFormattedTime(KTimeToStr, "", true)(args, d)
		}
	}

	return FromArgList(KToChar, args)
}

// buildReplaceWithOptionalReplacement mirrors dialect.build_replace_with_optional_replacement.
func buildReplaceWithOptionalReplacement(args []*Expr, d *Dialect) *Expr {
	replacement := seqGet(args, 2)
	if replacement == nil {
		replacement = LiteralString("")
	}
	return New(KReplace, "this", seqGet(args, 0), "expression", seqGet(args, 1), "replacement", replacement)
}

// regexpReplaceGlobalModifier mirrors dialect.regexp_replace_global_modifier.
func regexpReplaceGlobalModifier(e *Expr) *Expr {
	modifiers := e.ArgE("modifiers")
	singleReplace := e.ArgB("single_replace")
	occurrence := e.ArgE("occurrence")

	if !singleReplace && (occurrence == nil || (occurrence.IsInt() && dhIsZero(occurrence))) {
		if modifiers == nil || modifiers.IsString() {
			// Append 'g' to the modifiers if they are not provided since
			// the semantics of REGEXP_REPLACE from the input dialect
			// is to replace all occurrences of the pattern.
			value := ""
			if modifiers != nil {
				value = modifiers.Name()
			}
			modifiers = LiteralString(value + "g")
		}
	}

	return modifiers
}

// dhIsZero mirrors `expr.to_py() == 0` for an integer literal.
func dhIsZero(e *Expr) bool {
	v, _ := e.toPyNumber()
	return v != nil && v.Sign() == 0
}

// getbitSQL mirrors dialect.getbit_sql.
//
// Generates SQL for Getbit according to DuckDB and Postgres, transpiling it if either:
// 1. The zero index corresponds to the least-significant bit
// 2. The input type is an integer value.
func getbitSQL(g *Generator, e *Expr) string {
	value := e.This()
	position := e.Expression()

	intTypes := append(DataType_SIGNED_INTEGER_TYPES.Items(), DataType_UNSIGNED_INTEGER_TYPES.Items()...)
	if !e.ArgB("zero_is_msb") && dhIsType(e, intTypes...) {
		// Use bitwise operations: (value >> position) & 1
		shifted := New(KBitwiseRightShift, "this", value, "expression", position)
		masked := New(KBitwiseAnd, "this", shifted, "expression", LiteralInt(1))
		return g.sql(masked)
	}

	return g.fn("GET_BIT", value, position)
}

// jarowinklerSimilarity mirrors dialect.jarowinkler_similarity(func).
func jarowinklerSimilarity(fn string) GenFunc {
	return func(g *Generator, e *Expr) string {
		this := e.This()
		expr := e.Expression()
		if e.ArgB("case_insensitive") {
			this = New(KUpper, "this", this)
			expr = New(KUpper, "this", expr)
		}

		return g.fn(fn, this, expr)
	}
}

// ---------------------------------------------------------------------------------------------
// Local ports of sqlglot builders / helpers used above (prefixed `dh`).
// ---------------------------------------------------------------------------------------------

// dhPyStr mirrors Python's str(value) / f"{value}" (expressions render with the default dialect).
func dhPyStr(v any) string {
	switch x := v.(type) {
	case nil:
		return "None"
	case *Expr:
		if x == nil {
			return "None"
		}
		return exprSQL(x)
	case string:
		return x
	case bool:
		if x {
			return "True"
		}
		return "False"
	case int:
		return strconv.Itoa(x)
	case DType:
		return "DType." + dtypeNames[x]
	}
	return fmt.Sprint(v)
}

// dhPyIntValue mirrors int(text) for a string accepted by isPyInt.
func dhPyIntValue(text string) int {
	s := strings.ReplaceAll(pyStrip(text), "_", "")
	n, err := strconv.Atoi(s)
	if err != nil {
		panic(&ValueError{Msg: "invalid literal for int() with base 10: " + pyRepr(text)})
	}
	return n
}

// dhIsType mirrors Expr.is_type(*dtypes) (DataType, Cast and generic `_type` variants).
func dhIsType(e *Expr, dtypes ...DType) bool {
	args := make([]any, len(dtypes))
	for i, d := range dtypes {
		args[i] = d
	}
	return dhIsTypeAny(e, args...)
}

func dhIsTypeAny(e *Expr, dtypes ...any) bool {
	if e == nil {
		return false
	}
	switch propOwner_is_type[e.kind] {
	case KDataType:
		return DataTypeIsType(e, dtypes, false)
	case KCast:
		to := e.ArgE("to")
		return to != nil && DataTypeIsType(to, dtypes, false)
	}
	t := e.RawType()
	return t != nil && DataTypeIsType(t, dtypes, false)
}

// dhConvert mirrors exp.convert(value, copy) for the value types used by the helpers.
func dhConvert(v any, copy bool) *Expr {
	switch x := v.(type) {
	case *Expr:
		if x == nil {
			return Null()
		}
		if copy {
			return x.Copy()
		}
		return x
	case string:
		return LiteralString(x)
	case bool:
		return Boolean(x)
	case nil:
		return Null()
	case int:
		return LiteralInt(x)
	}
	panic(&ValueError{Msg: fmt.Sprintf("Cannot convert %v", v)})
}

// dhBinop mirrors Expr._binop(klass, other) (i.e. the Python operator overloads `a + b`, `a.eq(b)`...).
func dhBinop(klass Kind, self *Expr, other any) *Expr {
	this := self.Copy()
	o := dhConvert(other, true)
	if !this.IsA(klass) && !o.IsA(klass) {
		this = wrapIfKind(this, KBinary)
		o = wrapIfKind(o, KBinary)
	}
	return New(klass, "this", this, "expression", o)
}

// dhFunc mirrors exp.func(name, *args) with the default dialect (args: *Expr, string or int).
func dhFunc(name string, args ...any) *Expr {
	d := MustDialect("")

	converted := make([]*Expr, 0, len(args))
	for _, a := range args {
		switch x := a.(type) {
		case *Expr:
			converted = append(converted, maybeParseExpr(x, true))
		case string:
			converted = append(converted, MaybeParse(x, KNone, "", d))
		default:
			// maybe_parse(1) parses str(1)
			converted = append(converted, MaybeParse(dhPyStr(x), KNone, "", d))
		}
	}

	var function *Expr
	var constructor FuncBuilder
	if d.P != nil {
		constructor = d.P.FUNCTIONS[pyUpper(name)]
	}
	if constructor != nil && len(converted) > 0 {
		function, converted = callFuncBuilder(constructor, converted, d)
	} else if constructor != nil {
		panic(&ValueError{Msg: fmt.Sprintf("Unable to convert '%s' into a Func. Either manually construct the Func expression of interest or parse the function call.", name)})
	} else {
		function = New(KAnonymous, "this", name, "expressions", converted)
	}

	for _, msg := range function.ErrorMessages(converted) {
		panic(&ValueError{Msg: msg})
	}

	return function
}

// dhApplyIndexOffset mirrors exp.apply_index_offset(this, expressions, offset, dialect).
func dhApplyIndexOffset(this *Expr, expressions []*Expr, offset int, d *Dialect) []*Expr {
	if offset == 0 || len(expressions) != 1 {
		return expressions
	}

	expression := expressions[0]

	if this.Type() == nil {
		annotateTypes(this, d)
	}

	if t := this.Type().DTypeOf(); t != DT_UNKNOWN && t != DT_ARRAY {
		return expressions
	}

	if expression.Type() == nil {
		annotateTypes(expression, d)
	}

	if DataType_INTEGER_TYPES.Has(expression.Type().DTypeOf()) {
		expression = simplifyExpr(dhBinop(KAdd, expression, offset), MustDialect(""))
		return []*Expr{expression}
	}

	return expressions
}

// dhSubsecondPrecision mirrors sqlglot.time.subsecond_precision (datetime.fromisoformat semantics).
//
// Given an ISO-8601 timestamp literal, eg '2023-01-01 12:13:14.123456+00:00'
// figure out its subsecond precision so we can construct types like DATETIME(6).
func dhSubsecondPrecision(timestampLiteral string) int {
	microsecond, ok := dhFromISOFormatMicrosecond(timestampLiteral)
	if !ok {
		return 0
	}
	subsecondDigitCount := len(strings.TrimRight(fmt.Sprintf("%06d", microsecond), "0"))
	precision := 0
	if subsecondDigitCount > 3 {
		precision = 6
	} else if subsecondDigitCount > 0 {
		precision = 3
	}
	return precision
}

// dhFromISOFormatMicrosecond mirrors CPython's datetime.fromisoformat (C implementation) and returns
// the parsed microsecond; ok=false where Python raises ValueError.
func dhFromISOFormatMicrosecond(s string) (int, bool) {
	r := []rune(s)
	n := len(r)
	if n < 7 {
		return 0, false
	}
	at := func(i int) rune {
		if i >= 0 && i < n {
			return r[i]
		}
		return 0
	}
	isDigit := func(c rune) bool { return c >= '0' && c <= '9' }
	parseDigits := func(p, num int) (int, int, bool) {
		v := 0
		for i := 0; i < num; i++ {
			c := at(p + i)
			if !isDigit(c) {
				return 0, p, false
			}
			v = v*10 + int(c-'0')
		}
		return v, p + num, true
	}

	// _find_isoformat_datetime_separator
	var sep int
	if n == 7 {
		sep = 7
	} else if at(4) == '-' {
		if at(5) == 'W' {
			if n < 8 {
				return 0, false
			}
			if n > 8 && at(8) == '-' {
				if n == 9 {
					return 0, false
				}
				if n > 10 && isDigit(at(10)) {
					sep = 8
				} else {
					sep = 10
				}
			} else {
				sep = 8
			}
		} else {
			sep = 10
		}
	} else if at(4) == 'W' {
		idx := 7
		for ; idx < n; idx++ {
			if !isDigit(at(idx)) {
				break
			}
		}
		if idx < 9 {
			sep = idx
		} else if idx%2 == 0 {
			sep = 7
		} else {
			sep = 8
		}
	} else {
		sep = 8
	}

	// parse_isoformat_date
	year, p, ok := parseDigits(0, 4)
	if !ok {
		return 0, false
	}
	usesSeparator := at(p) == '-'
	if usesSeparator {
		p++
	}
	var month, day int
	if at(p) == 'W' {
		p++
		isoWeek, np, ok := parseDigits(p, 2)
		if !ok {
			return 0, false
		}
		p = np
		isoDay := 1
		if p < sep {
			if usesSeparator {
				if at(p) != '-' {
					return 0, false
				}
				p++
			}
			isoDay, _, ok = parseDigits(p, 1)
			if !ok {
				return 0, false
			}
		}
		if !dhISOToYMD(year, isoWeek, isoDay) {
			return 0, false
		}
	} else {
		month, p, ok = parseDigits(p, 2)
		if !ok {
			return 0, false
		}
		if usesSeparator {
			if at(p) != '-' {
				return 0, false
			}
			p++
		}
		day, _, ok = parseDigits(p, 2)
		if !ok {
			return 0, false
		}
		if year < 1 || month < 1 || month > 12 || day < 1 || day > dhDaysInMonth(year, month) {
			return 0, false
		}
	}

	if n <= sep {
		return 0, true
	}

	// parse_isoformat_time
	start := sep + 1
	end := n
	tzPos := start
	for {
		c := at(tzPos)
		if c == 'Z' || c == '+' || c == '-' {
			break
		}
		tzPos++
		if tzPos >= end {
			break
		}
	}

	hour, minute, second, microsecond, rv := dhParseHHMMSSFF(at, start, tzPos)
	if rv < 0 {
		return 0, false
	}
	if tzPos == end {
		if rv == 1 {
			return 0, false
		}
	} else if at(tzPos) == 'Z' {
		if at(tzPos+1) != 0 {
			return 0, false
		}
	} else {
		tzh, tzm, tzs, tzus, rv := dhParseHHMMSSFF(at, tzPos+1, end)
		if rv != 0 {
			return 0, false
		}
		// timezone offsets must be strictly between -24h and 24h (timedelta normalizes the components)
		total := ((tzh*3600+tzm*60+tzs)*1000000 + tzus)
		if total >= 24*3600*1000000 {
			return 0, false
		}
	}

	if hour > 23 || minute > 59 || second > 59 || microsecond > 999999 {
		return 0, false
	}
	return microsecond, true
}

// dhParseHHMMSSFF mirrors CPython's parse_hh_mm_ss_ff over runes [p, pEnd). Returns rv < 0 on error,
// 1 when trailing characters remain and 0 on a clean parse.
func dhParseHHMMSSFF(at func(int) rune, p, pEnd int) (hour, minute, second, microsecond, rv int) {
	isDigit := func(c rune) bool { return c >= '0' && c <= '9' }
	vals := [3]int{}
	hasSeparator := true
	for i := 0; i < 3; i++ {
		v := 0
		for j := 0; j < 2; j++ {
			c := at(p + j)
			if !isDigit(c) {
				return 0, 0, 0, 0, -3
			}
			v = v*10 + int(c-'0')
		}
		vals[i] = v
		p += 2

		c := at(p)
		p++
		if i == 0 {
			hasSeparator = c == ':'
		}

		if p >= pEnd {
			if c != 0 {
				return vals[0], vals[1], vals[2], 0, 1
			}
			return vals[0], vals[1], vals[2], 0, 0
		} else if hasSeparator && c == ':' {
			if i == 2 {
				return 0, 0, 0, 0, -4
			}
			continue
		} else if c == '.' || c == ',' {
			break
		} else if !hasSeparator {
			p--
		} else {
			return 0, 0, 0, 0, -4
		}
	}

	// Parse fractional components
	lenRemains := pEnd - p
	toParse := lenRemains
	if lenRemains >= 6 {
		toParse = 6
	}
	us := 0
	for j := 0; j < toParse; j++ {
		c := at(p + j)
		if !isDigit(c) {
			return 0, 0, 0, 0, -3
		}
		us = us*10 + int(c-'0')
	}
	p += toParse
	correction := []int{100000, 10000, 1000, 100, 10}
	if toParse < 6 && toParse > 0 {
		us *= correction[toParse-1]
	}
	for isDigit(at(p)) {
		p++ // skip truncated digits
	}
	if at(p) != 0 {
		return vals[0], vals[1], vals[2], us, 1
	}
	return vals[0], vals[1], vals[2], us, 0
}

// dhDaysInMonth returns the number of days of the given month (proleptic Gregorian).
func dhDaysInMonth(year, month int) int {
	return time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// dhISOToYMD validates an ISO calendar date (mirrors datetime's iso_to_ymd range checks).
func dhISOToYMD(isoYear, isoWeek, isoDay int) bool {
	if isoYear < 1 || isoYear > 9999 {
		return false
	}
	jan1 := time.Date(isoYear, 1, 1, 0, 0, 0, 0, time.UTC)
	if isoWeek <= 0 || isoWeek >= 53 {
		outOfRange := true
		if isoWeek == 53 {
			// ISO years have 53 weeks in them on years starting with a Thursday
			// and leap years starting on a Wednesday
			leap := isoYear%4 == 0 && (isoYear%100 != 0 || isoYear%400 == 0)
			if jan1.Weekday() == time.Thursday || (jan1.Weekday() == time.Wednesday && leap) {
				outOfRange = false
			}
		}
		if outOfRange {
			return false
		}
	}
	if isoDay <= 0 || isoDay >= 8 {
		return false
	}
	firstWeekday := (int(jan1.Weekday()) + 6) % 7 // Monday == 0
	week1Monday := jan1.AddDate(0, 0, -firstWeekday)
	if firstWeekday > 3 {
		week1Monday = week1Monday.AddDate(0, 0, 7)
	}
	result := week1Monday.AddDate(0, 0, (isoWeek-1)*7+(isoDay-1))
	return result.Year() >= 1 && result.Year() <= 9999
}

// dhTIMEZONES mirrors sqlglot.time.TIMEZONES (lowercased IANA names).
var dhTIMEZONES = newStrSet(
	"africa/abidjan", "africa/accra", "africa/addis_ababa", "africa/algiers", "africa/asmara", "africa/asmera",
	"africa/bamako", "africa/bangui", "africa/banjul", "africa/bissau", "africa/blantyre", "africa/brazzaville",
	"africa/bujumbura", "africa/cairo", "africa/casablanca", "africa/ceuta", "africa/conakry", "africa/dakar",
	"africa/dar_es_salaam", "africa/djibouti", "africa/douala", "africa/el_aaiun", "africa/freetown",
	"africa/gaborone", "africa/harare", "africa/johannesburg", "africa/juba", "africa/kampala",
	"africa/khartoum", "africa/kigali", "africa/kinshasa", "africa/lagos", "africa/libreville", "africa/lome",
	"africa/luanda", "africa/lubumbashi", "africa/lusaka", "africa/malabo", "africa/maputo", "africa/maseru",
	"africa/mbabane", "africa/mogadishu", "africa/monrovia", "africa/nairobi", "africa/ndjamena",
	"africa/niamey", "africa/nouakchott", "africa/ouagadougou", "africa/porto-novo", "africa/sao_tome",
	"africa/timbuktu", "africa/tripoli", "africa/tunis", "africa/windhoek", "america/adak", "america/anchorage",
	"america/anguilla", "america/antigua", "america/araguaina", "america/argentina/buenos_aires",
	"america/argentina/catamarca", "america/argentina/comodrivadavia", "america/argentina/cordoba",
	"america/argentina/jujuy", "america/argentina/la_rioja", "america/argentina/mendoza",
	"america/argentina/rio_gallegos", "america/argentina/salta", "america/argentina/san_juan",
	"america/argentina/san_luis", "america/argentina/tucuman", "america/argentina/ushuaia", "america/aruba",
	"america/asuncion", "america/atikokan", "america/atka", "america/bahia", "america/bahia_banderas",
	"america/barbados", "america/belem", "america/belize", "america/blanc-sablon", "america/boa_vista",
	"america/bogota", "america/boise", "america/buenos_aires", "america/cambridge_bay", "america/campo_grande",
	"america/cancun", "america/caracas", "america/catamarca", "america/cayenne", "america/cayman",
	"america/chicago", "america/chihuahua", "america/ciudad_juarez", "america/coral_harbour", "america/cordoba",
	"america/costa_rica", "america/creston", "america/cuiaba", "america/curacao", "america/danmarkshavn",
	"america/dawson", "america/dawson_creek", "america/denver", "america/detroit", "america/dominica",
	"america/edmonton", "america/eirunepe", "america/el_salvador", "america/ensenada", "america/fort_nelson",
	"america/fort_wayne", "america/fortaleza", "america/glace_bay", "america/godthab", "america/goose_bay",
	"america/grand_turk", "america/grenada", "america/guadeloupe", "america/guatemala", "america/guayaquil",
	"america/guyana", "america/halifax", "america/havana", "america/hermosillo", "america/indiana/indianapolis",
	"america/indiana/knox", "america/indiana/marengo", "america/indiana/petersburg",
	"america/indiana/tell_city", "america/indiana/vevay", "america/indiana/vincennes",
	"america/indiana/winamac", "america/indianapolis", "america/inuvik", "america/iqaluit", "america/jamaica",
	"america/jujuy", "america/juneau", "america/kentucky/louisville", "america/kentucky/monticello",
	"america/knox_in", "america/kralendijk", "america/la_paz", "america/lima", "america/los_angeles",
	"america/louisville", "america/lower_princes", "america/maceio", "america/managua", "america/manaus",
	"america/marigot", "america/martinique", "america/matamoros", "america/mazatlan", "america/mendoza",
	"america/menominee", "america/merida", "america/metlakatla", "america/mexico_city", "america/miquelon",
	"america/moncton", "america/monterrey", "america/montevideo", "america/montreal", "america/montserrat",
	"america/nassau", "america/new_york", "america/nipigon", "america/nome", "america/noronha",
	"america/north_dakota/beulah", "america/north_dakota/center", "america/north_dakota/new_salem",
	"america/nuuk", "america/ojinaga", "america/panama", "america/pangnirtung", "america/paramaribo",
	"america/phoenix", "america/port-au-prince", "america/port_of_spain", "america/porto_acre",
	"america/porto_velho", "america/puerto_rico", "america/punta_arenas", "america/rainy_river",
	"america/rankin_inlet", "america/recife", "america/regina", "america/resolute", "america/rio_branco",
	"america/rosario", "america/santa_isabel", "america/santarem", "america/santiago", "america/santo_domingo",
	"america/sao_paulo", "america/scoresbysund", "america/shiprock", "america/sitka", "america/st_barthelemy",
	"america/st_johns", "america/st_kitts", "america/st_lucia", "america/st_thomas", "america/st_vincent",
	"america/swift_current", "america/tegucigalpa", "america/thule", "america/thunder_bay", "america/tijuana",
	"america/toronto", "america/tortola", "america/vancouver", "america/virgin", "america/whitehorse",
	"america/winnipeg", "america/yakutat", "america/yellowknife", "antarctica/casey", "antarctica/davis",
	"antarctica/dumontdurville", "antarctica/macquarie", "antarctica/mawson", "antarctica/mcmurdo",
	"antarctica/palmer", "antarctica/rothera", "antarctica/south_pole", "antarctica/syowa", "antarctica/troll",
	"antarctica/vostok", "arctic/longyearbyen", "asia/aden", "asia/almaty", "asia/amman", "asia/anadyr",
	"asia/aqtau", "asia/aqtobe", "asia/ashgabat", "asia/ashkhabad", "asia/atyrau", "asia/baghdad",
	"asia/bahrain", "asia/baku", "asia/bangkok", "asia/barnaul", "asia/beirut", "asia/bishkek", "asia/brunei",
	"asia/calcutta", "asia/chita", "asia/choibalsan", "asia/chongqing", "asia/chungking", "asia/colombo",
	"asia/dacca", "asia/damascus", "asia/dhaka", "asia/dili", "asia/dubai", "asia/dushanbe", "asia/famagusta",
	"asia/gaza", "asia/harbin", "asia/hebron", "asia/ho_chi_minh", "asia/hong_kong", "asia/hovd",
	"asia/irkutsk", "asia/istanbul", "asia/jakarta", "asia/jayapura", "asia/jerusalem", "asia/kabul",
	"asia/kamchatka", "asia/karachi", "asia/kashgar", "asia/kathmandu", "asia/katmandu", "asia/khandyga",
	"asia/kolkata", "asia/krasnoyarsk", "asia/kuala_lumpur", "asia/kuching", "asia/kuwait", "asia/macao",
	"asia/macau", "asia/magadan", "asia/makassar", "asia/manila", "asia/muscat", "asia/nicosia",
	"asia/novokuznetsk", "asia/novosibirsk", "asia/omsk", "asia/oral", "asia/phnom_penh", "asia/pontianak",
	"asia/pyongyang", "asia/qatar", "asia/qostanay", "asia/qyzylorda", "asia/rangoon", "asia/riyadh",
	"asia/saigon", "asia/sakhalin", "asia/samarkand", "asia/seoul", "asia/shanghai", "asia/singapore",
	"asia/srednekolymsk", "asia/taipei", "asia/tashkent", "asia/tbilisi", "asia/tehran", "asia/tel_aviv",
	"asia/thimbu", "asia/thimphu", "asia/tokyo", "asia/tomsk", "asia/ujung_pandang", "asia/ulaanbaatar",
	"asia/ulan_bator", "asia/urumqi", "asia/ust-nera", "asia/vientiane", "asia/vladivostok", "asia/yakutsk",
	"asia/yangon", "asia/yekaterinburg", "asia/yerevan", "atlantic/azores", "atlantic/bermuda",
	"atlantic/canary", "atlantic/cape_verde", "atlantic/faeroe", "atlantic/faroe", "atlantic/jan_mayen",
	"atlantic/madeira", "atlantic/reykjavik", "atlantic/south_georgia", "atlantic/st_helena",
	"atlantic/stanley", "australia/act", "australia/adelaide", "australia/brisbane", "australia/broken_hill",
	"australia/canberra", "australia/currie", "australia/darwin", "australia/eucla", "australia/hobart",
	"australia/lhi", "australia/lindeman", "australia/lord_howe", "australia/melbourne", "australia/north",
	"australia/nsw", "australia/perth", "australia/queensland", "australia/south", "australia/sydney",
	"australia/tasmania", "australia/victoria", "australia/west", "australia/yancowinna", "brazil/acre",
	"brazil/denoronha", "brazil/east", "brazil/west", "canada/atlantic", "canada/central", "canada/eastern",
	"canada/mountain", "canada/newfoundland", "canada/pacific", "canada/saskatchewan", "canada/yukon", "cet",
	"chile/continental", "chile/easterisland", "cst6cdt", "cuba", "eet", "egypt", "eire", "est", "est5edt",
	"etc/gmt", "etc/gmt+0", "etc/gmt+1", "etc/gmt+10", "etc/gmt+11", "etc/gmt+12", "etc/gmt+2", "etc/gmt+3",
	"etc/gmt+4", "etc/gmt+5", "etc/gmt+6", "etc/gmt+7", "etc/gmt+8", "etc/gmt+9", "etc/gmt-0", "etc/gmt-1",
	"etc/gmt-10", "etc/gmt-11", "etc/gmt-12", "etc/gmt-13", "etc/gmt-14", "etc/gmt-2", "etc/gmt-3", "etc/gmt-4",
	"etc/gmt-5", "etc/gmt-6", "etc/gmt-7", "etc/gmt-8", "etc/gmt-9", "etc/gmt0", "etc/greenwich", "etc/uct",
	"etc/universal", "etc/utc", "etc/zulu", "europe/amsterdam", "europe/andorra", "europe/astrakhan",
	"europe/athens", "europe/belfast", "europe/belgrade", "europe/berlin", "europe/bratislava",
	"europe/brussels", "europe/bucharest", "europe/budapest", "europe/busingen", "europe/chisinau",
	"europe/copenhagen", "europe/dublin", "europe/gibraltar", "europe/guernsey", "europe/helsinki",
	"europe/isle_of_man", "europe/istanbul", "europe/jersey", "europe/kaliningrad", "europe/kiev",
	"europe/kirov", "europe/kyiv", "europe/lisbon", "europe/ljubljana", "europe/london", "europe/luxembourg",
	"europe/madrid", "europe/malta", "europe/mariehamn", "europe/minsk", "europe/monaco", "europe/moscow",
	"europe/nicosia", "europe/oslo", "europe/paris", "europe/podgorica", "europe/prague", "europe/riga",
	"europe/rome", "europe/samara", "europe/san_marino", "europe/sarajevo", "europe/saratov",
	"europe/simferopol", "europe/skopje", "europe/sofia", "europe/stockholm", "europe/tallinn", "europe/tirane",
	"europe/tiraspol", "europe/ulyanovsk", "europe/uzhgorod", "europe/vaduz", "europe/vatican", "europe/vienna",
	"europe/vilnius", "europe/volgograd", "europe/warsaw", "europe/zagreb", "europe/zaporozhye",
	"europe/zurich", "gb", "gb-eire", "gmt", "gmt+0", "gmt-0", "gmt0", "greenwich", "hongkong", "hst",
	"iceland", "indian/antananarivo", "indian/chagos", "indian/christmas", "indian/cocos", "indian/comoro",
	"indian/kerguelen", "indian/mahe", "indian/maldives", "indian/mauritius", "indian/mayotte",
	"indian/reunion", "iran", "israel", "jamaica", "japan", "kwajalein", "libya", "met", "mexico/bajanorte",
	"mexico/bajasur", "mexico/general", "mst", "mst7mdt", "navajo", "nz", "nz-chat", "pacific/apia",
	"pacific/auckland", "pacific/bougainville", "pacific/chatham", "pacific/chuuk", "pacific/easter",
	"pacific/efate", "pacific/enderbury", "pacific/fakaofo", "pacific/fiji", "pacific/funafuti",
	"pacific/galapagos", "pacific/gambier", "pacific/guadalcanal", "pacific/guam", "pacific/honolulu",
	"pacific/johnston", "pacific/kanton", "pacific/kiritimati", "pacific/kosrae", "pacific/kwajalein",
	"pacific/majuro", "pacific/marquesas", "pacific/midway", "pacific/nauru", "pacific/niue", "pacific/norfolk",
	"pacific/noumea", "pacific/pago_pago", "pacific/palau", "pacific/pitcairn", "pacific/pohnpei",
	"pacific/ponape", "pacific/port_moresby", "pacific/rarotonga", "pacific/saipan", "pacific/samoa",
	"pacific/tahiti", "pacific/tarawa", "pacific/tongatapu", "pacific/truk", "pacific/wake", "pacific/wallis",
	"pacific/yap", "poland", "portugal", "prc", "pst8pdt", "roc", "rok", "singapore", "turkey", "uct",
	"universal", "us/alaska", "us/aleutian", "us/arizona", "us/central", "us/east-indiana", "us/eastern",
	"us/hawaii", "us/indiana-starke", "us/michigan", "us/mountain", "us/pacific", "us/samoa", "utc", "w-su",
	"wet", "zulu",
)
