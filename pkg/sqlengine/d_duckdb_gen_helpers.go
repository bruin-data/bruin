package sqlengine

// Port of the module-level helper functions of sqlglot/generators/duckdb.py (sqlglot v30.13.0).

import (
	"fmt"
	"regexp"
	"strings"
)

var duckdbConnectByArgsToSkip = newStrSet("connect", "where", "from_", "with_", "expressions")

// Regex to detect time zones in timestamps of the form [+|-]TT[:tt]
// The pattern matches timezone offsets that appear after the time portion.
var duckdbTimezonePattern = regexp.MustCompile(`:\d{2}.*?[+\-]\d{2}(?::\d{2})?`)

// Characters that must be escaped when building regex expressions in INITCAP (ordered).
var duckdbRegexEscapeReplacements = [][2]string{
	{"\\", "\\\\"},
	{"-", `\-`},
	{"^", `\^`},
	{"[", `\[`},
	{"]", `\]`},
}

func duckdbRegexEscapeReplacement(ch string) (string, bool) {
	for _, kv := range duckdbRegexEscapeReplacements {
		if kv[0] == ch {
			return kv[1], true
		}
	}
	return "", false
}

// Whitespace control characters that DuckDB must process with `CHR({val})` calls.
var duckdbWSControlCharsToDuck = map[rune]int{
	'\u000b': 11,
	'\u001c': 28,
	'\u001d': 29,
	'\u001e': 30,
	'\u001f': 31,
}

func duckdbMaxBitPosition() *Expr { return LiteralInt(32768) }

// cs/as/ps are Snowflake defaults; DuckDB already behaves the same way, so they are safe to drop.
// Note: "as" is also a reserved keyword in DuckDB, making it impossible to pass through.
var (
	duckdbSnowflakeCollationDefaults    = newStrSet("cs", "as", "ps")
	duckdbSnowflakeCollationUnsupported = newStrSet(
		"ci", "ai", "upper", "lower", "utf8", "bin", "pi", "fl", "fu", "trim", "ltrim", "rtrim",
	)
)

// Window functions that support IGNORE/RESPECT NULLS in DuckDB

var duckdbSeqRestricted = []Kind{KWhere, KHaving, KAggFunc, KOrder, KSelect}

// Maps SEQ expression types to their byte width (suffix indicates bytes: SEQ1=1, SEQ2=2, etc.)
var duckdbSeqByteWidth = map[Kind]int{KSeq1: 1, KSeq2: 2, KSeq4: 4, KSeq8: 8}

// duckdbWeekStartDayToDow mirrors dialect.WEEK_START_DAY_TO_DOW in insertion order.
var duckdbWeekStartDayToDow = []struct {
	day string
	dow int
}{
	{"MONDAY", 1},
	{"TUESDAY", 2},
	{"WEDNESDAY", 3},
	{"THURSDAY", 4},
	{"FRIDAY", 5},
	{"SATURDAY", 6},
	{"SUNDAY", 7},
}

// duckdbApplyBase64AlphabetReplacements mirrors _apply_base64_alphabet_replacements.
//
// Base64 alphabet can be 1-3 chars: 1st = index 62 ('+'), 2nd = index 63 ('/'), 3rd = padding ('=').
// zip truncates to the shorter string, so 1-char alphabet only replaces '+', 2-char replaces '+/', etc.
func duckdbApplyBase64AlphabetReplacements(result, alphabet *Expr, reverse bool) *Expr {
	if alphabet.IsA(KLiteral) && alphabet.IsString() {
		defaults := []rune("+/=")
		chars := []rune(alphabet.ThisS())
		for i := 0; i < len(defaults) && i < len(chars); i++ {
			defaultChar, newChar := string(defaults[i]), string(chars[i])
			if newChar != defaultChar {
				find, replace := defaultChar, newChar
				if reverse {
					find, replace = newChar, defaultChar
				}
				result = New(
					KReplace,
					"this", result,
					"expression", LiteralString(find),
					"replacement", LiteralString(replace),
				)
			}
		}
	}
	return result
}

// duckdbBase64DecodeSQL mirrors _base64_decode_sql.
//
// Transpile Snowflake BASE64_DECODE_STRING/BINARY to DuckDB.
// DuckDB uses FROM_BASE64() which returns BLOB. For string output, wrap with DECODE().
func duckdbBase64DecodeSQL(g *Generator, e *Expr, toString bool) string {
	inputExpr := e.This()
	alphabet := e.ArgE("alphabet")

	// Handle custom alphabet by replacing non-standard chars with standard ones
	inputExpr = duckdbApplyBase64AlphabetReplacements(inputExpr, alphabet, true)

	// FROM_BASE64 returns BLOB
	inputExpr = New(KFromBase64, "this", inputExpr)

	if toString {
		inputExpr = New(KDecode, "this", inputExpr)
	}

	return g.sql(inputExpr)
}

// duckdbLastDaySQL mirrors _last_day_sql.
//
// DuckDB's LAST_DAY only supports finding the last day of a month.
// For other date parts (year, quarter, week), we need to implement equivalent logic.
func duckdbLastDaySQL(g *Generator, e *Expr) string {
	dateExpr := e.This()
	unit := e.Text("unit")

	if unit == "" || pyUpper(unit) == "MONTH" {
		// Default behavior - use DuckDB's native LAST_DAY
		return g.fn("LAST_DAY", dateExpr)
	}

	if pyUpper(unit) == "YEAR" {
		// Last day of year: December 31st of the same year
		yearExpr := duckdbFunc("EXTRACT", "YEAR", dateExpr)
		makeDateExpr := duckdbFunc("MAKE_DATE", yearExpr, LiteralInt(12), LiteralInt(31))
		return g.sql(makeDateExpr)
	}

	if pyUpper(unit) == "QUARTER" {
		// Last day of quarter
		yearExpr := duckdbFunc("EXTRACT", "YEAR", dateExpr)
		quarterExpr := duckdbFunc("EXTRACT", "QUARTER", dateExpr)

		// Calculate last month of quarter: quarter * 3. Quarter can be 1 to 4
		lastMonthExpr := New(KMul, "this", quarterExpr, "expression", LiteralInt(3))
		firstDayLastMonthExpr := duckdbFunc("MAKE_DATE", yearExpr, lastMonthExpr, LiteralInt(1))

		// Last day of the last month of the quarter
		lastDayExpr := duckdbFunc("LAST_DAY", firstDayLastMonthExpr)
		return g.sql(lastDayExpr)
	}

	if pyUpper(unit) == "WEEK" {
		// DuckDB DAYOFWEEK: Sunday=0, Monday=1, ..., Saturday=6
		dow := duckdbFunc("EXTRACT", "DAYOFWEEK", dateExpr)
		// Days to the last day of week: (7 - dayofweek) % 7, assuming the last day of week is Sunday (Snowflake)
		// Wrap in parentheses to ensure correct precedence
		daysToSundayExpr := New(
			KMod,
			"this", New(KParen, "this", New(KSub, "this", LiteralInt(7), "expression", dow)),
			"expression", LiteralInt(7),
		)
		intervalExpr := New(KInterval, "this", daysToSundayExpr, "unit", VarExpr("DAY"))
		addExpr := New(KAdd, "this", dateExpr, "expression", intervalExpr)
		castExpr := CastExpr(addExpr, DT_DATE, true, nil)
		return g.sql(castExpr)
	}

	g.unsupported(fmt.Sprintf("Unsupported date part '%s' in LAST_DAY function", unit))
	return g.functionFallbackSQL(e)
}

// duckdbIsNanosecondUnit mirrors _is_nanosecond_unit.
func duckdbIsNanosecondUnit(unit *Expr) bool {
	return unit.IsA(KVar, KLiteral) && pyUpper(unit.Name()) == "NANOSECOND"
}

// duckdbHandleNanosecondDiff mirrors _handle_nanosecond_diff.
// Generate NANOSECOND diff using EPOCH_NS since DATE_DIFF doesn't support it.
func duckdbHandleNanosecondDiff(g *Generator, endTime, startTime *Expr) string {
	endNS := CastExpr(endTime, DT_TIMESTAMP_NS, true, nil)
	startNS := CastExpr(startTime, DT_TIMESTAMP_NS, true, nil)

	// Build expression tree: EPOCH_NS(end) - EPOCH_NS(start)
	return g.sql(New(KSub, "this", duckdbFunc("EPOCH_NS", endNS), "expression", duckdbFunc("EPOCH_NS", startNS)))
}

// duckdbToBooleanSQL mirrors _to_boolean_sql.
//
// Transpile TO_BOOLEAN and TRY_TO_BOOLEAN functions from Snowflake to DuckDB equivalent.
func duckdbToBooleanSQL(g *Generator, e *Expr) string {
	arg := e.This()
	isSafe := e.ArgB("safe")

	baseCaseExpr := duckdbCase()
	// Handle 'on' -> TRUE (case insensitive)
	baseCaseExpr = duckdbWhen(baseCaseExpr,
		duckdbEQ(New(KUpper, "this", CastExpr(arg, DT_VARCHAR, true, nil)), LiteralString("ON")),
		Boolean(true), true)
	// Handle 'off' -> FALSE (case insensitive)
	baseCaseExpr = duckdbWhen(baseCaseExpr,
		duckdbEQ(New(KUpper, "this", CastExpr(arg, DT_VARCHAR, true, nil)), LiteralString("OFF")),
		Boolean(false), true)

	var caseExpr *Expr
	if isSafe {
		// TRY_TO_BOOLEAN: handle 'on'/'off' and use TRY_CAST for everything else
		caseExpr = duckdbElse(baseCaseExpr, duckdbFunc("TRY_CAST", arg, duckdbNewTypeExpr(DT_BOOLEAN)), true)
	} else {
		// TO_BOOLEAN: handle NaN/INF errors, 'on'/'off', and use regular CAST
		castToReal := duckdbFunc("TRY_CAST", arg, duckdbNewTypeExpr(DT_FLOAT))

		// Check for NaN and INF values
		nanInfCheck := New(
			KOr,
			"this", duckdbFunc("ISNAN", castToReal),
			"expression", duckdbFunc("ISINF", castToReal),
		)

		caseExpr = duckdbWhen(baseCaseExpr, nanInfCheck,
			duckdbFunc("ERROR", LiteralString("TO_BOOLEAN: Non-numeric values NaN and INF are not supported")),
			true)
		caseExpr = duckdbElse(caseExpr, CastExpr(arg, DT_BOOLEAN, true, nil), true)
	}

	return g.sql(caseExpr)
}

// duckdbDateSQL mirrors _date_sql (BigQuery -> DuckDB conversion for the DATE function).
func duckdbDateSQL(g *Generator, e *Expr) string {
	this := e.This()
	zone := g.sqlKey(e, "zone")

	if zone != "" {
		// BigQuery considers "this" at UTC, converts it to the specified
		// time zone and then keeps only the DATE part
		// To micmic that, we:
		//   (1) Cast to TIMESTAMP to remove DuckDB's local tz
		//   (2) Apply consecutive AtTimeZone calls for UTC -> zone conversion
		this = CastExpr(this, DT_TIMESTAMP, true, nil)
		atUTC := New(KAtTimeZone, "this", this, "zone", LiteralString("UTC"))
		this = New(KAtTimeZone, "this", atUTC, "zone", zone)
	}

	return g.sql(CastExpr(this, DT_DATE, true, nil))
}

// duckdbTimediffSQL mirrors _timediff_sql (BigQuery -> DuckDB conversion for TIME_DIFF).
func duckdbTimediffSQL(g *Generator, e *Expr) string {
	unit := e.ArgE("unit")

	if duckdbIsNanosecondUnit(unit) {
		return duckdbHandleNanosecondDiff(g, e.Expression(), e.This())
	}

	this := CastExpr(e.This(), DT_TIME, true, nil)
	expr := CastExpr(e.Expression(), DT_TIME, true, nil)

	// Although the 2 dialects share similar signatures, BQ seems to inverse
	// the sign of the result so the start/end time operands are flipped
	return g.fn("DATE_DIFF", unitToStr(e, "DAY"), expr, this)
}

// duckdbDateDeltaToBinaryIntervalOp mirrors _date_delta_to_binary_interval_op.
//
// DuckDB override to handle:
// 1. NANOSECOND operations (DuckDB doesn't support INTERVAL ... NANOSECOND)
// 2. Float/decimal interval values (DuckDB INTERVAL requires integers).
func duckdbDateDeltaToBinaryIntervalOp(cast bool) GenFunc {
	baseImpl := dateDeltaToBinaryIntervalOp(cast)

	return func(g *Generator, e *Expr) string {
		unit := e.ArgE("unit")
		intervalValue := e.Expression()

		// Handle NANOSECOND unit (DuckDB doesn't support INTERVAL ... NANOSECOND)
		if duckdbIsNanosecondUnit(unit) {
			if intervalValue.IsA(KInterval) {
				intervalValue = intervalValue.This()
			}

			timestampNS := CastExpr(e.This(), DT_TIMESTAMP_NS, true, nil)

			return g.sql(duckdbFunc(
				"MAKE_TIMESTAMP_NS",
				New(KAdd, "this", duckdbFunc("EPOCH_NS", timestampNS), "expression", intervalValue),
			))
		}

		// Handle float/decimal interval values as duckDB INTERVAL requires integer expressions
		if intervalValue == nil || intervalValue.IsA(KInterval) {
			return baseImpl(g, e)
		}

		if duckdbIsType(intervalValue, DataType_REAL_TYPES.Items()...) {
			e.Set("expression", CastExpr(duckdbFunc("ROUND", intervalValue), "INT", true, nil))
		}

		return baseImpl(g, e)
	}
}

// duckdbArrayInsertSQL mirrors _array_insert_sql.
//
// Transpile ARRAY_INSERT to DuckDB using LIST_CONCAT and slicing.
func duckdbArrayInsertSQL(g *Generator, e *Expr) string {
	this := e.This()
	position := e.ArgE("position")
	element := e.Expression()
	elementArray := New(KArray, "expressions", []*Expr{element})
	indexOffset := duckdbArgInt(e, "offset", 0)

	if position == nil || !position.IsInt() {
		g.unsupported("ARRAY_INSERT can only be transpiled with a literal position")
		return g.fn("ARRAY_INSERT", this, position, element)
	}

	posValue, _ := duckdbToPyInt(position)

	// Normalize one-based indexing to zero-based for slice calculations
	// Spark (1-based) -> Snowflake (0-based):
	//   Positive: pos=1 -> pos=0 (subtract 1)
	//   Negative: pos=-2 -> pos=-1 (add 1)
	// Example: Spark array_insert([a,b,c], -2, d) -> [a,b,d,c] is same as Snowflake pos=-1
	if posValue > 0 {
		posValue = posValue - indexOffset
	} else if posValue < 0 {
		posValue = posValue + indexOffset
	}

	// Build the appropriate list_concat expression based on position
	var concatExprs []any
	if posValue == 0 {
		// insert at beginning
		concatExprs = []any{elementArray, this}
	} else if posValue > 0 {
		// Positive position: LIST_CONCAT(arr[1:pos], [elem], arr[pos+1:])
		// 0-based -> DuckDB 1-based slicing

		// left slice: arr[1:pos]
		sliceStart := New(
			KBracket,
			"this", this,
			"expressions", []*Expr{New(KSlice, "this", LiteralInt(1), "expression", LiteralInt(posValue))},
		)

		// right slice: arr[pos+1:]
		sliceEnd := New(KBracket, "this", this, "expressions", []*Expr{New(KSlice, "this", LiteralInt(posValue+1))})

		concatExprs = []any{sliceStart, elementArray, sliceEnd}
	} else {
		// Negative position: arr[1:LEN(arr)+pos], [elem], arr[LEN(arr)+pos+1:]
		// pos=-1 means insert before last element
		arrLen := New(KLength, "this", this)

		// Calculate slice position: LEN(arr) + pos (e.g., LEN(arr) + (-1) = LEN(arr) - 1)
		sliceEndPos := duckdbAdd(arrLen, LiteralInt(posValue))
		sliceStartPos := duckdbAdd(sliceEndPos, LiteralInt(1))

		// left slice: arr[1:LEN(arr)+pos]
		sliceStart := New(
			KBracket,
			"this", this,
			"expressions", []*Expr{New(KSlice, "this", LiteralInt(1), "expression", sliceEndPos)},
		)

		// right slice: arr[LEN(arr)+pos+1:]
		sliceEnd := New(KBracket, "this", this, "expressions", []*Expr{New(KSlice, "this", sliceStartPos)})

		concatExprs = []any{sliceStart, elementArray, sliceEnd}
	}

	// All dialects that support ARRAY_INSERT propagate NULLs (Snowflake/Spark/Databricks)
	// Wrap in CASE WHEN array IS NULL THEN NULL ELSE func_expr END
	return g.sql(New(
		KIf,
		"this", New(KIs, "this", this, "expression", Null()),
		"true", Null(),
		"false", g.fn("LIST_CONCAT", concatExprs...),
	))
}

// duckdbArrayRemoveAtSQL mirrors _array_remove_at_sql.
//
// Transpile ARRAY_REMOVE_AT to DuckDB using LIST_CONCAT and slicing.
func duckdbArrayRemoveAtSQL(g *Generator, e *Expr) string {
	this := e.This()
	position := e.ArgE("position")

	if position == nil || !position.IsInt() {
		g.unsupported("ARRAY_REMOVE_AT can only be transpiled with a literal position")
		return g.fn("ARRAY_REMOVE_AT", this, position)
	}

	posValue, _ := duckdbToPyInt(position)

	// Build the appropriate expression based on position
	var resultExpr any
	if posValue == 0 {
		// Remove first element: arr[2:]
		resultExpr = New(KBracket, "this", this, "expressions", []*Expr{New(KSlice, "this", LiteralInt(2))})
	} else if posValue > 0 {
		// Remove at positive position: LIST_CONCAT(arr[1:pos], arr[pos+2:])
		// DuckDB uses 1-based slicing
		leftSlice := New(
			KBracket,
			"this", this,
			"expressions", []*Expr{New(KSlice, "this", LiteralInt(1), "expression", LiteralInt(posValue))},
		)
		rightSlice := New(KBracket, "this", this, "expressions", []*Expr{New(KSlice, "this", LiteralInt(posValue+2))})
		resultExpr = g.fn("LIST_CONCAT", leftSlice, rightSlice)
	} else if posValue == -1 {
		// Remove last element: arr[1:LEN(arr)-1]
		// Optimization: simpler than general negative case
		arrLen := New(KLength, "this", this)
		sliceEnd := duckdbAdd(arrLen, LiteralInt(-1))
		resultExpr = New(
			KBracket,
			"this", this,
			"expressions", []*Expr{New(KSlice, "this", LiteralInt(1), "expression", sliceEnd)},
		)
	} else {
		// Remove at negative position: LIST_CONCAT(arr[1:LEN(arr)+pos], arr[LEN(arr)+pos+2:])
		arrLen := New(KLength, "this", this)
		sliceEndPos := duckdbAdd(arrLen, LiteralInt(posValue))
		sliceStartPos := duckdbAdd(sliceEndPos, LiteralInt(2))

		leftSlice := New(
			KBracket,
			"this", this,
			"expressions", []*Expr{New(KSlice, "this", LiteralInt(1), "expression", sliceEndPos)},
		)
		rightSlice := New(KBracket, "this", this, "expressions", []*Expr{New(KSlice, "this", sliceStartPos)})
		resultExpr = g.fn("LIST_CONCAT", leftSlice, rightSlice)
	}

	// Snowflake ARRAY_FUNCS_PROPAGATES_NULLS=True, so wrap in NULL check
	// CASE WHEN array IS NULL THEN NULL ELSE result_expr END
	return g.sql(New(
		KIf,
		"this", New(KIs, "this", this, "expression", Null()),
		"true", Null(),
		"false", resultExpr,
	))
}

// duckdbArraySortSQL mirrors _array_sort_sql.
func duckdbArraySortSQL(g *Generator, e *Expr) string {
	if e.ArgB("expression") {
		g.unsupported("DuckDB's ARRAY_SORT does not support a comparator.")
	}
	return g.fn("ARRAY_SORT", e.Arg("this"))
}

// duckdbArrayContainsSQL mirrors _array_contains_sql.
func duckdbArrayContainsSQL(g *Generator, e *Expr) string {
	this := e.This()
	expr := e.Expression()

	fn := g.fn("ARRAY_CONTAINS", this, expr)

	if e.ArgB("check_null") {
		checkNullInArray := New(
			KNullif,
			"this", New(KNEQ, "this", New(KArraySize, "this", this), "expression", duckdbFunc("LIST_COUNT", this)),
			"expression", Boolean(false),
		)
		return g.sql(New(KIf, "this", duckdbIs(expr, Null()), "true", checkNullInArray, "false", fn))
	}

	return fn
}

// duckdbArrayOverlapsSQL mirrors _array_overlaps_sql.
//
// Translates Snowflake's NULL-safe ARRAYS_OVERLAP to DuckDB.
func duckdbArrayOverlapsSQL(g *Generator, e *Expr) string {
	if !e.ArgB("null_safe") {
		return g.binary(e, "&&")
	}

	arr1 := e.This()
	arr2 := e.Expression()

	checkNulls := AndExprOpts([]*Expr{
		New(
			KNEQ,
			"this", New(KArraySize, "this", arr1.Copy()),
			"expression", duckdbFunc("LIST_COUNT", arr1.Copy()),
		),
		New(
			KNEQ,
			"this", New(KArraySize, "this", arr2.Copy()),
			"expression", duckdbFunc("LIST_COUNT", arr2.Copy()),
		),
	}, false, true)

	overlap := New(KArrayOverlaps, "this", arr1.Copy(), "expression", arr2.Copy())

	return g.sql(OrExprOpts([]*Expr{
		ParenExpr(overlap, false),
		ParenExpr(checkNulls, false),
	}, false, false))
}

// duckdbStructSQL mirrors _struct_sql.
func duckdbStructSQL(g *Generator, e *Expr) string {
	ancestorCast := e.FindAncestor(KCast, KSelect)
	if ancestorCast.IsA(KSelect) {
		ancestorCast = nil
	}

	// Empty struct cast works with MAP() since DuckDB can't parse {}
	if len(e.Expressions()) == 0 {
		if ancestorCast.IsA(KCast) && duckdbIsType(ancestorCast.ArgE("to"), DT_MAP) {
			return "MAP()"
		}
	}

	var args []string

	// BigQuery allows inline construction such as "STRUCT<a STRING, b INTEGER>('str', 1)" which is
	// canonicalized to "ROW('str', 1) AS STRUCT(a TEXT, b INT)" in DuckDB
	// The transformation to ROW will take place if:
	//  1. The STRUCT itself does not have proper fields (key := value) as a "proper" STRUCT would
	//  2. A cast to STRUCT / ARRAY of STRUCTs is found
	isBqInlineStruct := false
	if e.Find(KPropertyEQ) == nil && ancestorCast != nil {
		for castedType := range ancestorCast.FindAll(KDataType) {
			if duckdbIsType(castedType, DT_STRUCT) {
				isBqInlineStruct = true
				break
			}
		}
	}

	for i, expr := range e.Expressions() {
		isPropertyEQ := expr.IsA(KPropertyEQ)
		this, _ := expr.Arg("this").(*Expr)
		value := expr
		if isPropertyEQ {
			value = expr.Expression()
		}

		if isBqInlineStruct {
			args = append(args, g.sql(value))
		} else {
			var key string
			if this.IsA(KIdentifier) {
				key = g.sql(LiteralString(expr.Name()))
			} else if isPropertyEQ {
				key = g.sql(this)
			} else {
				key = g.sql(LiteralString(fmt.Sprintf("_%d", i)))
			}

			args = append(args, key+": "+g.sql(value))
		}
	}

	csvArgs := strings.Join(args, ", ")

	if isBqInlineStruct {
		return "ROW(" + csvArgs + ")"
	}
	return "{" + csvArgs + "}"
}

// duckdbDatatypeSQL mirrors _datatype_sql.
func duckdbDatatypeSQL(g *Generator, e *Expr) string {
	if duckdbIsType(e, DT_ARRAY) {
		return g.expressions(e, exprsOpts{flat: true}) + "[" + g.expressions(e, exprsOpts{key: "values", flat: true}) + "]"
	}

	// Modifiers are not supported for TIME, [TIME | TIMESTAMP] WITH TIME ZONE
	if duckdbIsType(e, DT_TIME, DT_TIMETZ, DT_TIMESTAMPTZ) {
		return dtypeValues[e.DTypeOf()]
	}

	return g.datatypeSQL(e)
}

// duckdbJSONFormatSQL mirrors _json_format_sql.
func duckdbJSONFormatSQL(g *Generator, e *Expr) string {
	sql := g.fn("TO_JSON", e.Arg("this"), e.ArgE("options"))
	return "CAST(" + sql + " AS TEXT)"
}

// duckdbBuildSeqExpression mirrors _build_seq_expression.
// Build a SEQ expression with the given base, byte width, and signedness.
func duckdbBuildSeqExpression(base *Expr, byteWidth int, signed bool) *Expr {
	bits := byteWidth * 8
	maxVal := LiteralNumber(duckdbPow2(bits))

	if signed {
		half := LiteralNumber(duckdbPow2(bits - 1))
		return duckdbReplacePlaceholders(duckdbSeqSigned.get().Copy(), map[string]*Expr{
			"base": base, "max_val": maxVal, "half": half,
		})
	}
	return duckdbReplacePlaceholders(duckdbSeqUnsigned.get().Copy(), map[string]*Expr{
		"base": base, "max_val": maxVal,
	})
}

// duckdbSeqToRangeInGenerator mirrors _seq_to_range_in_generator.
//
// Transform SEQ functions to `range` column references when inside a GENERATOR context.
func duckdbSeqToRangeInGenerator(expression *Expr) *Expr {
	if !expression.IsA(KSelect) {
		return expression
	}

	from := expression.ArgE("from_")
	if !(from != nil && from.This().IsA(KTableFromRows) && from.This().This().IsA(KGenerator)) {
		return expression
	}

	replaceSeq := func(node *Expr) *Expr {
		if node.IsA(KSeq1, KSeq2, KSeq4, KSeq8) {
			byteWidth := duckdbSeqByteWidth[node.Kind()]
			return duckdbBuildSeqExpression(ColumnExpr("range", nil, nil, nil, nil, nil, true), byteWidth, node.Name() == "1")
		}
		return node
	}

	return expression.Transform(replaceSeq, false)
}

// duckdbConnectByToRecursiveCte mirrors connect_by_to_recursive_cte.
//
// Rewrites START WITH ... CONNECT BY PRIOR into WITH RECURSIVE
// Falls through unchanged if there are no PRIORs.
func duckdbConnectByToRecursiveCte(expression *Expr) *Expr {
	if !expression.IsA(KSelect) || !expression.ArgB("connect") {
		return expression
	}

	connect := expression.ArgE("connect")
	connectPred := connect.ArgE("connect")

	priors := connectPred.FindAllList(KPrior)
	if len(priors) == 0 {
		return expression
	}

	from := expression.ArgE("from_")
	if from == nil || expression.ArgB("joins") {
		return expression
	}

	sourceTable := from.This()
	baseSelectExprs := expression.Expressions()
	baseWhere := expression.ArgE("where")
	baseWith := expression.ArgE("with_")

	// LEVEL is a Snowflake pseudo-column: it's always computed as a depth counter in the CTE.
	hasLevel := false
	for _, e := range baseSelectExprs {
		for col := range e.FindAll(KColumn) {
			if col.IsA(KColumn) && pyUpper(col.Name()) == "LEVEL" {
				hasLevel = true
			}
		}
	}
	hasStar := expression.IsStar()

	// CONNECT_BY_ROOT col yields the value of `col` from the START WITH row that begins each
	// branch. Each one is threaded through the CTE as an extra column: the anchor binds it to the
	// row's own value, the recursive arm forwards the parent's value unchanged.
	var rootColNames []string
	var anchorRootCols []*Expr
	var innerRootCols []*Expr
	var roots []*Expr
	for _, e := range baseSelectExprs {
		for root := range e.FindAll(KConnectByRoot) {
			roots = append(roots, root)
		}
	}

	for i, root := range roots {
		name := fmt.Sprintf("_connect_by_root_%d", i)
		rootColNames = append(rootColNames, name)
		anchorRootCols = append(anchorRootCols, AliasExpr(root.This(), name, nil, true))
		innerRootCols = append(innerRootCols, AliasExpr(ColumnExpr(name, "_parent_row", nil, nil, nil, nil, true), name, nil, true))
		root.Replace(ColumnExpr(name, nil, nil, nil, nil, nil, true))
	}

	// Build the join condition from the full CONNECT BY predicate:
	// PRIOR(col) → _parent_row.col, unqualified cols → _child_row.col.
	qualifyConnectPred := func(node *Expr) *Expr {
		for col := range FindAllInScope(node, KColumn) {
			tableName := "_child_row"
			if col.Parent().IsA(KPrior) {
				tableName = "_parent_row"
			}
			col.Set("table", ToIdentifier(tableName, nil))
		}
		for prior := range FindAllInScope(node, KPrior) {
			prior.Replace(prior.This())
		}
		return node
	}

	// Avoid colliding with any CTE names already on the query.
	taken := newStrSet()
	if baseWith != nil {
		for _, cte := range baseWith.Expressions() {
			taken.Add(cte.Alias())
		}
	}
	cteName := findNewName(taken, "_rootcte")

	// Anchor: project all source columns + seed LEVEL at 1 + bind each root column to its own value.
	anchorExprs := []*Expr{Star(), AliasExpr(LiteralInt(1), "level", nil, true)}
	anchorExprs = append(anchorExprs, anchorRootCols...)
	anchor := SelectExpr(anchorExprs...).SelectFrom(sourceTable, true)
	if connect.ArgB("start") {
		anchor = anchor.QueryWhere([]*Expr{connect.ArgE("start")}, true, true)
	}

	// Recursive arm: carry all child columns + increment level + forward each root value.
	// SELECT * in both arms means WHERE/PRIOR columns are always available without explicit tracking.
	innerExprs := []*Expr{
		New(KColumn, "this", Star(), "table", ToIdentifier("_child_row", nil)),
		AliasExpr(duckdbAdd(ColumnExpr("level", "_parent_row", nil, nil, nil, nil, true), 1), "level", nil, true),
	}
	innerExprs = append(innerExprs, innerRootCols...)
	parentTable := MaybeParse(cteName, KTable, "", nil).ExprAs("_parent_row", nil, true)
	innerQuery := SelectExpr(innerExprs...).
		SelectFrom(sourceTable.ExprAs("_child_row", nil, true), true).
		SelectJoin(parentTable, []*Expr{qualifyConnectPred(connectPred)}, nil, true, "", nil, true)

	// Outer SELECT re-projects from the CTE. Synthetic level/root columns are excluded from any
	// star expansion (level only when not referenced) but kept where explicitly projected.
	var outerSelectExprs []*Expr
	if hasStar {
		var exceptCols []*Expr
		if !hasLevel {
			exceptCols = append(exceptCols, ColumnExpr("level", nil, nil, nil, nil, nil, true))
		}
		for _, name := range rootColNames {
			exceptCols = append(exceptCols, ColumnExpr(name, nil, nil, nil, nil, nil, true))
		}
		star := Star()
		if len(exceptCols) > 0 {
			star = New(KStar, "except_", exceptCols)
		}
		outerSelectExprs = []*Expr{star}
		for _, e := range baseSelectExprs {
			if !e.IsStar() {
				outerSelectExprs = append(outerSelectExprs, e)
			}
		}
	} else {
		outerSelectExprs = baseSelectExprs
	}
	outerQuery := SelectExpr(outerSelectExprs...).SelectFrom(MaybeParse(cteName, KFrom, "FROM", nil), true)
	if baseWhere != nil {
		outerQuery = outerQuery.QueryWhere([]*Expr{baseWhere.This()}, true, true)
	}

	// Attach the CTE, marking the WITH clause recursive.
	if baseWith != nil {
		outerQuery.Set("with_", baseWith)
	}
	outerQuery = outerQuery.QueryWith(
		MaybeParse(cteName, KTableAlias, "", nil),
		anchor.QueryUnion([]*Expr{innerQuery}, false, true),
		true, nil, true, false, nil,
	)

	for _, key := range expression.ArgKeys() {
		val := expression.Arg(key)
		if truthy(val) && !duckdbConnectByArgsToSkip.Has(key) {
			outerQuery.Set(key, val)
		}
	}

	// Strip stale source table qualifiers in one pass; CTEs are child scopes so
	// find_all_in_scope stays within the outer query only.
	for col := range FindAllInScope(outerQuery, KColumn) {
		col.Set("table", nil)
	}

	return outerQuery
}

// duckdbSeqSQL mirrors _seq_sql.
//
// Transpile Snowflake SEQ1/SEQ2/SEQ4/SEQ8 to DuckDB.
func duckdbSeqSQL(g *Generator, e *Expr, byteWidth int) string {
	// Warn if SEQ is in a restricted context (Select stops search at current scope)
	ancestor := e.FindAncestor(duckdbSeqRestricted...)
	if ancestor != nil && ((!ancestor.IsA(KOrder, KSelect)) ||
		(ancestor.IsA(KOrder) && ancestor.Parent().IsA(KWindow))) {
		g.unsupported("SEQ in restricted context is not supported - use CTE or subquery")
	}

	result := duckdbBuildSeqExpression(duckdbSeqBase.get().Copy(), byteWidth, e.Name() == "1")
	return g.sql(result)
}

// duckdbUnixToTimeSQL mirrors _unix_to_time_sql.
func duckdbUnixToTimeSQL(g *Generator, e *Expr) string {
	scale := e.ArgE("scale")
	timestamp := e.This()
	targetType := e.ArgE("target_type")

	// Check if we need NTZ (naive timestamp in UTC)
	isNtz := false
	if targetType != nil {
		t := targetType.Arg("this")
		isNtz = t == DT_TIMESTAMP || t == DT_TIMESTAMPNTZ
	}

	if scale != nil && scale.Equal(duckdbUnixToTimeMillis()) {
		// EPOCH_MS already returns TIMESTAMP (naive, UTC)
		return g.fn("EPOCH_MS", timestamp)
	}
	if scale != nil && scale.Equal(duckdbUnixToTimeMicros()) {
		// MAKE_TIMESTAMP already returns TIMESTAMP (naive, UTC)
		return g.fn("MAKE_TIMESTAMP", timestamp)
	}

	// Other scales: divide and use TO_TIMESTAMP
	if scale != nil && !scale.Equal(duckdbUnixToTimeSeconds()) {
		timestamp = New(KDiv, "this", timestamp, "expression", duckdbFunc("POW", 10, scale))
	}

	toTimestamp := New(KAnonymous, "this", "TO_TIMESTAMP", "expressions", []*Expr{timestamp})

	if isNtz {
		toTimestamp = New(KAtTimeZone, "this", toTimestamp, "zone", LiteralString("UTC"))
	}

	return g.sql(toTimestamp)
}

var duckdbWrappedJSONExtractExpressions = []Kind{KBinary, KBracket, KIn, KNot}

// duckdbArrowJSONExtractSQL mirrors _arrow_json_extract_sql.
func duckdbArrowJSONExtractSQL(g *Generator, e *Expr) string {
	arrowSQL := arrowJSONExtractSQL(g, e)
	if !e.SameParent() && e.Parent().IsA(duckdbWrappedJSONExtractExpressions...) {
		arrowSQL = g.wrap(arrowSQL)
	}
	return arrowSQL
}

// duckdbImplicitDatetimeCast mirrors _implicit_datetime_cast.
func duckdbImplicitDatetimeCast(arg *Expr, typ DType) *Expr {
	if arg.IsA(KLiteral) && arg.IsString() {
		ts := arg.Name()
		if typ == DT_DATE && strings.Contains(ts, ":") {
			if duckdbTimezonePattern.MatchString(ts) {
				typ = DT_TIMESTAMPTZ
			} else {
				typ = DT_TIMESTAMP
			}
		}

		arg = CastExpr(arg, typ, true, nil)
	}

	return arg
}

// duckdbBuildWeekTruncExpression mirrors _build_week_trunc_expression.
//
// Build DATE_TRUNC expression for week boundaries with custom start day.
// DuckDB's DATE_TRUNC('WEEK', ...) always returns Monday. To align to a different
// start day, we shift the date before truncating.
// Shift formula: Sunday (7) gets +1, others get (1 - start_dow).
func duckdbBuildWeekTruncExpression(dateExpr *Expr, startDow int, preserveStartDay bool) *Expr {
	shiftDays := 1 - startDow
	if startDow == 7 {
		shiftDays = 1
	}
	// exp.func("DATE_TRUNC", unit=exp.var("WEEK"), this=date_expr)
	truncated := New(KDateTrunc, "unit", VarExpr("WEEK"), "this", maybeParseExpr(dateExpr, true))

	if shiftDays == 0 {
		return truncated
	}

	shift := New(KInterval, "this", LiteralString(fmt.Sprint(shiftDays)), "unit", VarExpr("DAY"))
	shiftedDate := New(KDateAdd, "this", dateExpr, "expression", shift)
	truncated.Set("this", shiftedDate)

	if preserveStartDay {
		interval := New(KInterval, "this", LiteralString(fmt.Sprint(-shiftDays)), "unit", VarExpr("DAY"))
		return CastExpr(New(KDateAdd, "this", truncated, "expression", interval), DT_DATE, false, nil)
	}

	return truncated
}

// duckdbDateDiffSQL mirrors _date_diff_sql.
func duckdbDateDiffSQL(g *Generator, e *Expr) string {
	unit := e.ArgE("unit")

	if duckdbIsNanosecondUnit(unit) {
		return duckdbHandleNanosecondDiff(g, e.This(), e.Expression())
	}

	this := duckdbImplicitDatetimeCast(e.This(), DT_DATE)
	expr := duckdbImplicitDatetimeCast(e.Expression(), DT_DATE)

	// DuckDB's WEEK diff does not respect Monday crossing (week boundaries), it checks (end_day - start_day) / 7:
	//  SELECT DATE_DIFF('WEEK', CAST('2024-12-13' AS DATE), CAST('2024-12-17' AS DATE)) --> 0 (Monday crossed)
	//  SELECT DATE_DIFF('WEEK', CAST('2024-12-13' AS DATE), CAST('2024-12-20' AS DATE)) --> 1 (7 days difference)
	// Whereas for other units such as MONTH it does respect month boundaries:
	//  SELECT DATE_DIFF('MONTH', CAST('2024-11-30' AS DATE), CAST('2024-12-01' AS DATE)) --> 1 (Month crossed)
	datePartBoundary := e.ArgB("date_part_boundary")

	// Extract week start day; returns None if day is dynamic (column/placeholder)
	weekStart, ok := weekUnitToDow(unit)
	if datePartBoundary && ok && weekStart != 0 && this != nil && expr != nil {
		e.Set("unit", LiteralString("WEEK"))

		// Truncate both dates to week boundaries to respect input dialect semantics
		this = duckdbBuildWeekTruncExpression(this, weekStart, false)
		expr = duckdbBuildWeekTruncExpression(expr, weekStart, false)
	}

	return g.fn("DATE_DIFF", unitToStr(e, "DAY"), expr, this)
}

// duckdbGenerateDatetimeArraySQL mirrors _generate_datetime_array_sql.
func duckdbGenerateDatetimeArraySQL(g *Generator, e *Expr) string {
	isGenerateDateArray := e.IsA(KGenerateDateArray)

	typ := DT_TIMESTAMP
	if isGenerateDateArray {
		typ = DT_DATE
	}
	start := duckdbImplicitDatetimeCast(e.ArgE("start"), typ)
	end := duckdbImplicitDatetimeCast(e.ArgE("end"), typ)

	// BQ's GENERATE_DATE_ARRAY & GENERATE_TIMESTAMP_ARRAY are transformed to DuckDB'S GENERATE_SERIES
	genSeries := New(KGenerateSeries, "start", start, "end", end, "step", e.ArgE("step"))

	if isGenerateDateArray {
		// The GENERATE_SERIES result type is TIMESTAMP array, so to match BQ's semantics for
		// GENERATE_DATE_ARRAY we must cast it back to DATE array
		genSeries = CastExpr(genSeries, duckdbDataTypeFromStr("ARRAY<DATE>", nil), true, nil)
	}

	return g.sql(genSeries)
}

// duckdbJSONExtractValueArraySQL mirrors _json_extract_value_array_sql.
func duckdbJSONExtractValueArraySQL(g *Generator, e *Expr) string {
	jsonExtract := New(KJSONExtract, "this", e.This(), "expression", e.Expression())
	dataType := "ARRAY<JSON>"
	if e.IsA(KJSONValueArray) {
		dataType = "ARRAY<STRING>"
	}
	return g.sql(CastExpr(jsonExtract, duckdbDataTypeFromStr(dataType, nil), true, nil))
}

// duckdbCastToVarchar mirrors _cast_to_varchar.
func duckdbCastToVarchar(arg *Expr) *Expr {
	if arg != nil && arg.Type() != nil && !duckdbIsType(arg, append(DataType_TEXT_TYPES.Items(), DT_UNKNOWN)...) {
		return CastExpr(arg, DT_VARCHAR, true, nil)
	}
	return arg
}

// duckdbCastToBoolean mirrors _cast_to_boolean.
func duckdbCastToBoolean(arg *Expr) *Expr {
	if arg != nil && !duckdbIsType(arg, DT_BOOLEAN) {
		return CastExpr(arg, DT_BOOLEAN, true, nil)
	}
	return arg
}

// duckdbIsBinary mirrors _is_binary.
func duckdbIsBinary(arg *Expr) bool {
	return duckdbIsType(arg, DT_BINARY, DT_VARBINARY, DT_BLOB)
}

// duckdbGenWithCastToBlob mirrors _gen_with_cast_to_blob.
func duckdbGenWithCastToBlob(g *Generator, e *Expr, resultSQL string) string {
	if duckdbIsBinary(e) {
		blob := duckdbDataTypeFromStr("BLOB", MustDialect("duckdb"))
		resultSQL = g.sql(New(KCast, "this", resultSQL, "to", blob))
	}
	return resultSQL
}

// duckdbCastToBit mirrors _cast_to_bit.
func duckdbCastToBit(arg *Expr) *Expr {
	if !duckdbIsBinary(arg) {
		return arg
	}

	if arg.IsA(KHexString) {
		arg = New(KUnhex, "this", LiteralString(arg.ThisS()))
	}

	return CastExpr(arg, DT_BIT, true, nil)
}

// duckdbPrepareBinaryBitwiseArgs mirrors _prepare_binary_bitwise_args.
func duckdbPrepareBinaryBitwiseArgs(e *Expr) {
	if duckdbIsBinary(e.This()) {
		e.Set("this", duckdbCastToBit(e.This()))
	}
	if duckdbIsBinary(e.Expression()) {
		e.Set("expression", duckdbCastToBit(e.Expression()))
	}
}

// duckdbDayNavigationSQL mirrors _day_navigation_sql.
//
// Transpile Snowflake's NEXT_DAY / PREVIOUS_DAY to DuckDB using date arithmetic.
//
// Formulas:
// - NEXT_DAY: (target_dow - current_dow + 6) % 7 + 1
// - PREVIOUS_DAY: (current_dow - target_dow + 6) % 7 + 1.
func duckdbDayNavigationSQL(g *Generator, e *Expr) string {
	dateExpr := e.This()
	dayNameExpr := e.Expression()

	// Build ISODOW call for current day of week
	isodowCall := duckdbFunc("ISODOW", dateExpr)

	// Determine target day of week
	var targetDow *Expr
	if dayNameExpr.IsA(KLiteral) {
		// Literal day name: lookup target_dow directly
		dayNameStr := pyUpper(dayNameExpr.Name())
		found := false
		for _, kv := range duckdbWeekStartDayToDow {
			if strings.HasPrefix(kv.day, dayNameStr) {
				targetDow = LiteralInt(kv.dow)
				found = true
				break
			}
		}
		if !found {
			// Unrecognized day name, use fallback
			return g.functionFallbackSQL(e)
		}
	} else {
		// Non-literal day name: build CASE statement for runtime mapping
		upperDayName := New(KUpper, "this", dayNameExpr)
		ifs := make([]*Expr, 0, len(duckdbWeekStartDayToDow))
		for _, kv := range duckdbWeekStartDayToDow {
			ifs = append(ifs, New(
				KIf,
				"this", duckdbFunc("STARTS_WITH", upperDayName.Copy(), LiteralString(kv.day[:2])),
				"true", LiteralInt(kv.dow),
			))
		}
		targetDow = New(KCase, "ifs", ifs)
	}

	// Calculate days offset and apply interval based on direction
	var dateWithOffset *Expr
	if e.IsA(KNextDay) {
		// NEXT_DAY: (target_dow - current_dow + 6) % 7 + 1
		daysOffset := duckdbAdd(duckdbMod(ParenExpr(duckdbAdd(duckdbSub(targetDow, isodowCall), 6), false), 7), 1)
		dateWithOffset = duckdbAdd(dateExpr, New(KInterval, "this", daysOffset, "unit", VarExpr("DAY")))
	} else { // exp.PreviousDay
		// PREVIOUS_DAY: (current_dow - target_dow + 6) % 7 + 1
		daysOffset := duckdbAdd(duckdbMod(ParenExpr(duckdbAdd(duckdbSub(isodowCall, targetDow), 6), false), 7), 1)
		dateWithOffset = duckdbSub(dateExpr, New(KInterval, "this", daysOffset, "unit", VarExpr("DAY")))
	}

	// Build final: CAST(date_with_offset AS DATE)
	return g.sql(CastExpr(dateWithOffset, DT_DATE, true, nil))
}

// duckdbAnyvalueSQL mirrors _anyvalue_sql.
func duckdbAnyvalueSQL(g *Generator, e *Expr) string {
	// Transform ANY_VALUE(expr HAVING MAX/MIN having_expr) to ARG_MAX_NULL/ARG_MIN_NULL
	having := e.This()
	if having.IsA(KHavingMax) {
		funcName := "ARG_MIN_NULL"
		if having.ArgB("max") {
			funcName = "ARG_MAX_NULL"
		}
		return g.fn(funcName, having.Arg("this"), having.Arg("expression"))
	}
	return g.functionFallbackSQL(e)
}

// duckdbBitwiseAggSQL mirrors _bitwise_agg_sql.
//
// DuckDB's bitwise aggregate functions only accept integer types. For other types:
// - DECIMAL/STRING: Use CAST(arg AS INT) to convert directly, will round to nearest int
// - FLOAT/DOUBLE: Use ROUND(arg)::INT to round to nearest integer, required due to float precision loss.
func duckdbBitwiseAggSQL(g *Generator, e *Expr) string {
	var funcName string
	if e.IsA(KBitwiseOrAgg) {
		funcName = "BIT_OR"
	} else if e.IsA(KBitwiseAndAgg) {
		funcName = "BIT_AND"
	} else { // exp.BitwiseXorAgg
		funcName = "BIT_XOR"
	}

	arg := e.This()

	if arg.Type() == nil {
		arg = annotateTypes(arg, g.d)
	}

	if duckdbIsType(arg, duckdbTypes([]DTypeSet{DataType_REAL_TYPES, DataType_TEXT_TYPES})...) {
		if duckdbIsType(arg, DataType_FLOAT_TYPES.Items()...) {
			// float types need to be rounded first due to precision loss
			arg = duckdbFunc("ROUND", arg)
		}

		arg = CastExpr(arg, DT_INT, true, nil)
	}

	return g.fn(funcName, arg)
}

// duckdbLiteralSQLWithWSChr mirrors _literal_sql_with_ws_chr.
func duckdbLiteralSQLWithWSChr(g *Generator, literal string) string {
	// DuckDB does not support \uXXXX escapes, so we must use CHR() instead of replacing them directly
	has := false
	for _, ch := range literal {
		if _, ok := duckdbWSControlCharsToDuck[ch]; ok {
			has = true
			break
		}
	}
	if !has {
		return g.sql(LiteralString(literal))
	}

	var sqlSegments []string
	runes := []rune(literal)
	for i := 0; i < len(runes); {
		_, isWSControl := duckdbWSControlCharsToDuck[runes[i]]
		j := i
		for j < len(runes) {
			_, ok := duckdbWSControlCharsToDuck[runes[j]]
			if ok != isWSControl {
				break
			}
			j++
		}
		group := runes[i:j]
		if isWSControl {
			for _, ch := range group {
				duckdbCharCode := duckdbWSControlCharsToDuck[ch]
				sqlSegments = append(sqlSegments, g.fn("CHR", LiteralNumber(fmt.Sprint(duckdbCharCode))))
			}
		} else {
			sqlSegments = append(sqlSegments, g.sql(LiteralString(string(group))))
		}
		i = j
	}

	sql := strings.Join(sqlSegments, " || ")
	if len(sqlSegments) == 1 {
		return sql
	}
	return "(" + sql + ")"
}

// duckdbEscapeRegexMetachars mirrors _escape_regex_metachars.
//
// Escapes regex metacharacters \ - ^ [ ] for use in character classes regex expressions.
// Literal strings are escaped at transpile time, expressions handled with REPLACE() calls.
func duckdbEscapeRegexMetachars(g *Generator, delimiters *Expr, delimitersSQL string) string {
	if delimiters == nil {
		return delimitersSQL
	}

	if delimiters.IsString() {
		literalValue := delimiters.ThisS()
		var b strings.Builder
		for _, ch := range literalValue {
			if r, ok := duckdbRegexEscapeReplacement(string(ch)); ok {
				b.WriteString(r)
			} else {
				b.WriteRune(ch)
			}
		}
		return duckdbLiteralSQLWithWSChr(g, b.String())
	}

	escapedSQL := delimitersSQL
	for _, kv := range duckdbRegexEscapeReplacements {
		escapedSQL = g.fn(
			"REPLACE",
			escapedSQL,
			g.sql(LiteralString(kv[0])),
			g.sql(LiteralString(kv[1])),
		)
	}

	return escapedSQL
}

// duckdbBuildCapitalizationSQL mirrors _build_capitalization_sql.
func duckdbBuildCapitalizationSQL(g *Generator, valueToSplit string, delimitersSQL string) string {
	// empty string delimiter --> treat value as one word, no need to split
	if delimitersSQL == "''" {
		return "UPPER(LEFT(" + valueToSplit + ", 1)) || LOWER(SUBSTRING(" + valueToSplit + ", 2))"
	}

	delimRegexSQL := "CONCAT('[', " + delimitersSQL + ", ']')"
	splitRegexSQL := "CONCAT('([', " + delimitersSQL + ", ']+|[^', " + delimitersSQL + ", ']+)')"

	// REGEXP_EXTRACT_ALL produces a list of string segments, alternating between delimiter and non-delimiter segments.
	// We do not know whether the first segment is a delimiter or not, so we check the first character of the string
	// with REGEXP_MATCHES. If the first char is a delimiter, we capitalize even list indexes, otherwise capitalize odd.
	c := duckdbWhen(
		duckdbCase(),
		"REGEXP_MATCHES(LEFT("+valueToSplit+", 1), "+delimRegexSQL+")",
		g.fn(
			"LIST_TRANSFORM",
			g.fn("REGEXP_EXTRACT_ALL", valueToSplit, splitRegexSQL),
			"(seg, idx) -> CASE WHEN idx % 2 = 0 THEN UPPER(LEFT(seg, 1)) || LOWER(SUBSTRING(seg, 2)) ELSE seg END",
		),
		true,
	)
	c = duckdbElse(
		c,
		g.fn(
			"LIST_TRANSFORM",
			g.fn("REGEXP_EXTRACT_ALL", valueToSplit, splitRegexSQL),
			"(seg, idx) -> CASE WHEN idx % 2 = 1 THEN UPPER(LEFT(seg, 1)) || LOWER(SUBSTRING(seg, 2)) ELSE seg END",
		),
		true,
	)
	return g.fn("ARRAY_TO_STRING", c, "''")
}

// duckdbInitcapSQL mirrors _initcap_sql.
func duckdbInitcapSQL(g *Generator, e *Expr) string {
	thisSQL := g.sqlKey(e, "this")
	delimiters := e.ArgE("expression")
	if delimiters == nil {
		// fallback for manually created exp.Initcap w/o delimiters arg
		delimiters = LiteralString(g.d.S.INITCAP_DEFAULT_DELIMITER_CHARS)
	}
	delimitersSQL := g.sql(delimiters)

	escapedDelimitersSQL := duckdbEscapeRegexMetachars(g, delimiters, delimitersSQL)

	return duckdbBuildCapitalizationSQL(g, thisSQL, escapedDelimitersSQL)
}

// duckdbBoolxorAggSQL mirrors _boolxor_agg_sql.
//
// Snowflake's `BOOLXOR_AGG(col)` returns TRUE if exactly one input in `col` is TRUE, FALSE otherwise;
// Since DuckDB does not have a mapping function, we mimic the behavior by generating `COUNT_IF(col) = 1`.
func duckdbBoolxorAggSQL(g *Generator, e *Expr) string {
	return g.sql(New(
		KEQ,
		"this", New(KCountIf, "this", duckdbCastToBoolean(e.This())),
		"expression", LiteralInt(1),
	))
}

// duckdbBitshiftSQL mirrors _bitshift_sql.
//
// Transform bitshift expressions for DuckDB by injecting BIT/INT128 casts.
func duckdbBitshiftSQL(g *Generator, e *Expr) string {
	operator := ">>"
	if e.IsA(KBitwiseLeftShift) {
		operator = "<<"
	}
	resultIsBlob := false
	this := e.This()

	if duckdbIsBinary(this) {
		resultIsBlob = true
		e.Set("this", CastExpr(this, DT_BIT, true, nil))
	} else if e.ArgB("requires_int128") {
		this.Replace(CastExpr(this, DT_INT128, true, nil))
	}

	resultSQL := g.binary(e, operator)

	// Wrap in parentheses if parent is a bitwise operator to "fix" DuckDB precedence issue
	// DuckDB parses: a << b | c << d  as  (a << b | c) << d
	if e.Parent().IsA(KBinary) {
		resultSQL = g.sql(New(KParen, "this", resultSQL))
	}

	if resultIsBlob {
		resultSQL = g.sql(New(KCast, "this", resultSQL, "to", duckdbDataTypeFromStr("BLOB", MustDialect("duckdb"))))
	}

	return resultSQL
}

// duckdbScaleRoundingSQL mirrors _scale_rounding_sql. ok=false mirrors a None result.
//
// DuckDB doesn't support the scale parameter for certain functions (e.g., FLOOR, CEIL),
// so we transform: FUNC(x, n) to ROUND(FUNC(x * 10^n) / 10^n, n).
func duckdbScaleRoundingSQL(g *Generator, e *Expr, roundingFunc Kind) (string, bool) {
	decimals := e.ArgE("decimals")

	if decimals == nil || e.ArgE("to") != nil {
		return "", false
	}

	this := e.This()
	if this.IsA(KBinary) {
		this = New(KParen, "this", this)
	}

	nInt := decimals
	if !(decimals.IsInt() || duckdbIsType(decimals, DataType_INTEGER_TYPES.Items()...)) {
		nInt = CastExpr(decimals, DT_INT, true, nil)
	}

	pow := New(KPow, "this", LiteralNumber("10"), "expression", nInt)
	rounded := New(roundingFunc, "this", New(KMul, "this", this, "expression", pow))
	result := New(KDiv, "this", rounded, "expression", pow.Copy())

	return duckdbRoundSQL(g, New(KRound, "this", result, "decimals", decimals, "casts_non_integer_decimals", true)), true
}

// duckdbCeilFloor mirrors _ceil_floor.
func duckdbCeilFloor(g *Generator, e *Expr) string {
	if scaledSQL, ok := duckdbScaleRoundingSQL(g, e, e.Kind()); ok {
		return scaledSQL
	}
	return g.ceilFloor(e)
}

// duckdbRegrValSQL mirrors _regr_val_sql.
//
// REGR_VALX(y, x) returns NULL if y is NULL; otherwise returns x.
// REGR_VALY(y, x) returns NULL if x is NULL; otherwise returns y.
func duckdbRegrValSQL(g *Generator, e *Expr) string {
	y := e.This()
	x := e.Expression()

	// Determine which argument to check for NULL and which to return based on expression type
	var checkForNull, returnValue *Expr
	var returnValueAttr string
	if e.IsA(KRegrValx) {
		// REGR_VALX: check y for NULL, return x
		checkForNull = y
		returnValue = x
		returnValueAttr = "expression"
	} else {
		// REGR_VALY: check x for NULL, return y
		checkForNull = x
		returnValue = y
		returnValueAttr = "this"
	}

	// Get the type from the return argument
	resultType := returnValue.Type()

	// If no type info, annotate the expression to infer types
	if resultType == nil || resultType.DTypeOf() == DT_UNKNOWN {
		func() {
			defer func() { _ = recover() }()
			annotated := annotateTypes(e.Copy(), g.d)
			resultType = annotated.ArgE(returnValueAttr).Type()
		}()
	}

	// Default to DOUBLE for regression functions if type still unknown
	if resultType == nil || resultType.DTypeOf() == DT_UNKNOWN {
		resultType = duckdbNewTypeExpr(DT_DOUBLE)
	}

	// Cast NULL to the same type as return_value to avoid DuckDB type inference issues
	typedNull := New(KCast, "this", Null(), "to", resultType)

	return g.sql(New(
		KIf,
		"this", New(KIs, "this", checkForNull.Copy(), "expression", Null()),
		"true", typedNull,
		"false", returnValue.Copy(),
	))
}

// duckdbMaybeCorrNullToFalse mirrors _maybe_corr_null_to_false.
func duckdbMaybeCorrNullToFalse(e *Expr) *Expr {
	corr := e
	for corr.IsA(KWindow, KFilter) {
		corr = corr.This()
	}

	if !corr.IsA(KCorr) || !corr.ArgB("null_on_zero_variance") {
		return nil
	}

	corr.Set("null_on_zero_variance", false)
	return e
}

// duckdbDateFromPartsSQL mirrors _date_from_parts_sql.
//
// Snowflake's DATE_FROM_PARTS allows out-of-range values for the month and day input.
// DuckDB's MAKE_DATE does not support out-of-range values, but DuckDB's INTERVAL type does.
func duckdbDateFromPartsSQL(g *Generator, e *Expr) string {
	yearExpr := e.ArgE("year")
	monthExpr := e.ArgE("month")
	dayExpr := e.ArgE("day")

	if e.ArgB("allow_overflow") {
		baseDate := duckdbFunc("MAKE_DATE", yearExpr, LiteralInt(1), LiteralInt(1))

		if monthExpr != nil {
			baseDate = duckdbAdd(baseDate, New(KInterval, "this", duckdbSub(monthExpr, 1), "unit", VarExpr("MONTH")))
		}

		if dayExpr != nil {
			baseDate = duckdbAdd(baseDate, New(KInterval, "this", duckdbSub(dayExpr, 1), "unit", VarExpr("DAY")))
		}

		return g.sql(CastExpr(baseDate, DT_DATE, true, nil))
	}

	return g.fn("MAKE_DATE", yearExpr, monthExpr, dayExpr)
}

// duckdbRoundArg mirrors _round_arg.
func duckdbRoundArg(arg *Expr, roundInput bool) *Expr {
	if roundInput {
		return duckdbFunc("ROUND", arg, LiteralInt(0))
	}
	return arg
}

// duckdbBoolnotSQL mirrors _boolnot_sql.
func duckdbBoolnotSQL(g *Generator, e *Expr) string {
	arg := duckdbRoundArg(e.This(), e.ArgB("round_input"))
	return g.sql(NotExpr(ParenExpr(arg, true), true))
}

// duckdbBoolandSQL mirrors _booland_sql.
func duckdbBoolandSQL(g *Generator, e *Expr) string {
	roundInput := e.ArgB("round_input")
	left := duckdbRoundArg(e.This(), roundInput)
	right := duckdbRoundArg(e.Expression(), roundInput)
	return g.sql(ParenExpr(AndExprOpts([]*Expr{ParenExpr(left, true), ParenExpr(right, true)}, true, false), true))
}

// duckdbBoolorSQL mirrors _boolor_sql.
func duckdbBoolorSQL(g *Generator, e *Expr) string {
	roundInput := e.ArgB("round_input")
	left := duckdbRoundArg(e.This(), roundInput)
	right := duckdbRoundArg(e.Expression(), roundInput)
	return g.sql(ParenExpr(OrExprOpts([]*Expr{ParenExpr(left, true), ParenExpr(right, true)}, true, false), true))
}

// duckdbXorSQL mirrors _xor_sql.
func duckdbXorSQL(g *Generator, e *Expr) string {
	roundInput := e.ArgB("round_input")
	left := duckdbRoundArg(e.This(), roundInput)
	right := duckdbRoundArg(e.Expression(), roundInput)
	return g.sql(OrExprOpts([]*Expr{
		ParenExpr(AndExprOpts([]*Expr{left.Copy(), ParenExpr(right.ExprNot(true), true)}, true, false), true),
		ParenExpr(AndExprOpts([]*Expr{ParenExpr(left.ExprNot(true), true), right.Copy()}, true, false), true),
	}, true, false))
}

// duckdbExplodeToUnnestSQL mirrors _explode_to_unnest_sql.
// Handle LATERAL VIEW EXPLODE/INLINE conversion to UNNEST for DuckDB.
func duckdbExplodeToUnnestSQL(g *Generator, e *Expr) string {
	explode := e.This()

	if explode.IsA(KInline) {
		// For INLINE, create CROSS JOIN LATERAL (SELECT UNNEST(..., max_depth => 2))
		// Build the UNNEST call with DuckDB-style named parameter
		unnestExpr := New(KUnnest, "expressions", []*Expr{
			explode.This(),
			New(KKwarg, "this", VarExpr("max_depth"), "expression", LiteralInt(2)),
		})
		selectExpr := New(KSelect, "expressions", []*Expr{unnestExpr}).QuerySubquery(nil, true)

		aliasExpr := e.ArgE("alias")
		if aliasExpr != nil && !aliasExpr.ArgB("this") {
			// we need to provide a table name if not present
			idx := "None"
			if e.Index() >= 0 {
				idx = fmt.Sprint(e.Index())
			}
			aliasExpr.Set("this", ToIdentifier("_u_"+idx, nil))
		}

		transformedLateralExpr := New(KLateral, "this", selectExpr, "alias", aliasExpr)
		crossJoinLateralExpr := New(KJoin, "this", transformedLateralExpr, "kind", "CROSS")

		return g.sql(crossJoinLateralExpr)
	}

	// For other cases, use the standard conversion
	return explodeToUnnestSQL(g, e)
}

// duckdbShaSQL mirrors _sha_sql.
func duckdbShaSQL(g *Generator, e *Expr, hashFunc string, isBinary bool) string {
	arg := e.This()

	// For SHA2 variants, check digest length (DuckDB only supports SHA256)
	if hashFunc == "SHA256" {
		length := e.Text("length")
		if length == "" {
			length = "256"
		}
		if length != "256" {
			g.unsupported("DuckDB only supports SHA256 hashing algorithm.")
		}
	}

	// Cast if type is incompatible with DuckDB
	if arg.Type() != nil &&
		arg.Type().DTypeOf() != DT_UNKNOWN &&
		!duckdbIsType(arg, DataType_TEXT_TYPES.Items()...) &&
		!duckdbIsBinary(arg) {
		arg = CastExpr(arg, DT_VARCHAR, true, nil)
	}

	result := g.fn(hashFunc, arg)
	if isBinary {
		return g.fn("UNHEX", result)
	}
	return result
}
