package sqlengine

import (
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

// These constants duplicate the Hive dialect settings TIME_FORMAT, DATE_FORMAT and
// DATEINT_FORMAT (dialect_settings.go) and must be kept in sync with them.
const (
	HIVE_TIME_FORMAT    = "'yyyy-MM-dd HH:mm:ss'"
	HIVE_DATE_FORMAT    = "'yyyy-MM-dd'"
	HIVE_DATEINT_FORMAT = "'yyyyMMdd'"
)

// HIVE_NON_PADDED_TIME_FORMATS are the default formats as rendered by the lenient rewrite.
var HIVE_NON_PADDED_TIME_FORMATS = []string{"'yyyy-M-d HH:mm:ss'", "'yyyy-M-d'"}

var hivePARSE_TIME_EXPRESSIONS = []Kind{KStrToTime, KStrToDate, KStrToUnix, KTsOrDsToDate}

var hiveCANONICAL_TIME_FORMAT = regexp.MustCompile(`%(?:mstrict|dstrict|[-:].|.)`)

var hiveLAX_TO_NON_PADDED_FORMATS = map[string]string{"%m": "%-m", "%d": "%-d"}

// hiveLenientParseFormat changes the lax %m/%d in a canonical format to the non-padded %-m/%-d,
// which java.time parses with or without a leading zero. This is only safe for delimited
// specifiers, because adjacent fields parse greedily. A %m/%d is changed only when its neighbors
// don't touch a digit run, i.e., neither side is another specifier or a literal digit.
func hiveLenientParseFormat(fmt_ string) string {
	parts := hiveCANONICAL_TIME_FORMAT.Split(fmt_, -1)
	formats := hiveCANONICAL_TIME_FORMAT.FindAllString(fmt_, -1)

	lastIsDigit := func(s string) bool {
		r := []rune(s)
		return unicode.IsDigit(r[len(r)-1])
	}
	firstIsDigit := func(s string) bool {
		r := []rune(s)
		return unicode.IsDigit(r[0])
	}

	for i, f := range formats {
		if repl, ok := hiveLAX_TO_NON_PADDED_FORMATS[f]; ok {
			left, right := parts[i], parts[i+1]
			leftAdjacent := (left == "" && i > 0) || (left != "" && lastIsDigit(left))
			rightAdjacent := (right == "" && i < len(formats)-1) || (right != "" && firstIsDigit(right))
			if !leftAdjacent && !rightAdjacent {
				formats[i] = repl
			}
		}
	}

	var b strings.Builder
	formats = append(formats, "")
	for i := 0; i < len(parts) && i < len(formats); i++ {
		b.WriteString(parts[i])
		b.WriteString(formats[i])
	}
	return b.String()
}

type hiveDeltaInterval struct {
	fn         string
	multiplier int
}

var hiveDATE_DELTA_INTERVAL = map[string]hiveDeltaInterval{
	"YEAR":    {"ADD_MONTHS", 12},
	"MONTH":   {"ADD_MONTHS", 1},
	"QUARTER": {"ADD_MONTHS", 3},
	"WEEK":    {"DATE_ADD", 7},
	"DAY":     {"DATE_ADD", 1},
}

var hiveTIME_DIFF_FACTOR = map[string]string{
	"MILLISECOND": " * 1000",
	"SECOND":      "",
	"MINUTE":      " / 60",
	"HOUR":        " / 3600",
}

var hiveDIFF_MONTH_SWITCH = newStrSet("YEAR", "QUARTER", "MONTH")

// hiveNilIfEmpty turns an empty string result into nil so g.fn omits the argument.
func hiveNilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// hiveMulNumberLiteral returns a number literal for text times multiplier: exact for integers,
// keeping the operand's exponent for decimals. Panics with a ValueError if text is not a number.
func hiveMulNumberLiteral(text string, multiplier int) *Expr {
	clean := strings.ReplaceAll(strings.TrimSpace(text), "_", "")
	if i, ok := new(big.Int).SetString(clean, 10); ok {
		return LiteralNumber(new(big.Int).Mul(i, big.NewInt(int64(multiplier))).String())
	}
	// Decimal text: the product keeps the exponent of the operand (2.50 * 3 is 7.50).
	if d, ok := hivePyDecimal(clean); ok {
		return LiteralNumber(d.mulInt(multiplier).String())
	}
	panic(&ValueError{Msg: "[<class 'decimal.ConversionSyntax'>]"})
}

type hiveDecimal struct {
	neg      bool
	coef     *big.Int
	exponent int
}

func hivePyDecimal(s string) (hiveDecimal, bool) {
	var d hiveDecimal
	if s == "" {
		return d, false
	}
	if s[0] == '+' || s[0] == '-' {
		d.neg = s[0] == '-'
		s = s[1:]
	}
	exp := 0
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e, err := strconv.Atoi(s[i+1:])
		if err != nil {
			return d, false
		}
		exp = e
		s = s[:i]
	}
	intPart, fracPart := s, ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		intPart, fracPart = s[:i], s[i+1:]
	}
	digits := intPart + fracPart
	if digits == "" {
		return d, false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return d, false
		}
	}
	d.coef, _ = new(big.Int).SetString(digits, 10)
	d.exponent = exp - len(fracPart)
	return d, true
}

func (d hiveDecimal) mulInt(m int) hiveDecimal {
	out := hiveDecimal{exponent: d.exponent}
	mm := m
	neg := d.neg
	if mm < 0 {
		mm = -mm
		neg = !neg
	}
	out.coef = new(big.Int).Mul(d.coef, big.NewInt(int64(mm)))
	out.neg = neg
	return out
}

// String formats d with the General Decimal Arithmetic to-scientific-string rules: plain notation
// when the exponent is not positive and the adjusted exponent is at least -6, else scientific
// notation such as 1.2E+3.
func (d hiveDecimal) String() string {
	digits := d.coef.String()
	sign := ""
	if d.neg {
		sign = "-"
	}
	leftdigits := d.exponent + len(digits)
	var intPart, fracPart, expPart string
	if d.exponent <= 0 && leftdigits > -6 {
		dotplace := leftdigits
		if dotplace <= 0 {
			intPart = "0"
			fracPart = "." + strings.Repeat("0", -dotplace) + digits
		} else if dotplace >= len(digits) {
			intPart = digits + strings.Repeat("0", dotplace-len(digits))
		} else {
			intPart = digits[:dotplace]
			fracPart = "." + digits[dotplace:]
		}
	} else {
		dotplace := 1
		if dotplace >= len(digits) {
			intPart = digits
		} else {
			intPart = digits[:dotplace]
			fracPart = "." + digits[dotplace:]
		}
		e := leftdigits - dotplace
		if e != 0 {
			expPart = fmt.Sprintf("E%+d", e)
		}
	}
	return sign + intPart + fracPart + expPart
}

// hiveAddDateSQL renders DateAdd, DateSub and TsOrDsAdd as DATE_ADD or ADD_MONTHS, scaling the
// increment by the unit's multiplier (negated for DateSub). A unitless TsOrDsAdd is a plain
// DATE_ADD.
func hiveAddDateSQL(g *Generator, e *Expr) string {
	if e.IsA(KTsOrDsAdd) && !e.ArgB("unit") {
		return g.fn("DATE_ADD", e.Arg("this"), e.Arg("expression"))
	}

	unit := pyUpper(e.Text("unit"))
	delta, ok := hiveDATE_DELTA_INTERVAL[unit]
	if !ok {
		delta = hiveDeltaInterval{"DATE_ADD", 1}
	}
	fn, multiplier := delta.fn, delta.multiplier

	if e.IsA(KDateSub) {
		multiplier *= -1
	}

	increment := e.Expression()
	if increment.IsA(KLiteral) {
		if increment.IsNumber() {
			increment = hiveMulNumberLiteral(increment.ThisS(), multiplier)
		} else {
			v, err := strconv.Atoi(strings.ReplaceAll(strings.TrimSpace(increment.Name()), "_", ""))
			if err != nil {
				panic(&ValueError{Msg: fmt.Sprintf("invalid literal for int() with base 10: '%s'", increment.Name())})
			}
			increment = LiteralInt(v * multiplier)
		}
	} else if multiplier != 1 {
		increment = dhBinop(KMul, increment, LiteralInt(multiplier))
	}

	return g.fn(fn, e.Arg("this"), increment)
}

// hiveDateDiffSQL renders DateDiff and TsOrDsDiff: sub-day units as a scaled UNIX_TIMESTAMP
// difference, YEAR/QUARTER/MONTH via MONTHS_BETWEEN and other units via DATEDIFF.
func hiveDateDiffSQL(g *Generator, e *Expr) string {
	unit := pyUpper(e.Text("unit"))

	if factor, ok := hiveTIME_DIFF_FACTOR[unit]; ok {
		left := g.sqlKey(e, "this")
		right := g.sqlKey(e, "expression")
		secDiff := "UNIX_TIMESTAMP(" + left + ") - UNIX_TIMESTAMP(" + right + ")"
		if factor != "" {
			return "(" + secDiff + ")" + factor
		}
		return secDiff
	}

	monthsBetween := hiveDIFF_MONTH_SWITCH.Has(unit)
	sqlFunc := "DATEDIFF"
	if monthsBetween {
		sqlFunc = "MONTHS_BETWEEN"
	}
	multiplier := 1
	if delta, ok := hiveDATE_DELTA_INTERVAL[unit]; ok {
		multiplier = delta.multiplier
	}
	multiplierSQL := ""
	if multiplier > 1 {
		multiplierSQL = " / " + itoa(multiplier)
	}
	diffSQL := sqlFunc + "(" + g.formatArgs(", ", e.This(), e.Expression()) + ")"

	if monthsBetween || multiplierSQL != "" {
		// MONTHS_BETWEEN returns a float, so we need to truncate the fractional part.
		// For the same reason, we want to truncate if there's a divisor present.
		diffSQL = "CAST(" + diffSQL + multiplierSQL + " AS INT)"
	}

	return diffSQL
}

func hiveArraySortSQL(g *Generator, e *Expr) string {
	if e.ArgB("expression") {
		g.unsupported("Hive's SORT_ARRAY does not support a comparator.")
	}
	return g.fn("SORT_ARRAY", e.Arg("this"))
}

func hiveStrToUnixSQL(g *Generator, e *Expr) string {
	return g.fn("UNIX_TIMESTAMP", e.Arg("this"), timeFormat("hive")(g, e))
}

func hiveUnixToTimeSQL(g *Generator, e *Expr) string {
	timestamp := g.sqlKey(e, "this")
	scale := e.ArgE("scale")
	if scale == nil || scale.Equal(LiteralInt(0)) {
		return renameFunc("FROM_UNIXTIME")(g, e)
	}

	return "FROM_UNIXTIME(" + timestamp + " / POW(10, " + exprSQL(scale) + "))"
}

// hiveIsCastTimeFormat reports whether a plain CAST subsumes the expression's parse format: the
// default time or date format, padded or (for a lax %m/%d) non-padded.
func hiveIsCastTimeFormat(g *Generator, e *Expr, timeFormat string) bool {
	if timeFormat == HIVE_TIME_FORMAT || timeFormat == HIVE_DATE_FORMAT {
		return true
	}

	if timeFormat == HIVE_NON_PADDED_TIME_FORMATS[0] || timeFormat == HIVE_NON_PADDED_TIME_FORMATS[1] {
		// The base render skips the lenient rewrite: a lax %m/%d pads back to MM/dd, an explicit %-m/%-d doesn't
		paddedFormat := g.baseFormatTime(e, nil, nil)
		return paddedFormat == HIVE_TIME_FORMAT || paddedFormat == HIVE_DATE_FORMAT
	}

	return false
}

func hiveStrToDateSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	timeFormat := g.formatTime(e, nil, nil)
	if timeFormat != "" && !hiveIsCastTimeFormat(g, e, timeFormat) {
		this = "FROM_UNIXTIME(UNIX_TIMESTAMP(" + this + ", " + timeFormat + "))"
	}
	return "CAST(" + this + " AS DATE)"
}

func hiveStrToTimeSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	timeFormat := g.formatTime(e, nil, nil)
	if timeFormat != "" && !hiveIsCastTimeFormat(g, e, timeFormat) {
		this = "FROM_UNIXTIME(UNIX_TIMESTAMP(" + this + ", " + timeFormat + "))"
	}
	return "CAST(" + this + " AS TIMESTAMP)"
}

func hiveToDateSQL(g *Generator, e *Expr) string {
	timeFormat := g.formatTime(e, nil, nil)
	if timeFormat != "" && !hiveIsCastTimeFormat(g, e, timeFormat) {
		return g.fn("TO_DATE", e.Arg("this"), timeFormat)
	}

	if e.Parent().IsA(g.s.TS_OR_DS_EXPRESSIONS...) {
		return g.sqlKey(e, "this")
	}

	return g.fn("TO_DATE", e.Arg("this"))
}

func customizeHiveGenerator(d *Dialect) {
	T := d.G.TRANSFORMS

	T[KProperty] = propertySQL
	T[KAnyValue] = renameFunc("FIRST")
	T[KApproxDistinct] = approxCountDistinctSQL
	T[KArgMax] = argMaxOrMinNoCount("MAX_BY")
	T[KArgMin] = argMaxOrMinNoCount("MIN_BY")
	T[KArray] = transformPreprocess([]func(*Expr) *Expr{transformInheritStructFieldNames}, nil)
	T[KArrayConcat] = renameFunc("CONCAT")
	T[KArrayToString] = func(g *Generator, e *Expr) string { return g.fn("CONCAT_WS", e.Arg("expression"), e.Arg("this")) }
	T[KArraySort] = hiveArraySortSQL
	T[KWith] = noRecursiveCteSQL
	T[KDateAdd] = hiveAddDateSQL
	T[KDateDiff] = hiveDateDiffSQL
	T[KDateStrToDate] = datestrtodateSQL
	T[KDateSub] = hiveAddDateSQL
	T[KDateToDi] = func(g *Generator, e *Expr) string {
		return "CAST(DATE_FORMAT(" + g.sqlKey(e, "this") + ", " + HIVE_DATEINT_FORMAT + ") AS INT)"
	}
	T[KDiToDate] = func(g *Generator, e *Expr) string {
		return "TO_DATE(CAST(" + g.sqlKey(e, "this") + " AS STRING), " + HIVE_DATEINT_FORMAT + ")"
	}
	T[KStorageHandlerProperty] = func(g *Generator, e *Expr) string { return "STORED BY " + g.sqlKey(e, "this") }
	T[KFromBase64] = renameFunc("UNBASE64")
	T[KGenerateSeries] = sequenceSQL
	T[KGenerateDateArray] = sequenceSQL
	T[KIf] = ifSQL("IF", nil)
	T[KILike] = noIlikeSQL
	T[KIntDiv] = func(g *Generator, e *Expr) string { return g.binary(e, "DIV") }
	T[KIsNan] = renameFunc("ISNAN")
	T[KJSONExtract] = func(g *Generator, e *Expr) string { return g.fn("GET_JSON_OBJECT", e.Arg("this"), e.Arg("expression")) }
	T[KJSONExtractScalar] = func(g *Generator, e *Expr) string {
		return g.fn("GET_JSON_OBJECT", e.Arg("this"), e.Arg("expression"))
	}
	T[KJSONFormat] = renameFunc("TO_JSON")
	T[KLeft] = leftToSubstringSQL
	T[KMap] = func(g *Generator, e *Expr) string { return varMapSQL(g, e, "MAP") }
	T[KMax] = maxOrGreatest
	T[KMD5Digest] = func(g *Generator, e *Expr) string { return g.fn("UNHEX", g.fn("MD5", e.This())) }
	T[KMin] = minOrLeast
	T[KMonthsBetween] = func(g *Generator, e *Expr) string { return g.fn("MONTHS_BETWEEN", e.Arg("this"), e.Arg("expression")) }
	T[KNotNullColumnConstraint] = func(g *Generator, e *Expr) string {
		if e.ArgB("allow_null") {
			return ""
		}
		return "NOT NULL"
	}
	T[KVarMap] = func(g *Generator, e *Expr) string { return varMapSQL(g, e, "MAP") }
	T[KCreate] = transformPreprocess([]func(*Expr) *Expr{
		transformRemoveUniqueConstraints,
		transformCtasWithTmpTablesToCreateTmpView,
		transformMoveSchemaColumnsToPartitionedBy,
	}, nil)
	T[KQuantile] = renameFunc("PERCENTILE")
	T[KApproxQuantile] = renameFunc("PERCENTILE_APPROX")
	T[KRegexpExtract] = regexpExtractSQL
	T[KRegexpExtractAll] = regexpExtractSQL
	T[KRegexpReplace] = regexpReplaceSQL
	T[KRegexpLike] = func(g *Generator, e *Expr) string { return g.binary(e, "RLIKE") }
	T[KRegexpSplit] = renameFunc("SPLIT")
	T[KRight] = rightToSubstringSQL
	T[KSchemaCommentProperty] = func(g *Generator, e *Expr) string { return g.nakedProperty(e) }
	T[KArrayUniqueAgg] = renameFunc("COLLECT_SET")
	T[KSplit] = func(g *Generator, e *Expr) string {
		return g.fn("SPLIT", e.Arg("this"), g.fn("CONCAT", "'\\\\Q'", e.Expression(), "'\\\\E'"))
	}
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		transformEliminateQualify,
		transformEliminateDistinctOn,
		func(e *Expr) *Expr { return transformUnnestToExplodeFull(e, false) },
		transformAnyToExists,
	}, nil)
	T[KStrPosition] = func(g *Generator, e *Expr) string {
		return strpositionSQL(g, e, "LOCATE", true, false, true)
	}
	T[KStrToDate] = hiveStrToDateSQL
	T[KStrToTime] = hiveStrToTimeSQL
	T[KStrToUnix] = hiveStrToUnixSQL
	T[KStructExtract] = structExtractSQL
	T[KStarMap] = renameFunc("MAP")
	T[KTable] = transformPreprocess([]func(*Expr) *Expr{transformUnnestGenerateSeries}, nil)
	T[KTimeStrToDate] = renameFunc("TO_DATE")
	T[KTimeStrToTime] = func(g *Generator, e *Expr) string { return timestrtotimeSQL(g, e, false) }
	T[KTimeStrToUnix] = renameFunc("UNIX_TIMESTAMP")
	T[KTimestampTrunc] = func(g *Generator, e *Expr) string { return g.fn("TRUNC", e.Arg("this"), unitToStr(e, "DAY")) }
	T[KTimeToUnix] = renameFunc("UNIX_TIMESTAMP")
	T[KToBase64] = renameFunc("BASE64")
	T[KTsOrDiToDi] = func(g *Generator, e *Expr) string {
		return "CAST(SUBSTR(REPLACE(CAST(" + g.sqlKey(e, "this") + " AS STRING), '-', ''), 1, 8) AS INT)"
	}
	T[KTsOrDsAdd] = hiveAddDateSQL
	T[KTsOrDsDiff] = hiveDateDiffSQL
	T[KTsOrDsToDate] = hiveToDateSQL
	T[KTryCast] = noTrycastSQL
	T[KTrim] = func(g *Generator, e *Expr) string { return trimSQL(g, e, "") }
	T[KUnicode] = renameFunc("ASCII")
	T[KUnixToStr] = func(g *Generator, e *Expr) string {
		return g.fn("FROM_UNIXTIME", e.Arg("this"), timeFormat("hive")(g, e))
	}
	T[KUnixToTime] = hiveUnixToTimeSQL
	T[KUnixToTimeStr] = renameFunc("FROM_UNIXTIME")
	T[KUnnest] = renameFunc("EXPLODE")
	T[KPartitionedByProperty] = func(g *Generator, e *Expr) string { return "PARTITIONED BY " + g.sqlKey(e, "this") }
	T[KNumberToStr] = renameFunc("FORMAT_NUMBER")
	T[KNational] = func(g *Generator, e *Expr) string { return g.nationalSQL(e, "") }
	T[KClusteredColumnConstraint] = func(g *Generator, e *Expr) string {
		return "(" + g.expressions(e, exprsOpts{key: "this", noIndent: true}) + ")"
	}
	T[KNonClusteredColumnConstraint] = func(g *Generator, e *Expr) string {
		return "(" + g.expressions(e, exprsOpts{key: "this", noIndent: true}) + ")"
	}
	T[KNotForReplicationColumnConstraint] = func(g *Generator, e *Expr) string { return "" }
	T[KOnProperty] = func(g *Generator, e *Expr) string { return "" }
	T[KPartitionedByBucket] = func(g *Generator, e *Expr) string { return g.fn("BUCKET", e.Arg("expression"), e.Arg("this")) }
	T[KPartitionByTruncate] = func(g *Generator, e *Expr) string { return g.fn("TRUNCATE", e.Arg("expression"), e.Arg("this")) }
	T[KPrimaryKeyColumnConstraint] = func(g *Generator, e *Expr) string { return "PRIMARY KEY" }
	T[KWeekOfYear] = renameFunc("WEEKOFYEAR")
	T[KDayOfMonth] = renameFunc("DAYOFMONTH")
	T[KDayOfWeek] = renameFunc("DAYOFWEEK")
	levenshtein := renameFunc("LEVENSHTEIN")
	T[KLevenshtein] = func(g *Generator, e *Expr) string {
		dhUnsupportedArgs(g, e, "ins_cost", "del_cost", "sub_cost", "max_dist")
		return levenshtein(g, e)
	}

	M := d.G.methods
	M[KRowFormatSerdeProperty] = hiveRowformatserdepropertySQL
	M[KTrunc] = hiveTruncSQL
	M[KSerdeProperties] = hiveSerdepropertiesSQL
	M[KTimeToStr] = hiveTimetostrSQL
	M[KFileFormatProperty] = hiveFileformatpropertySQL

	h := &d.G.h
	h.formatTime = hiveFormatTime
	h.ignorenullsSQL = hiveIgnorenullsSQL
	h.unnestSQL = hiveUnnestSQL
	h.jsonpathkeySQL = hiveJsonpathkeySQL
	h.parameterSQL = hiveParameterSQL
	h.schemaSQL = hiveSchemaSQL
	h.constraintSQL = hiveConstraintSQL
	h.arrayaggSQL = hiveArrayaggSQL
	h.datatypeSQL = hiveDatatypeSQL
	h.versionSQL = hiveVersionSQL
	h.structSQL = hiveStructSQL
	h.columndefSQL = hiveColumndefSQL
	h.altercolumnSQL = hiveAltercolumnSQL
	h.renamecolumnSQL = hiveRenamecolumnSQL
	h.altersetSQL = hiveAltersetSQL
	h.existsSQL = hiveExistsSQL
	h.usingpropertySQL = hiveUsingpropertySQL
}

// hiveFormatTime renders a format argument. In a strict-month dialect, the format of an
// expression that parses a string renders lax %m/%d as non-padded M/d.
func hiveFormatTime(g *Generator, e *Expr, inverseTimeMapping map[string]string, inverseTimeTrie *trie) string {
	// Inferred from the time mapping because dialects derived from Hive reuse this function.
	isDialectStrict := g.d.S.TIME_MAPPING["MM"] == "%mstrict"

	if isDialectStrict && inverseTimeMapping == nil && e.IsA(hivePARSE_TIME_EXPRESSIONS...) {
		// Render a lenient %m/%d non-padded (M/d) so single-digit sources stay parseable
		s, _ := formatTime(
			hiveLenientParseFormat(g.sqlKey(e, "format")),
			g.d.S.INVERSE_TIME_MAPPING,
			g.d.inverseTimeTrie,
		)
		return s
	}

	return g.baseFormatTime(e, inverseTimeMapping, inverseTimeTrie)
}

// hiveIgnorenullsSQL renders IGNORE NULLS on a FIRST/LAST-style function as a TRUE second
// argument, e.g. FIRST(x, TRUE).
func hiveIgnorenullsSQL(g *Generator, e *Expr) string {
	this := e.This()
	if this.IsA(g.s.IGNORE_NULLS_FUNCS...) {
		return g.fn(this.Kind().SQLName(), this.Arg("this"), Boolean(true))
	}

	return g.baseIgnorenullsSQL(e)
}

func hiveUnnestSQL(g *Generator, e *Expr) string {
	return renameFunc("EXPLODE")(g, e)
}

func hiveJsonpathkeySQL(g *Generator, e *Expr) string {
	if e.This().IsA(KJSONPathWildcard) {
		g.unsupported("Unsupported wildcard in JSONPathKey expression")
		return ""
	}

	return g.baseJsonpathkeySQL(e)
}

func hiveParameterSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	expressionSQL := g.sqlKey(e, "expression")

	parent := e.Parent()
	if expressionSQL != "" {
		this = this + ":" + expressionSQL
	}

	if parent.IsA(KEQ) && parent.Parent().IsA(KSetItem) {
		// We need to produce SET key = value instead of SET ${key} = value
		return this
	}

	return "${" + this + "}"
}

// hiveSchemaSQL renders a schema, dropping explicit ascending order (desc=false) from ordered
// columns.
func hiveSchemaSQL(g *Generator, e *Expr) string {
	for ordered := range e.FindAll(KOrdered) {
		if v, ok := ordered.Arg("desc").(bool); ok && !v {
			ordered.Set("desc", nil)
		}
	}

	return g.baseSchemaSQL(e)
}

func hiveConstraintSQL(g *Generator, e *Expr) string {
	for _, prop := range e.FindAllList(KProperties) {
		prop.Pop()
	}

	this := g.sqlKey(e, "this")
	expressions := g.expressions(e, exprsOpts{sep: strp2(" "), flat: true})
	return "CONSTRAINT " + this + " " + expressions
}

func hiveRowformatserdepropertySQL(g *Generator, e *Expr) string {
	serdeProps := g.sqlKey(e, "serde_properties")
	if serdeProps != "" {
		serdeProps = " " + serdeProps
	}
	return "ROW FORMAT SERDE " + g.sqlKey(e, "this") + serdeProps
}

func hiveArrayaggSQL(g *Generator, e *Expr) string {
	this := e.This()
	if this.IsA(KOrder) {
		this = this.This()
	}
	return g.fn("COLLECT_LIST", this)
}

// hiveTruncSQL renders numeric TRUNC as a CAST to BIGINT: Hive/Spark lack a native numeric
// TRUNC, and the cast truncates toward zero (it does not round). Decimals are unsupported.
func hiveTruncSQL(g *Generator, e *Expr) string {
	dhUnsupportedArgs(g, e, "decimals")
	return g.sql(CastExpr(e.This(), DT_BIGINT, true, nil))
}

// hiveDatatypeSQL renders a data type: unsized (or MAX) text types become TEXT, sized TEXT
// becomes VARCHAR, temporal types drop their precision and FLOAT(n) becomes FLOAT or DOUBLE.
func hiveDatatypeSQL(g *Generator, e *Expr) string {
	dt, isDType := e.Arg("this").(DType)
	exprs := e.Expressions()
	if isDType && g.s.PARAMETERIZABLE_TEXT_TYPES.Has(dt) && (len(exprs) == 0 || exprs[0].Name() == "MAX") {
		e.Set("this", DT_TEXT)
		e.Set("expressions", nil)
	} else if dhIsType(e, DT_TEXT) && len(exprs) > 0 {
		e.Set("this", DT_VARCHAR)
	} else if isDType && !g.s.SUPPORTS_TEMPORAL_PRECISION && DataType_TEMPORAL_TYPES.Has(dt) {
		e.Set("expressions", nil)
	} else if dhIsType(e, DT_FLOAT) {
		sizeExpression := e.Find(KDataTypeParam)
		if sizeExpression != nil {
			size, err := strconv.Atoi(strings.TrimSpace(sizeExpression.Name()))
			if err != nil {
				panic(&ValueError{Msg: fmt.Sprintf("invalid literal for int() with base 10: '%s'", sizeExpression.Name())})
			}
			if size <= 32 {
				e.Set("this", DT_FLOAT)
			} else {
				e.Set("this", DT_DOUBLE)
			}
			e.Set("expressions", nil)
		}
	}
	return g.baseDatatypeSQL(e)
}

func hiveVersionSQL(g *Generator, e *Expr) string {
	sql := g.baseVersionSQL(e)
	return strings.Replace(sql, "FOR ", "", 1)
}

func hiveStructSQL(g *Generator, e *Expr) string {
	values := []any{}

	for _, x := range e.Expressions() {
		if x.IsA(KPropertyEQ) {
			g.unsupported("Hive does not support named structs.")
			values = append(values, x.Expression())
		} else {
			values = append(values, x)
		}
	}

	return g.fn("STRUCT", values...)
}

func hiveColumndefSQL(g *Generator, e *Expr, sep string) string {
	if e.Parent().IsA(KDataType) && dhIsType(e.Parent(), DT_STRUCT) {
		sep = ": "
	}
	return g.baseColumndefSQL(e, sep)
}

func hiveAltercolumnSQL(g *Generator, e *Expr) string {
	if e.ArgB("exists") {
		g.unsupported("ALTER COLUMN IF EXISTS is not supported by this dialect")
	}

	this := g.sqlKey(e, "this")
	newName := g.sqlKey(e, "rename_to")
	if newName == "" {
		newName = this
	}
	dtype := g.sqlKey(e, "dtype")
	comment := ""
	if g.sqlKey(e, "comment") != "" {
		comment = " COMMENT " + g.sqlKey(e, "comment")
	}
	def := g.sqlKey(e, "default")
	visible := e.ArgB("visible")
	allowNull := e.ArgB("allow_null")
	drop := e.ArgB("drop")

	if def != "" || drop || visible || allowNull {
		g.unsupported("Unsupported CHANGE COLUMN syntax")
	}

	if dtype == "" {
		g.unsupported("CHANGE COLUMN without a type is not supported")
	}

	return "CHANGE COLUMN " + this + " " + newName + " " + dtype + comment
}

func hiveRenamecolumnSQL(g *Generator, e *Expr) string {
	g.unsupported("Cannot rename columns without data type defined in Hive")
	return ""
}

func hiveAltersetSQL(g *Generator, e *Expr) string {
	exprs := g.expressions(e, exprsOpts{flat: true})
	if exprs != "" {
		exprs = " " + exprs
	}
	location := g.sqlKey(e, "location")
	if location != "" {
		location = " LOCATION " + location
	}
	fileFormat := g.expressions(e, exprsOpts{key: "file_format", flat: true, sep: strp2(" ")})
	if fileFormat != "" {
		fileFormat = " FILEFORMAT " + fileFormat
	}
	serde := g.sqlKey(e, "serde")
	if serde != "" {
		serde = " SERDE " + serde
	}
	tags := g.expressions(e, exprsOpts{key: "tag", flat: true, sep: strp2("")})
	if tags != "" {
		tags = " TAGS " + tags
	}

	return "SET" + serde + exprs + location + fileFormat + tags
}

func hiveSerdepropertiesSQL(g *Generator, e *Expr) string {
	prefix := ""
	if e.ArgB("with_") {
		prefix = "WITH "
	}
	exprs := g.expressions(e, exprsOpts{flat: true})

	return prefix + "SERDEPROPERTIES (" + exprs + ")"
}

// hiveExistsSQL renders EXISTS; the two-argument function form is rendered as a plain function
// call.
func hiveExistsSQL(g *Generator, e *Expr) string {
	if e.Expression() != nil {
		return g.functionFallbackSQL(e)
	}

	return g.baseExistsSQL(e)
}

func hiveTimetostrSQL(g *Generator, e *Expr) string {
	this := e.This()
	if this.IsA(KTimeStrToTime) {
		this = this.This()
	}

	return g.fn("DATE_FORMAT", this, hiveNilIfEmpty(g.formatTime(e, nil, nil)))
}

func hiveUsingpropertySQL(g *Generator, e *Expr) string {
	kind := e.Arg("kind")
	return "USING " + chunkAPyStr(kind) + " " + g.sqlKey(e, "this")
}

func hiveFileformatpropertySQL(g *Generator, e *Expr) string {
	var this string
	if e.This().IsA(KInputOutputFormat) {
		this = g.sqlKey(e, "this")
	} else {
		this = pyUpper(e.Name())
	}

	return "STORED AS " + this
}
