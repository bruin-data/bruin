package sqlengine

import (
	"fmt"
	"strings"
)

// mysqlFormatTimeArg returns nil when e has no format, so g.fn drops the argument.
func mysqlFormatTimeArg(g *Generator, e *Expr) any {
	if s := g.formatTime(e, nil, nil); s != "" {
		return s
	}
	return nil
}

// mysqlDateTruncSQL renders DATE_TRUNC as STR_TO_DATE over a CONCAT of the kept date parts, or
// DATE(x) for DAY.
func mysqlDateTruncSQL(g *Generator, e *Expr) string {
	expr := g.sqlKey(e, "this")
	unit := pyUpper(e.Text("unit"))

	var concat, dateFormat string
	switch unit {
	case "WEEK":
		concat = fmt.Sprintf("CONCAT(YEAR(%s), ' ', WEEK(%s, 1), ' 1')", expr, expr)
		dateFormat = "%Y %u %w"
	case "MONTH":
		concat = fmt.Sprintf("CONCAT(YEAR(%s), ' ', MONTH(%s), ' 1')", expr, expr)
		dateFormat = "%Y %c %e"
	case "QUARTER":
		concat = fmt.Sprintf("CONCAT(YEAR(%s), ' ', QUARTER(%s) * 3 - 2, ' 1')", expr, expr)
		dateFormat = "%Y %c %e"
	case "YEAR":
		concat = fmt.Sprintf("CONCAT(YEAR(%s), ' 1 1')", expr)
		dateFormat = "%Y %c %e"
	default:
		if unit != "DAY" {
			g.unsupported("Unexpected interval unit: " + unit)
		}
		return g.fn("DATE", expr)
	}

	return g.fn("STR_TO_DATE", concat, "'"+dateFormat+"'")
}

func mysqlStrToDateSQL(g *Generator, e *Expr) string {
	return g.fn("STR_TO_DATE", e.Arg("this"), mysqlFormatTimeArg(g, e))
}

func mysqlUnixToTimeSQL(g *Generator, e *Expr) string {
	scale := e.ArgE("scale")
	timestamp := e.Arg("this")

	if scale == nil || scale.Equal(LiteralInt(0)) {
		return g.fn("FROM_UNIXTIME", timestamp, mysqlFormatTimeArg(g, e))
	}

	pow := New(KPow, "this", LiteralNumber("10"), "expression", scale.Copy())
	return g.fn(
		"FROM_UNIXTIME",
		New(KDiv, "this", timestamp, "expression", pow),
		mysqlFormatTimeArg(g, e),
	)
}

func mysqlDateAddSQL(kind string) GenFunc {
	return func(g *Generator, e *Expr) string {
		return g.fn(
			"DATE_"+kind,
			e.Arg("this"),
			New(KInterval, "this", e.Arg("expression"), "unit", unitToVar(e, "DAY")),
		)
	}
}

var mysqlMakeIntervalUnitAliases = map[string]string{
	"years":   "year",
	"months":  "month",
	"weeks":   "week",
	"days":    "day",
	"hours":   "hour",
	"minutes": "minute",
	"mins":    "minute",
	"seconds": "second",
	"secs":    "second",
}

func mysqlTsOrDsToDateSQL(g *Generator, e *Expr) string {
	if e.ArgB("format") {
		return mysqlStrToDateSQL(g, e)
	}
	return g.fn("DATE", e.Arg("this"))
}

func mysqlRemoveTsOrDsToDate(toSQL GenFunc, args ...string) GenFunc {
	if len(args) == 0 {
		args = []string{"this"}
	}
	return func(g *Generator, e *Expr) string {
		for _, argKey := range args {
			arg := e.ArgE(argKey)
			if arg.IsA(KTsOrDsToDate, KTsOrDsToTimestamp) && !arg.ArgB("format") {
				e.Set(argKey, arg.This())
			}
		}

		if toSQL != nil {
			return toSQL(g, e)
		}
		return g.functionFallbackSQL(e)
	}
}

func customizeMySQLGenerator(d *Dialect) {
	G := d.G

	for _, k := range []string{"cluster", "distribute", "sort"} {
		delete(G.AFTER_HAVING_MODIFIER_TRANSFORMS, k)
	}
	keys := []string{}
	for _, k := range G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS {
		if _, ok := G.AFTER_HAVING_MODIFIER_TRANSFORMS[k]; ok {
			keys = append(keys, k)
		}
	}
	G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS = keys

	T := G.TRANSFORMS
	T[KArrayAgg] = renameFunc("GROUP_CONCAT")
	T[KStddev] = renameFunc("STDDEV_SAMP")
	T[KVariancePop] = renameFunc("VAR_POP")
	T[KBitwiseAndAgg] = renameFunc("BIT_AND")
	T[KBitwiseOrAgg] = renameFunc("BIT_OR")
	T[KBitwiseXorAgg] = renameFunc("BIT_XOR")
	T[KBitwiseCount] = renameFunc("BIT_COUNT")
	T[KChr] = func(g *Generator, e *Expr) string { return g.chrSQL(e, "CHAR") }
	T[KCurrentDate] = noParenCurrentDateSQL
	T[KCurrentVersion] = renameFunc("VERSION")
	T[KDateDiff] = mysqlRemoveTsOrDsToDate(func(g *Generator, e *Expr) string {
		return g.fn("DATEDIFF", e.Arg("this"), e.Arg("expression"))
	}, "this", "expression")
	T[KDateAdd] = mysqlDateAddSQL("ADD")
	T[KDateStrToDate] = datestrtodateSQL
	T[KDateSub] = mysqlDateAddSQL("SUB")
	T[KDateTrunc] = mysqlDateTruncSQL
	T[KDay] = mysqlRemoveTsOrDsToDate(nil)
	T[KDayOfMonth] = mysqlRemoveTsOrDsToDate(renameFunc("DAYOFMONTH"))
	T[KDayOfWeek] = mysqlRemoveTsOrDsToDate(renameFunc("DAYOFWEEK"))
	T[KDayOfYear] = mysqlRemoveTsOrDsToDate(renameFunc("DAYOFYEAR"))
	T[KGroupConcat] = func(g *Generator, e *Expr) string {
		sep := g.sqlKey(e, "separator")
		if sep == "" {
			sep = "','"
		}
		return "GROUP_CONCAT(" + g.sqlKey(e, "this") + " SEPARATOR " + sep + ")"
	}
	T[KILike] = noIlikeSQL
	// https://dev.mysql.com/doc/refman/9.7/en/flow-control-functions.html#function_if
	T[KIf] = ifSQL("IF", "NULL")
	T[KJSONExtractScalar] = arrowJSONExtractSQL
	T[KLength] = lengthOrCharLengthSQL
	T[KLogicalOr] = renameFunc("MAX")
	T[KLogicalAnd] = renameFunc("MIN")
	T[KMax] = maxOrGreatest
	T[KMin] = minOrLeast
	T[KMonth] = mysqlRemoveTsOrDsToDate(nil)
	T[KNullSafeEQ] = func(g *Generator, e *Expr) string { return g.binary(e, "<=>") }
	T[KNullSafeNEQ] = func(g *Generator, e *Expr) string { return "NOT " + g.binary(e, "<=>") }
	T[KNumberToStr] = renameFunc("FORMAT")
	T[KPivot] = noPivotSQL
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		transformEliminateDistinctOn,
		transformEliminateSemiAndAntiJoins,
		transformEliminateQualify,
		transformEliminateFullOuterJoin,
		transformUnnestGenerateDateArrayUsingRecursiveCte,
	}, nil)
	T[KStrPosition] = func(g *Generator, e *Expr) string {
		return strpositionSQL(g, e, "LOCATE", true, false, true)
	}
	T[KStrToDate] = mysqlStrToDateSQL
	T[KStrToTime] = mysqlStrToDateSQL
	T[KStuff] = renameFunc("INSERT")
	T[KSessionUser] = func(g *Generator, e *Expr) string { return "SESSION_USER()" }
	T[KTableSample] = noTablesampleSQL
	T[KTimeFromParts] = renameFunc("MAKETIME")
	T[KTimestampAdd] = dateAddIntervalSQL("DATE", "ADD")
	T[KTimestampDiff] = func(g *Generator, e *Expr) string {
		return g.fn("TIMESTAMPDIFF", unitToVar(e, "DAY"), e.Arg("expression"), e.Arg("this"))
	}
	T[KTimestampSub] = dateAddIntervalSQL("DATE", "SUB")
	T[KTimeStrToUnix] = renameFunc("UNIX_TIMESTAMP")
	T[KTimeStrToTime] = func(g *Generator, e *Expr) string {
		return timestrtotimeSQL(g, e, !e.ArgB("zone"))
	}
	T[KTimeToStr] = mysqlRemoveTsOrDsToDate(func(g *Generator, e *Expr) string {
		return g.fn("DATE_FORMAT", e.Arg("this"), mysqlFormatTimeArg(g, e))
	})
	T[KTrim] = func(g *Generator, e *Expr) string { return trimSQL(g, e, "") }
	T[KTrunc] = func(g *Generator, e *Expr) string {
		decimals := e.ArgE("decimals")
		if decimals == nil {
			decimals = LiteralNumber("0")
		}
		return g.fn("TRUNCATE", e.This(), decimals)
	}
	T[KTryCast] = noTrycastSQL
	T[KTsOrDsAdd] = mysqlDateAddSQL("ADD")
	T[KTsOrDsDiff] = func(g *Generator, e *Expr) string {
		return g.fn("DATEDIFF", e.Arg("this"), e.Arg("expression"))
	}
	T[KTsOrDsToDate] = mysqlTsOrDsToDateSQL
	T[KUnicode] = func(g *Generator, e *Expr) string {
		return "ORD(CONVERT(" + g.sql(e.Arg("this")) + " USING utf32))"
	}
	T[KUnixToTime] = mysqlUnixToTimeSQL
	T[KWeek] = mysqlRemoveTsOrDsToDate(nil)
	T[KWeekOfYear] = mysqlRemoveTsOrDsToDate(renameFunc("WEEKOFYEAR"))
	T[KYear] = mysqlRemoveTsOrDsToDate(nil)
	T[KUtcTimestamp] = renameFunc("UTC_TIMESTAMP")
	T[KUtcTime] = renameFunc("UTC_TIME")

	G.h.locateProperties = mysqlLocateProperties
	G.h.computedcolumnconstraintSQL = mysqlComputedcolumnconstraintSQL
	G.h.dpipeSQL = mysqlDpipeSQL
	G.h.extractSQL = mysqlExtractSQL
	G.h.datatypeSQL = mysqlDatatypeSQL
	G.h.castSQL = mysqlCastSQL
	G.h.showSQL = mysqlShowSQL
	G.h.alterrenameSQL = mysqlAlterrenameSQL
	G.h.altercolumnSQL = mysqlAltercolumnSQL
	G.h.converttimezoneSQL = mysqlConverttimezoneSQL
	G.h.attimezoneSQL = mysqlAttimezoneSQL
	G.h.ignorenullsSQL = mysqlIgnorenullsSQL
	G.h.partitionSQL = mysqlPartitionSQL
	G.h.partitionbyrangepropertySQL = mysqlPartitionbyrangepropertySQL
	G.h.partitionrangeSQL = mysqlPartitionrangeSQL

	G.methods[KMakeInterval] = mysqlMakeintervalSQL
	G.methods[KIntDiv] = mysqlIntdivSQL
	G.methods[KArray] = mysqlArraySQL
	G.methods[KArrayContainsAll] = mysqlArraycontainsallSQL
	G.methods[KArrayContainedBy] = mysqlArraycontainedbySQL
	G.methods[KJSONArrayContains] = mysqlJsonarraycontainsSQL
	G.methods[KTimestampTrunc] = mysqlTimestamptruncSQL
	G.methods[KIsAscii] = mysqlIsasciiSQL
	G.methods[KCurrentSchema] = mysqlCurrentschemaSQL
	G.methods[KPartitionByListProperty] = mysqlPartitionbylistpropertySQL
	G.methods[KPartitionList] = mysqlPartitionlistSQL
}

// mysqlMakeintervalSQL renders MAKE_INTERVAL as a sum of INTERVAL terms (a difference when it is
// the subtrahend of a subtraction).
func mysqlMakeintervalSQL(g *Generator, e *Expr) string {
	var intervals []*Expr
	for _, a := range e.args {
		if a.val == nil {
			continue
		}
		value, ok := a.val.(*Expr)
		if !ok || value == nil {
			continue
		}

		var unitName string
		if value.IsA(KKwarg) {
			lower := pyLower(value.This().Name())
			if alias, ok := mysqlMakeIntervalUnitAliases[lower]; ok {
				unitName = alias
			} else {
				unitName = lower
			}
			value = value.Expression()
		} else {
			unitName = a.key
		}

		intervals = append(intervals, New(KInterval, "this", value.Copy(), "unit", VarExpr(pyUpper(unitName))))
	}

	if len(intervals) == 0 {
		return g.functionFallbackSQL(e)
	}

	parent := e.Parent()
	sep := " + "
	if parent.IsA(KSub) && parent.Expression() == e {
		sep = " - "
	}

	parts := make([]string, len(intervals))
	for i, interval := range intervals {
		parts[i] = g.sql(interval)
	}
	return strings.Join(parts, sep)
}

func mysqlLocateProperties(g *Generator, properties *Expr) propLocations {
	locations := g.baseLocateProperties(properties)

	// MySQL puts SQL SECURITY before VIEW but after the schema for functions/procedures
	if create := properties.Parent(); create.IsA(KCreate) && pyUpper(create.ArgS("kind")) == "VIEW" {
		postSchema := locations[Loc_POST_SCHEMA]
		for i, p := range postSchema {
			if p.IsA(KSqlSecurityProperty) {
				locations[Loc_POST_SCHEMA] = append(postSchema[:i:i], postSchema[i+1:]...)
				locations[g.s.SQL_SECURITY_VIEW_LOCATION] = append(locations[g.s.SQL_SECURITY_VIEW_LOCATION], p)
				break
			}
		}
	}

	return locations
}

func mysqlComputedcolumnconstraintSQL(g *Generator, e *Expr) string {
	persisted := "VIRTUAL"
	if e.ArgB("persisted") {
		persisted = "STORED"
	}
	return "GENERATED ALWAYS AS (" + g.sql(e.This().Unnest()) + ") " + persisted
}

func mysqlArraySQL(g *Generator, e *Expr) string {
	g.unsupported("Arrays are not supported by MySQL")
	return g.functionFallbackSQL(e)
}

func mysqlArraycontainsallSQL(g *Generator, e *Expr) string {
	g.unsupported("Array operations are not supported by MySQL")
	return g.functionFallbackSQL(e)
}

func mysqlArraycontainedbySQL(g *Generator, e *Expr) string {
	g.unsupported("Array operations are not supported by MySQL")
	return g.functionFallbackSQL(e)
}

func mysqlDpipeSQL(g *Generator, e *Expr) string {
	parts := e.Flatten(true)
	args := make([]any, len(parts))
	for i, x := range parts {
		args[i] = x
	}
	return g.fn("CONCAT", args...)
}

// mysqlIntdivSQL renders a DIV b, parenthesizing binary operands. The whole expression is
// parenthesized under a unary minus or bitwise NOT and as the right operand of *, /, DIV or %,
// where DIV would otherwise bind differently.
func mysqlIntdivSQL(g *Generator, e *Expr) string {
	this := g.sql(wrapIfKind(e.This(), KBinary))
	divisor := g.sql(wrapIfKind(e.Expression(), KBinary))
	sql := this + " " + g.maybeCommentC("DIV", nil, e.Comments()) + " " + divisor

	parent := e.Parent()
	if parent.IsA(KNeg, KBitwiseNot) || (parent.IsA(KMul, KDiv, KIntDiv, KMod) && parent.Expression() == e) {
		return "(" + sql + ")"
	}
	return sql
}

func mysqlExtractSQL(g *Generator, e *Expr) string {
	unit := e.Name()
	if unit != "" && pyLower(unit) == "epoch" {
		return g.fn("UNIX_TIMESTAMP", e.Arg("expression"))
	}

	return g.baseExtractSQL(e)
}

func mysqlDatatypeSQL(g *Generator, e *Expr) string {
	if g.s.VARCHAR_REQUIRES_SIZE && DataTypeIsType(e, []any{DT_VARCHAR}, false) && len(e.Expressions()) == 0 {
		// `VARCHAR` must always have a size - if it doesn't, we always generate `TEXT`
		return "TEXT"
	}

	// https://dev.mysql.com/doc/refman/8.0/en/numeric-type-syntax.html
	result := g.baseDatatypeSQL(e)
	if dt, ok := e.Arg("this").(DType); ok {
		if _, ok := g.s.UNSIGNED_TYPE_MAPPING[dt]; ok {
			result = result + " UNSIGNED"
		}
	}

	return result
}

func mysqlJsonarraycontainsSQL(g *Generator, e *Expr) string {
	return g.sqlKey(e, "this") + " MEMBER OF(" + g.sqlKey(e, "expression") + ")"
}

func mysqlCastSQL(g *Generator, e *Expr, safePrefix string) string {
	to := e.ArgE("to")
	toThis, isDType := to.Arg("this").(DType)
	if isDType && g.s.TIMESTAMP_FUNC_TYPES.Has(toThis) {
		return g.fn("TIMESTAMP", e.Arg("this"))
	}

	if isDType {
		if mapped := g.s.CAST_MAPPING[toThis]; mapped != "" {
			to.Set("this", mapped)
		}
	}
	return g.baseCastSQL(e, "")
}

func mysqlShowSQL(g *Generator, e *Expr) string {
	name := e.Name()
	this := " " + name
	full := ""
	if e.ArgB("full") {
		full = " FULL"
	}
	global := ""
	if e.ArgB("global_") {
		global = " GLOBAL"
	}

	target := g.sqlKey(e, "target")

	// SHOW CREATE ... takes a possibly qualified name, it has no FROM clause
	// https://dev.mysql.com/doc/refman/8.4/en/show-create-table.html
	var db string
	if dbID := g.sqlKey(e, "db"); strings.HasPrefix(name, "CREATE") && dbID != "" {
		target = dbID + "." + target
	} else {
		db = mysqlPrefixedSQL(g, "FROM", e, "db")
	}

	if target != "" {
		target = " " + target
	}
	switch name {
	case "COLUMNS", "INDEX":
		target = " FROM" + target
	case "GRANTS":
		target = " FOR" + target
	case "LINKS", "PARTITIONS":
		if target != "" {
			target = " ON" + target
		}
	case "PROJECTIONS":
		if target != "" {
			target = " ON TABLE" + target
		}
	}

	like := mysqlPrefixedSQL(g, "LIKE", e, "like")
	where := g.sqlKey(e, "where")

	types := g.expressions(e, exprsOpts{key: "types"})
	if types != "" {
		types = " " + types
	}
	query := mysqlPrefixedSQL(g, "FOR QUERY", e, "query")

	var offset, limit string
	if name == "PROFILE" {
		offset = mysqlPrefixedSQL(g, "OFFSET", e, "offset")
		limit = mysqlPrefixedSQL(g, "LIMIT", e, "limit")
	} else {
		offset = ""
		limit = mysqlOldstyleLimitSQL(g, e)
	}

	log := mysqlPrefixedSQL(g, "IN", e, "log")
	position := mysqlPrefixedSQL(g, "FROM", e, "position")

	channel := mysqlPrefixedSQL(g, "FOR CHANNEL", e, "channel")

	mutexOrStatus := ""
	if name == "ENGINE" {
		if e.ArgB("mutex") {
			mutexOrStatus = " MUTEX"
		} else {
			mutexOrStatus = " STATUS"
		}
	}

	forTable := mysqlPrefixedSQL(g, "FOR TABLE", e, "for_table")
	forGroup := mysqlPrefixedSQL(g, "FOR GROUP", e, "for_group")
	forUser := mysqlPrefixedSQL(g, "FOR USER", e, "for_user")
	forRole := mysqlPrefixedSQL(g, "FOR ROLE", e, "for_role")
	intoOutfile := mysqlPrefixedSQL(g, "INTO OUTFILE", e, "into_outfile")
	json := ""
	if e.ArgB("json") {
		json = " JSON"
	}

	return "SHOW" + full + global + this + json + target + forTable + types + db + query + log + position +
		channel + mutexOrStatus + like + where + offset + limit + forGroup + forUser + forRole + intoOutfile
}

// mysqlAlterrenameSQL omits the TO keyword of ALTER ... RENAME, which MySQL, Doris and StarRocks
// all leave out.
func mysqlAlterrenameSQL(g *Generator, e *Expr, includeTo bool) string {
	return g.baseAlterrenameSQL(e, false)
}

func mysqlAltercolumnSQL(g *Generator, e *Expr) string {
	dtype := g.sqlKey(e, "dtype")
	if dtype == "" {
		return g.baseAltercolumnSQL(e)
	}

	if e.ArgB("exists") {
		g.unsupported("ALTER COLUMN IF EXISTS is not supported by this dialect")
	}

	this := g.sqlKey(e, "this")
	return "MODIFY COLUMN " + this + " " + dtype
}

func mysqlPrefixedSQL(g *Generator, prefix string, e *Expr, arg string) string {
	sql := g.sqlKey(e, arg)
	if sql != "" {
		return " " + prefix + " " + sql
	}
	return ""
}

func mysqlOldstyleLimitSQL(g *Generator, e *Expr) string {
	limit := g.sqlKey(e, "limit")
	offset := g.sqlKey(e, "offset")
	if limit != "" {
		limitOffset := limit
		if offset != "" {
			limitOffset = offset + ", " + limit
		}
		return " LIMIT " + limitOffset
	}
	return ""
}

// mysqlTimestamptruncSQL renders TIMESTAMP_TRUNC as DATE_ADD(start, INTERVAL
// TIMESTAMPDIFF(unit, start, x) unit) from an old fixed start date.
func mysqlTimestamptruncSQL(g *Generator, e *Expr) string {
	unit := e.Arg("unit")

	// Pick an old-enough date to avoid negative timestamp diffs
	startTS := "'0000-01-01 00:00:00'"

	// Source: https://stackoverflow.com/a/32955740
	timestampDiff := New(KTimestampDiff, "this", e.Arg("this"), "expression", startTS, "unit", unit)
	interval := New(KInterval, "this", timestampDiff, "unit", unit)
	dateadd := New(KDateAdd, "this", startTS, "expression", interval.This(), "unit", unitToStr(interval, "DAY"))

	return g.sql(dateadd)
}

func mysqlConverttimezoneSQL(g *Generator, e *Expr) string {
	fromTz := e.Arg("source_tz")
	toTz := e.Arg("target_tz")
	dt := e.Arg("timestamp")

	return g.fn("CONVERT_TZ", dt, fromTz, toTz)
}

func mysqlAttimezoneSQL(g *Generator, e *Expr) string {
	g.unsupported("AT TIME ZONE is not supported by MySQL")
	return g.sql(e.Arg("this"))
}

func mysqlIsasciiSQL(g *Generator, e *Expr) string {
	return "REGEXP_LIKE(" + g.sql(e.Arg("this")) + ", '^[[:ascii:]]*$')"
}

func mysqlIgnorenullsSQL(g *Generator, e *Expr) string {
	// https://dev.mysql.com/doc/refman/8.4/en/window-function-descriptions.html
	g.unsupported("MySQL does not support IGNORE NULLS.")
	return g.sql(e.Arg("this"))
}

func mysqlCurrentschemaSQL(g *Generator, e *Expr) string {
	if e.ArgB("this") {
		g.unsupported(fmt.Sprintf("Argument '%s' is not supported for expression '%s' when targeting %s.", "this", e.Kind().Name(), g.d.ClassName))
	}
	return g.fn("SCHEMA")
}

func mysqlPartitionSQL(g *Generator, e *Expr) string {
	parent := e.Parent()
	if parent.IsA(KPartitionByRangeProperty, KPartitionByListProperty) {
		return g.expressions(e, exprsOpts{flat: true})
	}
	return g.basePartitionSQL(e)
}

func mysqlPartitionBySQL(g *Generator, e *Expr, kind string) string {
	partitions := g.expressions(e, exprsOpts{key: "partition_expressions", flat: true})
	create := g.expressions(e, exprsOpts{key: "create_expressions", flat: true})
	return "PARTITION BY " + kind + " (" + partitions + ") (" + create + ")"
}

func mysqlPartitionbyrangepropertySQL(g *Generator, e *Expr) string {
	return mysqlPartitionBySQL(g, e, "RANGE")
}

func mysqlPartitionbylistpropertySQL(g *Generator, e *Expr) string {
	return mysqlPartitionBySQL(g, e, "LIST")
}

func mysqlPartitionlistSQL(g *Generator, e *Expr) string {
	name := g.sqlKey(e, "this")
	values := g.expressions(e, exprsOpts{flat: true})
	return "PARTITION " + name + " VALUES IN (" + values + ")"
}

func mysqlPartitionrangeSQL(g *Generator, e *Expr) string {
	name := g.sqlKey(e, "this")
	values := g.expressions(e, exprsOpts{flat: true})
	return "PARTITION " + name + " VALUES LESS THAN (" + values + ")"
}
