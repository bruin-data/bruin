package sqlengine

// Port of sqlglot/generators/mysql.py (MySQLGenerator).

import (
	"fmt"
	"strings"
)

// ---------------------------------------------------------------------------------------------
// Module-level helpers of sqlglot/generators/mysql.py
// ---------------------------------------------------------------------------------------------

// mysqlFormatTimeArg returns Generator.format_time(expression) as a Generator.func argument
// (nil for Python None).
func mysqlFormatTimeArg(g *Generator, e *Expr) any {
	if s := g.formatTime(e, nil, nil); s != "" {
		return s
	}
	return nil
}

// mysqlDateTruncSQL mirrors _date_trunc_sql.
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

// mysqlStrToDateSQL mirrors _str_to_date_sql.
func mysqlStrToDateSQL(g *Generator, e *Expr) string {
	return g.fn("STR_TO_DATE", e.Arg("this"), mysqlFormatTimeArg(g, e))
}

// mysqlUnixToTimeSQL mirrors _unix_to_time_sql.
func mysqlUnixToTimeSQL(g *Generator, e *Expr) string {
	scale := e.ArgE("scale")
	timestamp := e.Arg("this")

	// scale in (None, exp.UnixToTime.SECONDS)
	if scale == nil || scale.Equal(LiteralInt(0)) {
		return g.fn("FROM_UNIXTIME", timestamp, mysqlFormatTimeArg(g, e))
	}

	// exp.func("POW", 10, scale)
	pow := New(KPow, "this", LiteralNumber("10"), "expression", scale.Copy())
	return g.fn(
		"FROM_UNIXTIME",
		New(KDiv, "this", timestamp, "expression", pow),
		mysqlFormatTimeArg(g, e),
	)
}

// mysqlDateAddSQL mirrors date_add_sql(kind).
func mysqlDateAddSQL(kind string) GenFunc {
	return func(g *Generator, e *Expr) string {
		return g.fn(
			"DATE_"+kind,
			e.Arg("this"),
			New(KInterval, "this", e.Arg("expression"), "unit", unitToVar(e, "DAY")),
		)
	}
}

// mysqlMakeIntervalUnitAliases mirrors _MAKE_INTERVAL_UNIT_ALIASES.
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

// mysqlTsOrDsToDateSQL mirrors _ts_or_ds_to_date_sql.
func mysqlTsOrDsToDateSQL(g *Generator, e *Expr) string {
	if e.ArgB("format") {
		return mysqlStrToDateSQL(g, e)
	}
	return g.fn("DATE", e.Arg("this"))
}

// mysqlRemoveTsOrDsToDate mirrors _remove_ts_or_ds_to_date(to_sql=None, args=("this",)).
// toSQL nil means None; args nil means ("this",).
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

// ---------------------------------------------------------------------------------------------
// Customizer (class body of MySQLGenerator)
// ---------------------------------------------------------------------------------------------

func customizeMySQLGenerator(d *Dialect) {
	G := d.G

	// AFTER_HAVING_MODIFIER_TRANSFORMS = generator.AFTER_HAVING_MODIFIER_TRANSFORMS (module-level)
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
	T[KDateAdd] = mysqlRemoveTsOrDsToDate(mysqlDateAddSQL("ADD"))
	T[KDateStrToDate] = datestrtodateSQL
	T[KDateSub] = mysqlRemoveTsOrDsToDate(mysqlDateAddSQL("SUB"))
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
	T[KTrunc] = renameFunc("TRUNCATE")
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

	// Method overrides (hooks)
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

	// Dialect-only <key>_sql methods
	G.methods[KMakeInterval] = mysqlMakeintervalSQL
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

// ---------------------------------------------------------------------------------------------
// MySQLGenerator methods
// ---------------------------------------------------------------------------------------------

// mysqlMakeintervalSQL mirrors MySQLGenerator.makeinterval_sql.
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

// mysqlLocateProperties mirrors MySQLGenerator.locate_properties.
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

// mysqlComputedcolumnconstraintSQL mirrors MySQLGenerator.computedcolumnconstraint_sql.
func mysqlComputedcolumnconstraintSQL(g *Generator, e *Expr) string {
	persisted := "VIRTUAL"
	if e.ArgB("persisted") {
		persisted = "STORED"
	}
	return "GENERATED ALWAYS AS (" + g.sql(e.This().Unnest()) + ") " + persisted
}

// mysqlArraySQL mirrors MySQLGenerator.array_sql.
func mysqlArraySQL(g *Generator, e *Expr) string {
	g.unsupported("Arrays are not supported by MySQL")
	return g.functionFallbackSQL(e)
}

// mysqlArraycontainsallSQL mirrors MySQLGenerator.arraycontainsall_sql.
func mysqlArraycontainsallSQL(g *Generator, e *Expr) string {
	g.unsupported("Array operations are not supported by MySQL")
	return g.functionFallbackSQL(e)
}

// mysqlArraycontainedbySQL mirrors MySQLGenerator.arraycontainedby_sql.
func mysqlArraycontainedbySQL(g *Generator, e *Expr) string {
	g.unsupported("Array operations are not supported by MySQL")
	return g.functionFallbackSQL(e)
}

// mysqlDpipeSQL mirrors MySQLGenerator.dpipe_sql.
func mysqlDpipeSQL(g *Generator, e *Expr) string {
	parts := e.Flatten(true)
	args := make([]any, len(parts))
	for i, x := range parts {
		args[i] = x
	}
	return g.fn("CONCAT", args...)
}

// mysqlExtractSQL mirrors MySQLGenerator.extract_sql.
func mysqlExtractSQL(g *Generator, e *Expr) string {
	unit := e.Name()
	if unit != "" && pyLower(unit) == "epoch" {
		return g.fn("UNIX_TIMESTAMP", e.Arg("expression"))
	}

	return g.baseExtractSQL(e)
}

// mysqlDatatypeSQL mirrors MySQLGenerator.datatype_sql.
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

// mysqlJsonarraycontainsSQL mirrors MySQLGenerator.jsonarraycontains_sql.
func mysqlJsonarraycontainsSQL(g *Generator, e *Expr) string {
	return g.sqlKey(e, "this") + " MEMBER OF(" + g.sqlKey(e, "expression") + ")"
}

// mysqlCastSQL mirrors MySQLGenerator.cast_sql.
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

// mysqlShowSQL mirrors MySQLGenerator.show_sql.
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

	db := mysqlPrefixedSQL(g, "FROM", e, "db")

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

// mysqlAlterrenameSQL mirrors MySQLGenerator.alterrename_sql.
//
// To avoid TO keyword in ALTER ... RENAME statements.
// It's moved from Doris, because it's the same for all MySQL, Doris, and StarRocks.
func mysqlAlterrenameSQL(g *Generator, e *Expr, includeTo bool) string {
	return g.baseAlterrenameSQL(e, false)
}

// mysqlAltercolumnSQL mirrors MySQLGenerator.altercolumn_sql.
func mysqlAltercolumnSQL(g *Generator, e *Expr) string {
	dtype := g.sqlKey(e, "dtype")
	if dtype == "" {
		return g.baseAltercolumnSQL(e)
	}

	this := g.sqlKey(e, "this")
	return "MODIFY COLUMN " + this + " " + dtype
}

// mysqlPrefixedSQL mirrors MySQLGenerator._prefixed_sql.
func mysqlPrefixedSQL(g *Generator, prefix string, e *Expr, arg string) string {
	sql := g.sqlKey(e, arg)
	if sql != "" {
		return " " + prefix + " " + sql
	}
	return ""
}

// mysqlOldstyleLimitSQL mirrors MySQLGenerator._oldstyle_limit_sql.
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

// mysqlTimestamptruncSQL mirrors MySQLGenerator.timestamptrunc_sql.
func mysqlTimestamptruncSQL(g *Generator, e *Expr) string {
	unit := e.Arg("unit")

	// Pick an old-enough date to avoid negative timestamp diffs
	startTS := "'0000-01-01 00:00:00'"

	// Source: https://stackoverflow.com/a/32955740
	// build_date_delta(exp.TimestampDiff)([unit, start_ts, expression.this])
	timestampDiff := New(KTimestampDiff, "this", e.Arg("this"), "expression", startTS, "unit", unit)
	interval := New(KInterval, "this", timestampDiff, "unit", unit)
	// build_date_delta_with_interval(exp.DateAdd)([start_ts, interval])
	dateadd := New(KDateAdd, "this", startTS, "expression", interval.This(), "unit", unitToStr(interval, "DAY"))

	return g.sql(dateadd)
}

// mysqlConverttimezoneSQL mirrors MySQLGenerator.converttimezone_sql.
func mysqlConverttimezoneSQL(g *Generator, e *Expr) string {
	fromTz := e.Arg("source_tz")
	toTz := e.Arg("target_tz")
	dt := e.Arg("timestamp")

	return g.fn("CONVERT_TZ", dt, fromTz, toTz)
}

// mysqlAttimezoneSQL mirrors MySQLGenerator.attimezone_sql.
func mysqlAttimezoneSQL(g *Generator, e *Expr) string {
	g.unsupported("AT TIME ZONE is not supported by MySQL")
	return g.sql(e.Arg("this"))
}

// mysqlIsasciiSQL mirrors MySQLGenerator.isascii_sql.
func mysqlIsasciiSQL(g *Generator, e *Expr) string {
	return "REGEXP_LIKE(" + g.sql(e.Arg("this")) + ", '^[[:ascii:]]*$')"
}

// mysqlIgnorenullsSQL mirrors MySQLGenerator.ignorenulls_sql.
func mysqlIgnorenullsSQL(g *Generator, e *Expr) string {
	// https://dev.mysql.com/doc/refman/8.4/en/window-function-descriptions.html
	g.unsupported("MySQL does not support IGNORE NULLS.")
	return g.sql(e.Arg("this"))
}

// mysqlCurrentschemaSQL mirrors MySQLGenerator.currentschema_sql (@unsupported_args("this")).
func mysqlCurrentschemaSQL(g *Generator, e *Expr) string {
	if e.ArgB("this") {
		g.unsupported(fmt.Sprintf("Argument '%s' is not supported for expression '%s' when targeting %s.", "this", e.Kind().Name(), g.d.ClassName))
	}
	return g.fn("SCHEMA")
}

// mysqlPartitionSQL mirrors MySQLGenerator.partition_sql.
func mysqlPartitionSQL(g *Generator, e *Expr) string {
	parent := e.Parent()
	if parent.IsA(KPartitionByRangeProperty, KPartitionByListProperty) {
		return g.expressions(e, exprsOpts{flat: true})
	}
	return g.basePartitionSQL(e)
}

// mysqlPartitionBySQL mirrors MySQLGenerator._partition_by_sql.
func mysqlPartitionBySQL(g *Generator, e *Expr, kind string) string {
	partitions := g.expressions(e, exprsOpts{key: "partition_expressions", flat: true})
	create := g.expressions(e, exprsOpts{key: "create_expressions", flat: true})
	return "PARTITION BY " + kind + " (" + partitions + ") (" + create + ")"
}

// mysqlPartitionbyrangepropertySQL mirrors MySQLGenerator.partitionbyrangeproperty_sql.
func mysqlPartitionbyrangepropertySQL(g *Generator, e *Expr) string {
	return mysqlPartitionBySQL(g, e, "RANGE")
}

// mysqlPartitionbylistpropertySQL mirrors MySQLGenerator.partitionbylistproperty_sql.
func mysqlPartitionbylistpropertySQL(g *Generator, e *Expr) string {
	return mysqlPartitionBySQL(g, e, "LIST")
}

// mysqlPartitionlistSQL mirrors MySQLGenerator.partitionlist_sql.
func mysqlPartitionlistSQL(g *Generator, e *Expr) string {
	name := g.sqlKey(e, "this")
	values := g.expressions(e, exprsOpts{flat: true})
	return "PARTITION " + name + " VALUES IN (" + values + ")"
}

// mysqlPartitionrangeSQL mirrors MySQLGenerator.partitionrange_sql.
func mysqlPartitionrangeSQL(g *Generator, e *Expr) string {
	name := g.sqlKey(e, "this")
	values := g.expressions(e, exprsOpts{flat: true})
	return "PARTITION " + name + " VALUES LESS THAN (" + values + ")"
}
