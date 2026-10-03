package sqlengine

// Port of sqlglot/parsers/spark2.py (Spark2Parser) and sqlglot/generators/spark2.py (Spark2Generator).

import "sync"

func init() { registerCustomizer("spark2", customizeSpark2) }

func customizeSpark2(d *Dialect) {
	customizeSpark2Parser(d)
	customizeSpark2Generator(d)
}

// ---------------------------------------------------------------------------------------------
// Module-level builders (parsers/spark2.py)
// ---------------------------------------------------------------------------------------------

var spark2DataTypeCache sync.Map // string -> *Expr

// spark2DataTypeFromStr mirrors exp.DataType.from_str(to_type) (default dialect), returning a fresh copy.
func spark2DataTypeFromStr(toType string) *Expr {
	if v, ok := spark2DataTypeCache.Load(toType); ok {
		return v.(*Expr).Copy()
	}
	dt := dataTypeFromStr(toType, nil, false)
	spark2DataTypeCache.Store(toType, dt.Copy())
	return dt
}

// spark2BuildAsCast mirrors parsers.spark2.build_as_cast.
func spark2BuildAsCast(toType string) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		return New(KCast, "this", seqGet(args, 0), "to", spark2DataTypeFromStr(toType))
	}
}

// ---------------------------------------------------------------------------------------------
// Spark2Parser
// ---------------------------------------------------------------------------------------------

func customizeSpark2Parser(d *Dialect) {
	P := d.P

	F := P.FUNCTIONS
	F["AGGREGATE"] = fromArgList(KReduce)
	F["BOOLEAN"] = spark2BuildAsCast("boolean")
	F["DATE"] = spark2BuildAsCast("date")
	F["DATE_TRUNC"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KTimestampTrunc, "this", seqGet(args, 1), "unit", VarOf(seqGet(args, 0)))
	}
	F["DAYOFMONTH"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KDayOfMonth, "this", New(KTsOrDsToDate, "this", seqGet(args, 0)))
	}
	F["DAYOFWEEK"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KDayOfWeek, "this", New(KTsOrDsToDate, "this", seqGet(args, 0)))
	}
	F["DAYOFYEAR"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KDayOfYear, "this", New(KTsOrDsToDate, "this", seqGet(args, 0)))
	}
	F["DOUBLE"] = spark2BuildAsCast("double")
	F["ELEMENT_AT"] = func(args []*Expr, d *Dialect) *Expr {
		return New(
			KBracket,
			"this", seqGet(args, 0),
			"expressions", ensureList(seqGet(args, 1)),
			"offset", 1,
			"safe", false,
		)
	}
	F["FLOAT"] = spark2BuildAsCast("float")
	F["FORMAT_STRING"] = fromArgList(KFormat)
	F["FROM_UTC_TIMESTAMP"] = func(args []*Expr, d *Dialect) *Expr {
		this := seqGet(args, 0)
		if this == nil {
			this = New(KVar, "this", "")
		}
		return New(
			KAtTimeZone,
			"this", CastExpr(this, DT_TIMESTAMP, true, d),
			"zone", seqGet(args, 1),
		)
	}
	F["LTRIM"] = func(args []*Expr, d *Dialect) *Expr { return buildTrim(args, true, true) }
	F["INT"] = spark2BuildAsCast("int")
	F["MAP_FROM_ARRAYS"] = fromArgList(KMap)
	F["RLIKE"] = fromArgList(KRegexpLike)
	F["RTRIM"] = func(args []*Expr, d *Dialect) *Expr { return buildTrim(args, false, true) }
	F["SHIFTLEFT"] = binaryFromFunction(KBitwiseLeftShift)
	F["SHIFTRIGHT"] = binaryFromFunction(KBitwiseRightShift)
	F["STRING"] = spark2BuildAsCast("string")
	F["SLICE"] = fromArgList(KArraySlice)
	F["TIMESTAMP"] = spark2BuildAsCast("timestamp")
	F["TO_TIMESTAMP"] = func(args []*Expr, d *Dialect) *Expr {
		if len(args) == 1 {
			return spark2BuildAsCast("timestamp")(args, d)
		}
		return buildFormattedTime(KStrToTime, "", nil)(args, d)
	}
	F["TO_UNIX_TIMESTAMP"] = fromArgList(KStrToUnix)
	F["TO_UTC_TIMESTAMP"] = func(args []*Expr, d *Dialect) *Expr {
		this := seqGet(args, 0)
		if this == nil {
			this = New(KVar, "this", "")
		}
		return New(
			KFromTimeZone,
			"this", CastExpr(this, DT_TIMESTAMP, true, d),
			"zone", seqGet(args, 1),
		)
	}
	F["TRUNC"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KDateTrunc, "unit", seqGet(args, 1), "this", seqGet(args, 0))
	}
	F["WEEKOFYEAR"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KWeekOfYear, "this", New(KTsOrDsToDate, "this", seqGet(args, 0)))
	}

	FP := P.FUNCTION_PARSERS
	FP["APPROX_PERCENTILE"] = func(p *Parser) *Expr { return p.parseDistinctArgFunction(KApproxQuantile, 0) }
	for _, hint := range []string{
		"BROADCAST", "BROADCASTJOIN", "MAPJOIN", "MERGE", "SHUFFLEMERGE", "MERGEJOIN",
		"SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL",
	} {
		hint := hint
		FP[hint] = func(p *Parser) *Expr { return p.parseJoinHint(hint) }
	}

	P.h.parseDropColumn = spark2ParseDropColumn
	P.h.pivotColumnNames = spark2PivotColumnNames
}

// spark2ParseDropColumn mirrors Spark2Parser._parse_drop_column.
func spark2ParseDropColumn(p *Parser) *Expr {
	if p.matchTextSeq("DROP", "COLUMNS") {
		return p.expression(New(KDrop, "this", p.parseSchema(nil), "kind", "COLUMNS"))
	}
	return nil
}

// spark2PivotColumnNames mirrors Spark2Parser._pivot_column_names.
func spark2PivotColumnNames(p *Parser, aggregations []*Expr) []string {
	if len(aggregations) == 1 {
		return []string{}
	}
	return pivotColumnNames(aggregations, MustDialect("spark"))
}

// ---------------------------------------------------------------------------------------------
// Module-level helpers (generators/spark2.py)
// ---------------------------------------------------------------------------------------------

// spark2JSONFormatSQL mirrors generators.spark2._json_format_sql.
func spark2JSONFormatSQL(g *Generator, e *Expr) string {
	this := e.This()

	if isParseJSON(this) {
		if this.This().IsString() {
			// Since FROM_JSON requires a nested type, we always wrap the json string with
			// an array to ensure that "naked" strings like "'a'" will be handled correctly
			wrappedJSON := LiteralString("[" + this.This().Name() + "]")

			fromJSON := g.fn("FROM_JSON", wrappedJSON, g.fn("SCHEMA_OF_JSON", wrappedJSON))
			toJSON := g.fn("TO_JSON", fromJSON)

			// This strips the [, ] delimiters of the dummy array printed by TO_JSON
			return g.fn("REGEXP_EXTRACT", toJSON, "'^.(.*).$'", "1")
		}
		return g.sql(this)
	}

	return g.fn("TO_JSON", this, e.Arg("options"))
}

// spark2MapSQL mirrors generators.spark2._map_sql.
func spark2MapSQL(g *Generator, e *Expr) string {
	keys := e.Arg("keys")
	values := e.Arg("values")

	if !truthy(keys) || !truthy(values) {
		return g.fn("MAP")
	}

	return g.fn("MAP_FROM_ARRAYS", keys, values)
}

// spark2StrToDate mirrors generators.spark2._str_to_date.
func spark2StrToDate(g *Generator, e *Expr) string {
	timeFormat := g.formatTime(e, nil, nil)
	if timeFormat == HIVE_DATE_FORMAT {
		return g.fn("TO_DATE", e.Arg("this"))
	}
	return g.fn("TO_DATE", e.Arg("this"), hiveNilIfEmpty(timeFormat))
}

// spark2UnixToTimeSQL mirrors generators.spark2._unix_to_time_sql.
func spark2UnixToTimeSQL(g *Generator, e *Expr) string {
	scale := e.ArgE("scale")
	timestamp := e.This()

	if scale == nil {
		return g.sql(CastExpr(dhFunc("from_unixtime", timestamp), DT_TIMESTAMP, true, nil))
	}
	if scale.Equal(LiteralInt(0)) {
		return g.fn("TIMESTAMP_SECONDS", timestamp)
	}
	if scale.Equal(LiteralInt(3)) {
		return g.fn("TIMESTAMP_MILLIS", timestamp)
	}
	if scale.Equal(LiteralInt(6)) {
		return g.fn("TIMESTAMP_MICROS", timestamp)
	}

	unixSeconds := New(KDiv, "this", timestamp, "expression", dhFunc("POW", 10, scale))
	return g.fn("TIMESTAMP_SECONDS", unixSeconds)
}

// spark2UnaliasPivot mirrors generators.spark2._unalias_pivot.
//
// Spark doesn't allow PIVOT aliases, so we need to remove them and possibly wrap a
// pivoted source in a subquery with the same alias to preserve the query's semantics.
func spark2UnaliasPivot(expression *Expr) *Expr {
	if expression.IsA(KFrom) && len(expression.This().ArgL("pivots")) > 0 {
		pivot := expression.This().ArgL("pivots")[0]
		if pivot.Alias() != "" {
			alias := pivot.ArgE("alias").Pop()
			sel := tfmSelectFrom(tfmSelect([]any{"*"}, true), expression.This().Copy(), false)
			return New(
				KFrom,
				"this", expression.This().Replace(tfmSubquery(sel, alias, false)),
			)
		}
	}

	return expression
}

// spark2UnqualifyPivotColumns mirrors generators.spark2._unqualify_pivot_columns.
//
// Spark doesn't allow the column referenced in the PIVOT's field to be qualified,
// so we need to unqualify it.
func spark2UnqualifyPivotColumns(expression *Expr) *Expr {
	if expression.IsA(KPivot) {
		fields := []*Expr{}
		for _, field := range expression.ArgL("fields") {
			fields = append(fields, transformUnqualifyColumns(field))
		}
		expression.Set("fields", fields)
	}

	return expression
}

// spark2TemporaryStorageProvider mirrors generators.spark2.temporary_storage_provider.
// spark2, spark, Databricks require a storage provider for temporary tables
func spark2TemporaryStorageProvider(expression *Expr) *Expr {
	provider := New(KFileFormatProperty, "this", LiteralString("parquet"))
	expression.ArgE("properties").Append("expressions", provider)
	return expression
}

// ---------------------------------------------------------------------------------------------
// Spark2Generator
// ---------------------------------------------------------------------------------------------

func customizeSpark2Generator(d *Dialect) {
	T := d.G.TRANSFORMS

	T[KApproxDistinct] = renameFunc("APPROX_COUNT_DISTINCT")
	T[KArraySum] = func(g *Generator, e *Expr) string {
		return "AGGREGATE(" + g.sqlKey(e, "this") + ", 0, (acc, x) -> acc + x, acc -> acc)"
	}
	T[KArrayToString] = renameFunc("ARRAY_JOIN")
	T[KArraySlice] = renameFunc("SLICE")
	T[KAtTimeZone] = func(g *Generator, e *Expr) string {
		return g.fn("FROM_UTC_TIMESTAMP", e.Arg("this"), e.Arg("zone"))
	}
	T[KBitwiseLeftShift] = renameFunc("SHIFTLEFT")
	T[KBitwiseRightShift] = renameFunc("SHIFTRIGHT")
	T[KCreate] = transformPreprocess([]func(*Expr) *Expr{
		transformRemoveUniqueConstraints,
		func(e *Expr) *Expr {
			return transformCtasWithTmpTablesToCreateTmpViewFull(e, spark2TemporaryStorageProvider)
		},
		transformMoveSchemaColumnsToPartitionedBy,
	}, nil)
	T[KDateFromParts] = renameFunc("MAKE_DATE")
	T[KDateTrunc] = func(g *Generator, e *Expr) string { return g.fn("TRUNC", e.Arg("this"), unitToStr(e, "DAY")) }
	T[KDayOfMonth] = renameFunc("DAYOFMONTH")
	T[KDayOfWeek] = renameFunc("DAYOFWEEK")
	// (DAY_OF_WEEK(datetime) % 7) + 1 is equivalent to DAYOFWEEK_ISO(datetime)
	T[KDayOfWeekIso] = func(g *Generator, e *Expr) string {
		return "((" + g.fn("DAYOFWEEK", e.Arg("this")) + " % 7) + 1)"
	}
	T[KDayOfYear] = renameFunc("DAYOFYEAR")
	T[KFormat] = renameFunc("FORMAT_STRING")
	T[KFrom] = transformPreprocess([]func(*Expr) *Expr{spark2UnaliasPivot}, nil)
	T[KFromTimeZone] = func(g *Generator, e *Expr) string {
		return g.fn("TO_UTC_TIMESTAMP", e.Arg("this"), e.Arg("zone"))
	}
	T[KJSONFormat] = spark2JSONFormatSQL
	T[KLogicalAnd] = renameFunc("BOOL_AND")
	T[KLogicalOr] = renameFunc("BOOL_OR")
	T[KMap] = spark2MapSQL
	T[KPivot] = transformPreprocess([]func(*Expr) *Expr{spark2UnqualifyPivotColumns}, nil)
	T[KReduce] = renameFunc("AGGREGATE")
	T[KRegexpReplace] = func(g *Generator, e *Expr) string {
		if !e.HasArgKey("replacement") {
			// e.args["replacement"] raises KeyError
			panic(&ValueError{Msg: "'replacement'"})
		}
		return g.fn("REGEXP_REPLACE", e.Arg("this"), e.Arg("expression"), e.Arg("replacement"), e.Arg("position"))
	}
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		transformEliminateQualify,
		transformEliminateDistinctOn,
		transformUnnestToExplode,
		transformAnyToExists,
	}, nil)
	T[KSHA2Digest] = func(g *Generator, e *Expr) string {
		var length any = e.ArgE("length")
		if !truthy(length) {
			length = LiteralInt(256)
		}
		return g.fn("SHA2", e.Arg("this"), length)
	}
	T[KStrToDate] = spark2StrToDate
	T[KStrToTime] = func(g *Generator, e *Expr) string {
		return g.fn("TO_TIMESTAMP", e.Arg("this"), hiveNilIfEmpty(g.formatTime(e, nil, nil)))
	}
	T[KTimestampTrunc] = func(g *Generator, e *Expr) string { return g.fn("DATE_TRUNC", unitToStr(e, "DAY"), e.Arg("this")) }
	T[KUnixToTime] = spark2UnixToTimeSQL
	T[KVariancePop] = renameFunc("VAR_POP")
	T[KWeekOfYear] = renameFunc("WEEKOFYEAR")
	T[KWithinGroup] = transformPreprocess([]func(*Expr) *Expr{transformRemoveWithinGroupForPercentiles}, nil)
	delete(T, KArraySort)
	delete(T, KILike)
	delete(T, KLeft)
	delete(T, KMonthsBetween)
	delete(T, KRight)

	d.G.methods[KFileFormatProperty] = spark2FileformatpropertySQL

	h := &d.G.h
	h.structSQL = spark2StructSQL
	h.castSQL = spark2CastSQL
	h.altercolumnSQL = spark2AltercolumnSQL
	h.renamecolumnSQL = spark2RenamecolumnSQL
	h.bracketSQL = spark2BracketSQL
}

// spark2StructSQL mirrors Spark2Generator.struct_sql.
func spark2StructSQL(g *Generator, e *Expr) string {
	return g.baseStructSQL(e)
}

// spark2CastSQL mirrors Spark2Generator.cast_sql.
func spark2CastSQL(g *Generator, e *Expr, safePrefix string) string {
	arg := e.This()
	isJSONExtract := arg.IsA(KJSONExtract, KJSONExtractScalar) && !arg.ArgB("variant_extract")

	// We can't use a non-nested type (eg. STRING) as a schema
	if e.ArgE("to").ArgB("nested") && (isParseJSON(arg) || isJSONExtract) {
		schema := "'" + g.sqlKey(e, "to") + "'"
		target := arg
		if !isJSONExtract {
			target = arg.This()
		}
		return g.fn("FROM_JSON", target, schema)
	}

	if isParseJSON(e) {
		return g.fn("TO_JSON", arg)
	}

	return g.baseCastSQL(e, safePrefix)
}

// spark2FileformatpropertySQL mirrors Spark2Generator.fileformatproperty_sql.
func spark2FileformatpropertySQL(g *Generator, e *Expr) string {
	if e.ArgB("hive_format") {
		return hiveFileformatpropertySQL(g, e)
	}

	return "USING " + pyUpper(e.Name())
}

// spark2AltercolumnSQL mirrors Spark2Generator.altercolumn_sql.
func spark2AltercolumnSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	newName := g.sqlKey(e, "rename_to")
	if newName == "" {
		newName = this
	}
	comment := g.sqlKey(e, "comment")
	if newName == this {
		if comment != "" {
			return "ALTER COLUMN " + this + " COMMENT " + comment
		}
		return g.baseAltercolumnSQL(e)
	}
	return "RENAME COLUMN " + this + " TO " + newName
}

// spark2RenamecolumnSQL mirrors Spark2Generator.renamecolumn_sql.
func spark2RenamecolumnSQL(g *Generator, e *Expr) string {
	return g.baseRenamecolumnSQL(e)
}

// spark2BracketSQL mirrors Spark2Generator.bracket_sql.
func spark2BracketSQL(g *Generator, e *Expr) string {
	if v, ok := e.Arg("safe").(bool); ok && !v {
		return bracketToElementAtSQL(g, e)
	}

	return g.baseBracketSQL(e)
}
