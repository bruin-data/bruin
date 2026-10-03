package sqlengine

// Port of sqlglot/parsers/spark.py (SparkParser) and sqlglot/generators/spark.py (SparkGenerator).

func init() { registerCustomizer("spark", customizeSpark) }

func customizeSpark(d *Dialect) {
	customizeSparkParser(d)
	customizeSparkGenerator(d)
}

// ---------------------------------------------------------------------------------------------
// Module-level builders (parsers/spark.py)
// ---------------------------------------------------------------------------------------------

// sparkBuildDatediff mirrors parsers.spark._build_datediff.
//
// Although Spark docs don't mention the "unit" argument, Spark3 added support for
// it at some point. Databricks also supports this variant.
func sparkBuildDatediff(args []*Expr, d *Dialect) *Expr {
	var unit *Expr
	this := seqGet(args, 0)
	expression := seqGet(args, 1)

	if len(args) == 3 {
		unit = VarChecked(this.Name())
		this = args[2]
	}

	return New(
		KDateDiff,
		"this", New(KTsOrDsToDate, "this", this),
		"expression", New(KTsOrDsToDate, "this", expression),
		"unit", unit,
	)
}

// sparkBuildDateadd mirrors parsers.spark._build_dateadd.
func sparkBuildDateadd(args []*Expr, d *Dialect) *Expr {
	expression := seqGet(args, 1)

	if len(args) == 2 {
		// DATE_ADD(startDate, numDays INTEGER)
		return New(KTsOrDsAdd, "this", seqGet(args, 0), "expression", expression, "unit", LiteralString("DAY"))
	}

	// DATE_ADD / DATEADD / TIMESTAMPADD(unit, value integer, expr)
	return New(KTimestampAdd, "this", seqGet(args, 2), "expression", expression, "unit", seqGet(args, 0))
}

// ---------------------------------------------------------------------------------------------
// SparkParser
// ---------------------------------------------------------------------------------------------

func customizeSparkParser(d *Dialect) {
	P := d.P

	P.SET_PARSERS["VAR"] = func(p *Parser) *Expr { return p.parseSetItemAssignment("VARIABLE") }
	P.SET_PARSERS["VARIABLE"] = func(p *Parser) *Expr { return p.parseSetItemAssignment("VARIABLE") }

	F := P.FUNCTIONS
	F["ANY_VALUE"] = hiveBuildWithIgnoreNulls(KAnyValue)
	F["ARRAY_INSERT"] = func(args []*Expr, d *Dialect) *Expr {
		return New(
			KArrayInsert,
			"this", seqGet(args, 0),
			"position", seqGet(args, 1),
			"expression", seqGet(args, 2),
			"offset", 1,
		)
	}
	F["BIT_AND"] = fromArgList(KBitwiseAndAgg)
	F["BIT_GET"] = fromArgList(KGetbit)
	F["BIT_OR"] = fromArgList(KBitwiseOrAgg)
	F["BIT_XOR"] = fromArgList(KBitwiseXorAgg)
	F["BIT_COUNT"] = fromArgList(KBitwiseCount)
	F["CURDATE"] = fromArgList(KCurrentDate)
	F["DATE_ADD"] = sparkBuildDateadd
	F["DATEADD"] = sparkBuildDateadd
	F["MAKE_TIMESTAMP"] = fromArgList(KTimestampFromParts)
	F["TIMESTAMPADD"] = sparkBuildDateadd
	F["TIMESTAMPDIFF"] = buildDateDelta(KTimestampDiff, nil, "DAY", false)
	F["TRY_ADD"] = fromArgList(KSafeAdd)
	F["TRY_DIVIDE"] = fromArgList(KSafeDivide)
	F["TRY_MULTIPLY"] = fromArgList(KSafeMultiply)
	F["TRY_SUBTRACT"] = fromArgList(KSafeSubtract)
	F["DATEDIFF"] = sparkBuildDatediff
	F["DATE_DIFF"] = sparkBuildDatediff
	F["JSON_OBJECT_KEYS"] = fromArgList(KJSONKeys)
	F["LISTAGG"] = fromArgList(KGroupConcat)
	F["TIMESTAMP_LTZ"] = spark2BuildAsCast("TIMESTAMP_LTZ")
	F["TIMESTAMP_NTZ"] = spark2BuildAsCast("TIMESTAMP_NTZ")
	F["TRY_ELEMENT_AT"] = func(args []*Expr, d *Dialect) *Expr {
		return New(
			KBracket,
			"this", seqGet(args, 0),
			"expressions", ensureList(seqGet(args, 1)),
			"offset", 1,
			"safe", true,
		)
	}
	F["LIKE"] = dialectBuildLike(KLike, false)
	F["ILIKE"] = dialectBuildLike(KILike, false)

	P.PLACEHOLDER_PARSERS[TK_L_BRACE] = sparkParseQueryParameter

	P.FUNCTION_PARSERS["SUBSTR"] = func(p *Parser) *Expr { return p.parseSubstring() }

	P.STATEMENT_PARSERS[TK_DECLARE] = func(p *Parser) *Expr { return p.parseDeclare() }

	P.h.parseGeneratedAsIdentity = sparkParseGeneratedAsIdentity
	P.h.parsePivotAggregation = sparkParsePivotAggregation
}

// sparkParseQueryParameter mirrors SparkParser._parse_query_parameter.
func sparkParseQueryParameter(p *Parser) *Expr {
	this := p.parseIdVar(true, nil)
	p.match(TK_R_BRACE)
	return p.expression(New(KPlaceholder, "this", this, "widget", true))
}

// sparkParseGeneratedAsIdentity mirrors SparkParser._parse_generated_as_identity.
func sparkParseGeneratedAsIdentity(p *Parser) *Expr {
	this := p.baseParseGeneratedAsIdentity()
	if this.Expression() != nil {
		return p.expression(New(KComputedColumnConstraint, "this", this.Expression()))
	}
	return this
}

// sparkParsePivotAggregation mirrors SparkParser._parse_pivot_aggregation.
//
// Spark 3+ and Databricks support non aggregate functions in PIVOT too, e.g
// PIVOT (..., 'foo' AS bar FOR col_to_pivot IN (...))
func sparkParsePivotAggregation(p *Parser) *Expr {
	aggregateExpr := p.parseFunction(nil, false, true, false)
	if aggregateExpr == nil {
		aggregateExpr = p.parseDisjunction()
	}
	return p.parseAlias(aggregateExpr, false)
}

// ---------------------------------------------------------------------------------------------
// Module-level helpers (generators/spark.py)
// ---------------------------------------------------------------------------------------------

// sparkNormalizePartition mirrors generators.spark._normalize_partition.
// Normalize the expressions in PARTITION BY (<expression>, <expression>, ...)
func sparkNormalizePartition(e *Expr) *Expr {
	if e.IsA(KLiteral) {
		return ToIdentifier(e.Name(), nil)
	}
	return e
}

// sparkDateaddSQL mirrors generators.spark._dateadd_sql.
func sparkDateaddSQL(g *Generator, e *Expr) string {
	if !e.ArgB("unit") || (e.IsA(KTsOrDsAdd) && pyUpper(e.Text("unit")) == "DAY") {
		// Coming from Hive/Spark2 DATE_ADD or roundtripping the 2-arg version of Spark3/DB
		return g.fn("DATE_ADD", e.Arg("this"), e.Arg("expression"))
	}

	this := g.fn("DATE_ADD", unitToVar(e, "DAY"), e.Arg("expression"), e.Arg("this"))

	if e.IsA(KTsOrDsAdd) {
		// The 3 arg version of DATE_ADD produces a timestamp in Spark3/DB but possibly not
		// in other dialects
		returnType := dhTsOrDsReturnType(e)
		if !dhIsType(returnType, DT_TIMESTAMP, DT_DATETIME) {
			this = "CAST(" + this + " AS " + exprSQL(returnType) + ")"
		}
	}

	return this
}

// sparkGroupconcatSQL mirrors generators.spark._groupconcat_sql.
func sparkGroupconcatSQL(g *Generator, e *Expr) string {
	if g.d.Version[0] < 4 {
		separator := e.ArgE("separator")
		if separator == nil {
			separator = LiteralString("")
		}
		expr := New(
			KArrayToString,
			"this", New(KArrayAgg, "this", e.This()),
			"expression", separator,
		)
		return g.sql(expr)
	}

	return groupconcatSQL(g, e, "LISTAGG", ",", true, false)
}

// ---------------------------------------------------------------------------------------------
// SparkGenerator
// ---------------------------------------------------------------------------------------------

func customizeSparkGenerator(d *Dialect) {
	T := d.G.TRANSFORMS

	T[KArrayConstructCompact] = func(g *Generator, e *Expr) string {
		return g.fn("ARRAY_COMPACT", g.fn("ARRAY", dhExprsToAny(e.Expressions())...))
	}
	T[KArrayInsert] = func(g *Generator, e *Expr) string {
		return g.fn("ARRAY_INSERT", e.Arg("this"), e.Arg("position"), e.Arg("expression"))
	}
	T[KArrayAppend] = arrayAppendSQL("ARRAY_APPEND", false)
	T[KArrayPrepend] = arrayAppendSQL("ARRAY_PREPEND", false)
	T[KBitwiseAndAgg] = renameFunc("BIT_AND")
	T[KBitwiseOrAgg] = renameFunc("BIT_OR")
	T[KBitwiseXorAgg] = renameFunc("BIT_XOR")
	T[KBitwiseCount] = renameFunc("BIT_COUNT")
	T[KCreate] = transformPreprocess([]func(*Expr) *Expr{
		transformRemoveUniqueConstraints,
		func(e *Expr) *Expr {
			return transformCtasWithTmpTablesToCreateTmpViewFull(e, spark2TemporaryStorageProvider)
		},
		transformMovePartitionedByToSchemaColumns,
	}, nil)
	T[KCurrentVersion] = renameFunc("VERSION")
	T[KDateFromUnixDate] = renameFunc("DATE_FROM_UNIX_DATE")
	T[KDatetimeAdd] = dateDeltaToBinaryIntervalOp(false)
	T[KDatetimeSub] = dateDeltaToBinaryIntervalOp(false)
	T[KGroupConcat] = sparkGroupconcatSQL
	T[KEndsWith] = renameFunc("ENDSWITH")
	T[KJSONKeys] = renameFunc("JSON_OBJECT_KEYS")
	T[KPartitionedByProperty] = func(g *Generator, e *Expr) string {
		sqls := []any{}
		for _, x := range e.This().Expressions() {
			sqls = append(sqls, sparkNormalizePartition(x))
		}
		return "PARTITIONED BY " + g.wrap(g.expressions(nil, exprsOpts{sqls: sqls, hasSqls: true, skipFirst: true}))
	}
	T[KSafeAdd] = renameFunc("TRY_ADD")
	T[KSafeDivide] = renameFunc("TRY_DIVIDE")
	T[KSafeMultiply] = renameFunc("TRY_MULTIPLY")
	T[KSafeSubtract] = renameFunc("TRY_SUBTRACT")
	T[KStartsWith] = renameFunc("STARTSWITH")
	T[KTimeAdd] = dateDeltaToBinaryIntervalOp(false)
	T[KTimeSub] = dateDeltaToBinaryIntervalOp(false)
	T[KTsOrDsAdd] = sparkDateaddSQL
	T[KTimestampAdd] = sparkDateaddSQL
	T[KTimestampFromParts] = renameFunc("MAKE_TIMESTAMP")
	T[KTimestampSub] = dateDeltaToBinaryIntervalOp(false)
	T[KDatetimeDiff] = timestampdiffSQL
	T[KTimestampDiff] = timestampdiffSQL
	T[KTryCast] = func(g *Generator, e *Expr) string {
		if e.ArgB("safe") {
			return g.trycastSQL(e)
		}
		return g.castSQL(e, "")
	}
	delete(T, KAnyValue)
	delete(T, KDateDiff)
	delete(T, KWith)

	M := d.G.methods
	M[KDateDiff] = sparkDatediffSQL
	M[KReadParquet] = sparkReadparquetSQL

	h := &d.G.h
	h.ignorenullsSQL = sparkIgnorenullsSQL
	h.bracketSQL = sparkBracketSQL
	h.computedcolumnconstraintSQL = sparkComputedcolumnconstraintSQL
	h.anyvalueSQL = sparkAnyvalueSQL
	h.placeholderSQL = sparkPlaceholderSQL
	h.ifblockSQL = sparkIfblockSQL
}

// sparkIgnorenullsSQL mirrors SparkGenerator.ignorenulls_sql.
func sparkIgnorenullsSQL(g *Generator, e *Expr) string {
	return g.baseIgnorenullsSQL(e)
}

// sparkBracketSQL mirrors SparkGenerator.bracket_sql.
func sparkBracketSQL(g *Generator, e *Expr) string {
	if e.ArgB("safe") {
		key := seqGet(g.bracketOffsetExpressions(e, 1), 0)
		return g.fn("TRY_ELEMENT_AT", e.Arg("this"), key)
	}

	return spark2BracketSQL(g, e)
}

// sparkComputedcolumnconstraintSQL mirrors SparkGenerator.computedcolumnconstraint_sql.
func sparkComputedcolumnconstraintSQL(g *Generator, e *Expr) string {
	return "GENERATED ALWAYS AS (" + g.sqlKey(e, "this") + ")"
}

// sparkAnyvalueSQL mirrors SparkGenerator.anyvalue_sql.
func sparkAnyvalueSQL(g *Generator, e *Expr) string {
	return g.functionFallbackSQL(e)
}

// sparkDatediffSQL mirrors SparkGenerator.datediff_sql.
func sparkDatediffSQL(g *Generator, e *Expr) string {
	end := g.sqlKey(e, "this")
	start := g.sqlKey(e, "expression")

	if e.ArgB("unit") {
		return g.fn("DATEDIFF", unitToVar(e, "DAY"), start, end)
	}

	return g.fn("DATEDIFF", end, start)
}

// sparkPlaceholderSQL mirrors SparkGenerator.placeholder_sql.
func sparkPlaceholderSQL(g *Generator, e *Expr) string {
	if !e.ArgB("widget") {
		return g.basePlaceholderSQL(e)
	}

	return "{" + e.Name() + "}"
}

// sparkReadparquetSQL mirrors SparkGenerator.readparquet_sql.
func sparkReadparquetSQL(g *Generator, e *Expr) string {
	if len(e.Expressions()) != 1 {
		g.unsupported("READ_PARQUET with multiple arguments is not supported")
		return ""
	}

	parquetFile := e.Expressions()[0]
	return "parquet.`" + parquetFile.Name() + "`"
}

// sparkIfblockSQL mirrors SparkGenerator.ifblock_sql.
func sparkIfblockSQL(g *Generator, e *Expr) string {
	condition := e.This()
	trueBlock := e.ArgE("true")

	var conditionExpr *Expr
	if condition.IsA(KNot) {
		inner := condition.This()
		if inner.IsA(KIs) && inner.Expression().IsA(KNull) {
			conditionExpr = inner.This()
		}
	}

	if conditionExpr.IsA(KObjectId) {
		objectType := conditionExpr.Expression()
		if objectType == nil || pyUpper(objectType.Name()) == "U" {
			if trueBlock.IsA(KBlock) {
				drop := trueBlock.Expressions()[0]
				if drop.IsA(KDrop) {
					drop.Set("exists", true)
					return g.sql(drop)
				}
			}
		}
	}

	return g.baseIfblockSQL(e)
}
