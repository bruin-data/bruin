package sqlengine

// Port of sqlglot/dialects/doris.py (class Doris), sqlglot/parsers/doris.py (DorisParser) and
// sqlglot/generators/doris.py (DorisGenerator). Doris inherits from MySQL: only the differences
// from the MySQL classes are ported here.

import (
	"strings"
	"unicode"
)

func init() {
	registerCustomizer("doris", customizeDoris)
	mysqlPartitionPropertyImpls["doris"] = dorisParsePartitionProperty
	mysqlPartitionRangeValueImpls["doris"] = dorisParsePartitionRangeValue
}

// ---------------------------------------------------------------------------------------------
// Module-level helpers of sqlglot/parsers/doris.py and sqlglot/generators/doris.py
// ---------------------------------------------------------------------------------------------

// dorisBuildDateTrunc mirrors _build_date_trunc.
// Accept both DATE_TRUNC(datetime, unit) and DATE_TRUNC(unit, datetime).
func dorisBuildDateTrunc(args []*Expr, d *Dialect) *Expr {
	a0, a1 := seqGet(args, 0), seqGet(args, 1)

	isUnitLike := func(e *Expr) bool {
		if !(e.IsA(KLiteral) && e.IsString()) {
			return false
		}
		text := e.ThisS()
		for _, ch := range text {
			if unicode.IsDigit(ch) {
				return false
			}
		}
		return true
	}

	// Determine which argument is the unit
	unit, this := a1, a0
	if isUnitLike(a0) {
		unit, this = a0, a1
	}

	return New(KTimestampTrunc, "this", this, "unit", unit)
}

// dorisLagLeadSQL mirrors _lag_lead_sql.
func dorisLagLeadSQL(g *Generator, e *Expr) string {
	name := "LEAD"
	if e.IsA(KLag) {
		name = "LAG"
	}
	offset := e.ArgE("offset")
	if offset == nil {
		offset = LiteralInt(1)
	}
	def := e.ArgE("default")
	if def == nil {
		def = Null()
	}
	return g.fn(name, e.Arg("this"), offset, def)
}

// ---------------------------------------------------------------------------------------------
// Customizer
// ---------------------------------------------------------------------------------------------

func customizeDoris(d *Dialect) {
	P := d.P

	// FUNCTIONS
	P.FUNCTIONS["ADDDATE"] = buildDateDeltaWithInterval(KDateAdd, "DAY")
	P.FUNCTIONS["COLLECT_SET"] = fromArgList(KArrayUniqueAgg)
	P.FUNCTIONS["DATE_ADD"] = buildDateDeltaWithInterval(KDateAdd, "DAY")
	P.FUNCTIONS["DATE_SUB"] = buildDateDeltaWithInterval(KDateSub, "DAY")
	P.FUNCTIONS["DATE_TRUNC"] = dorisBuildDateTrunc
	P.FUNCTIONS["L2_DISTANCE"] = fromArgList(KEuclideanDistance)
	P.FUNCTIONS["MONTHS_ADD"] = fromArgList(KAddMonths)
	P.FUNCTIONS["REGEXP"] = fromArgList(KRegexpLike)
	P.FUNCTIONS["SUBDATE"] = buildDateDeltaWithInterval(KDateSub, "DAY")
	P.FUNCTIONS["TO_DATE"] = fromArgList(KTsOrDsToDate)

	// FUNCTION_PARSERS
	delete(P.FUNCTION_PARSERS, "GROUP_CONCAT")

	// PROPERTY_PARSERS
	P.PROPERTY_PARSERS["PROPERTIES"] = mysqlNoKwargs("DorisParser.<lambda>", func(p *Parser) any {
		return p.parseWrappedProperties()
	})
	P.PROPERTY_PARSERS["UNIQUE"] = mysqlNoKwargs("DorisParser.<lambda>", func(p *Parser) any {
		return anyExpr(p.parseCompositeKeyProperty(KUniqueKeyProperty))
	})
	// Plain KEY without UNIQUE/DUPLICATE/AGGREGATE prefixes should be treated as UniqueKeyProperty with unique=False
	P.PROPERTY_PARSERS["KEY"] = mysqlNoKwargs("DorisParser.<lambda>", func(p *Parser) any {
		return anyExpr(p.parseCompositeKeyProperty(KUniqueKeyProperty))
	})
	P.PROPERTY_PARSERS["BUILD"] = mysqlNoKwargs("DorisParser.<lambda>", func(p *Parser) any {
		return anyExpr(dorisParseBuildProperty(p))
	})
	P.PROPERTY_PARSERS["REFRESH"] = mysqlNoKwargs("DorisParser.<lambda>", func(p *Parser) any {
		return anyExpr(dorisParseRefreshProperty(p))
	})

	customizeDorisGenerator(d)
}

// ---------------------------------------------------------------------------------------------
// DorisParser methods
// ---------------------------------------------------------------------------------------------

// dorisParsePartitionProperty mirrors DorisParser._parse_partition_property.
func dorisParsePartitionProperty(p *Parser) any {
	expr := mysqlParsePartitionProperty(p)

	if !truthy(expr) {
		return anyExpr(p.parsePartitionedBy())
	}

	if e, ok := expr.(*Expr); ok && e.IsA(KProperty) {
		return e
	}

	p.matchLParen(nil)

	var createExpressions any
	if p.matchTextSeqNoAdvance("FROM") {
		createExpressions = p.parseCSV(func() *Expr { return dorisParsePartitioningGranularityDynamic(p) }, TK_COMMA)
	}

	p.matchRParen(nil)

	return p.expression(New(
		KPartitionByRangeProperty,
		"partition_expressions", expr,
		"create_expressions", createExpressions,
	))
}

// dorisParsePartitioningGranularityDynamic mirrors DorisParser._parse_partitioning_granularity_dynamic.
func dorisParsePartitioningGranularityDynamic(p *Parser) *Expr {
	p.matchTextSeq("FROM")
	start := p.parseWrapped(func() *Expr { return p.parseString() }, false)
	p.matchTextSeq("TO")
	end := p.parseWrapped(func() *Expr { return p.parseString() }, false)
	p.matchTextSeq("INTERVAL")
	number := p.parseNumber()
	unit := p.parseVar(true, nil, false)
	every := p.expression(New(KInterval, "this", number, "unit", unit))
	return p.expression(New(KPartitionByRangePropertyDynamic, "start", start, "end", end, "every", every))
}

// dorisParsePartitionRangeValue mirrors DorisParser._parse_partition_range_value.
func dorisParsePartitionRangeValue(p *Parser) *Expr {
	expr := mysqlParsePartitionRangeValue(p)

	if expr.IsA(KPartition) {
		return expr
	}

	p.matchTextSeq("VALUES")
	name := expr

	// Doris-specific bracket syntax: VALUES [(...), (...))
	p.match(TK_L_BRACKET)
	values := parseCSVAny(p, func() []*Expr {
		return p.parseWrappedCSV(func() *Expr { return p.parseExpression() }, TK_COMMA, false)
	}, TK_COMMA, func([]*Expr) bool { return false })

	p.match(TK_R_BRACKET)
	p.match(TK_R_PAREN)

	// A list of lists, like in Python
	valuesAny := make([]any, len(values))
	for i, v := range values {
		valuesAny[i] = v
	}
	partRange := p.expression(New(KPartitionRange, "this", name, "expressions", valuesAny))
	return p.expression(New(KPartition, "expressions", []*Expr{partRange}))
}

// dorisParseBuildProperty mirrors DorisParser._parse_build_property.
func dorisParseBuildProperty(p *Parser) *Expr {
	return p.expression(New(KBuildProperty, "this", p.parseVar(false, nil, true)))
}

// dorisParseRefreshProperty mirrors DorisParser._parse_refresh_property.
func dorisParseRefreshProperty(p *Parser) *Expr {
	method := p.parseVar(false, nil, true)

	p.match(TK_ON)

	var kind any = false
	if p.matchTexts("MANUAL", "COMMIT", "SCHEDULE") {
		kind = upperText(p.prev)
	}
	var every any = false
	if p.matchTextSeq("EVERY") {
		every = anyExpr(p.parseNumber())
	}
	var unit any
	if truthy(every) {
		unit = anyExpr(p.parseVar(true, nil, false))
	}
	var starts any = false
	if p.matchTextSeq("STARTS") {
		starts = anyExpr(p.parseString())
	}

	return p.expression(New(
		KRefreshTriggerProperty,
		"method", method,
		"kind", kind,
		"every", every,
		"unit", unit,
		"starts", starts,
	))
}

// ---------------------------------------------------------------------------------------------
// DorisGenerator
// ---------------------------------------------------------------------------------------------

func customizeDorisGenerator(d *Dialect) {
	G := d.G
	T := G.TRANSFORMS

	T[KAddMonths] = renameFunc("MONTHS_ADD")
	T[KApproxDistinct] = approxCountDistinctSQL
	T[KArgMax] = renameFunc("MAX_BY")
	T[KArgMin] = renameFunc("MIN_BY")
	T[KArrayAgg] = renameFunc("COLLECT_LIST")
	T[KArrayToString] = renameFunc("ARRAY_JOIN")
	T[KArrayUniqueAgg] = renameFunc("COLLECT_SET")
	T[KCurrentDate] = func(g *Generator, e *Expr) string { return g.fn("CURRENT_DATE") }
	T[KCurrentTimestamp] = func(g *Generator, e *Expr) string { return g.fn("NOW") }
	T[KDateTrunc] = func(g *Generator, e *Expr) string { return g.fn("DATE_TRUNC", e.Arg("this"), unitToStr(e, "DAY")) }
	T[KEuclideanDistance] = renameFunc("L2_DISTANCE")
	T[KGroupConcat] = func(g *Generator, e *Expr) string {
		sep := e.ArgE("separator")
		if sep == nil {
			sep = LiteralString(",")
		}
		return g.fn("GROUP_CONCAT", e.Arg("this"), sep)
	}
	T[KJSONExtractScalar] = func(g *Generator, e *Expr) string { return g.fn("JSON_EXTRACT", e.Arg("this"), e.Arg("expression")) }
	T[KLag] = dorisLagLeadSQL
	T[KLead] = dorisLagLeadSQL
	T[KMap] = renameFunc("ARRAY_MAP")
	T[KProperty] = propertySQL
	T[KRegexpLike] = renameFunc("REGEXP")
	T[KRegexpSplit] = renameFunc("SPLIT_BY_STRING")
	T[KSchemaCommentProperty] = func(g *Generator, e *Expr) string { return g.nakedProperty(e) }
	T[KSplit] = renameFunc("SPLIT_BY_STRING")
	T[KStringToArray] = renameFunc("SPLIT_BY_STRING")
	T[KStrToUnix] = func(g *Generator, e *Expr) string {
		return g.fn("UNIX_TIMESTAMP", e.Arg("this"), mysqlFormatTimeArg(g, e))
	}
	T[KTimeStrToDate] = renameFunc("TO_DATE")
	T[KTsOrDsAdd] = func(g *Generator, e *Expr) string { return g.fn("DATE_ADD", e.Arg("this"), e.Arg("expression")) }
	T[KTsOrDsToDate] = func(g *Generator, e *Expr) string { return g.fn("TO_DATE", e.Arg("this")) }
	T[KTimeToUnix] = renameFunc("UNIX_TIMESTAMP")
	T[KTimestampTrunc] = func(g *Generator, e *Expr) string {
		return g.fn("DATE_TRUNC", e.Arg("this"), unitToStr(e, "DAY"))
	}
	T[KUnixToStr] = func(g *Generator, e *Expr) string {
		return g.fn("FROM_UNIXTIME", e.Arg("this"), timeFormat("doris")(g, e))
	}
	T[KUnixToTime] = renameFunc("FROM_UNIXTIME")

	G.h.uniquekeypropertySQL = dorisUniquekeypropertySQL
	G.h.partitionrangeSQL = dorisPartitionrangeSQL
	G.h.partitionbyrangepropertydynamicSQL = dorisPartitionbyrangepropertydynamicSQL
	G.h.tableSQL = dorisTableSQL
	G.methods[KPartitionedByProperty] = dorisPartitionedbypropertySQL
}

// dorisUniquekeypropertySQL mirrors DorisGenerator.uniquekeyproperty_sql.
func dorisUniquekeypropertySQL(g *Generator, e *Expr, prefix string) string {
	createStmt := e.FindAncestor(KCreate)
	if createStmt != nil {
		props := createStmt.ArgE("properties")
		if props == nil {
			// create_stmt.args["properties"].find(...) on None raises AttributeError
			panic(&ValueError{Msg: "'NoneType' object has no attribute 'find'"})
		}
		if props.Find(KMaterializedProperty) != nil {
			return g.baseUniquekeypropertySQL(e, "KEY")
		}
	}

	return g.baseUniquekeypropertySQL(e, "UNIQUE KEY")
}

// dorisPartitionrangeSQL mirrors DorisGenerator.partitionrange_sql.
func dorisPartitionrangeSQL(g *Generator, e *Expr) string {
	name := g.sqlKey(e, "this")
	// values = expression.expressions (may be a list of lists)
	var values []any
	switch v := e.Arg("expressions").(type) {
	case []*Expr:
		for _, x := range v {
			values = append(values, x)
		}
	case []any:
		values = v
	}

	if len(values) != 1 {
		// Multiple values: use VALUES [ ... )
		var parts []string
		if len(values) > 0 {
			if _, isList := values[0].([]*Expr); isList {
				for _, inner := range values {
					var innerSQL []string
					for _, v := range inner.([]*Expr) {
						innerSQL = append(innerSQL, g.sql(v))
					}
					parts = append(parts, "("+strings.Join(innerSQL, ", ")+")")
				}
			} else {
				for _, v := range values {
					parts = append(parts, "("+g.sql(v)+")")
				}
			}
		}

		return "PARTITION " + name + " VALUES [" + strings.Join(parts, ", ") + ")"
	}

	return "PARTITION " + name + " VALUES LESS THAN (" + g.sql(values[0]) + ")"
}

// dorisPartitionbyrangepropertydynamicSQL mirrors DorisGenerator.partitionbyrangepropertydynamic_sql.
func dorisPartitionbyrangepropertydynamicSQL(g *Generator, e *Expr) string {
	// Generates: FROM ("start") TO ("end") INTERVAL N UNIT
	start := g.sqlKey(e, "start")
	end := g.sqlKey(e, "end")
	every := e.ArgE("every")

	interval := ""
	if every != nil {
		number := g.sqlKey(every, "this")
		interval = "INTERVAL " + number + " " + g.sqlKey(every, "unit")
	}

	return "FROM (" + start + ") TO (" + end + ") " + interval
}

// dorisPartitionedbypropertySQL mirrors DorisGenerator.partitionedbyproperty_sql.
func dorisPartitionedbypropertySQL(g *Generator, e *Expr) string {
	this := e.This()
	if this.IsA(KSchema) {
		return "PARTITION BY (" + g.expressions(this, exprsOpts{flat: true}) + ")"
	}
	return "PARTITION BY (" + g.sql(this) + ")"
}

// dorisTableSQL mirrors DorisGenerator.table_sql.
// Override table_sql to avoid AS keyword in UPDATE and DELETE statements.
func dorisTableSQL(g *Generator, e *Expr, sep string) string {
	ancestor := e.FindAncestor(KUpdate, KDelete, KSelect)
	if !ancestor.IsA(KSelect) {
		sep = " "
	}
	return g.baseTableSQL(e, sep)
}
