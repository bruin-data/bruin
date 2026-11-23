package sqlengine

// Doris builds on MySQL; this file holds only the behavior that differs from it.

import (
	"strings"
	"unicode"
)

func init() {
	registerCustomizer("doris", customizeDoris)
	mysqlPartitionPropertyImpls["doris"] = dorisParsePartitionProperty
	mysqlPartitionRangeValueImpls["doris"] = dorisParsePartitionRangeValue
}

// dorisBuildDateTrunc builds TimestampTrunc from DATE_TRUNC(datetime, unit) or
// DATE_TRUNC(unit, datetime): a string literal without digits is taken as the unit.
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

	unit, this := a1, a0
	if isUnitLike(a0) {
		unit, this = a0, a1
	}

	return New(KTimestampTrunc, "this", this, "unit", unit)
}

// dorisLagLeadSQL renders LAG and LEAD with an explicit offset (default 1) and default value
// (default NULL).
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

func customizeDoris(d *Dialect) {
	P := d.P

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

	delete(P.FUNCTION_PARSERS, "GROUP_CONCAT")

	P.PROPERTY_PARSERS["PROPERTIES"] = noKwargs(func(p *Parser) any {
		return p.parseWrappedProperties()
	})
	P.PROPERTY_PARSERS["UNIQUE"] = noKwargs(func(p *Parser) any {
		return anyExpr(p.parseCompositeKeyProperty(KUniqueKeyProperty))
	})
	// Plain KEY without UNIQUE/DUPLICATE/AGGREGATE prefixes is parsed as a UniqueKeyProperty too
	P.PROPERTY_PARSERS["KEY"] = noKwargs(func(p *Parser) any {
		return anyExpr(p.parseCompositeKeyProperty(KUniqueKeyProperty))
	})
	P.PROPERTY_PARSERS["BUILD"] = noKwargs(func(p *Parser) any {
		return anyExpr(dorisParseBuildProperty(p))
	})
	P.PROPERTY_PARSERS["REFRESH"] = noKwargs(func(p *Parser) any {
		return anyExpr(dorisParseRefreshProperty(p))
	})

	P.h.parseRange = dorisParseRange

	customizeDorisGenerator(d)
}

// dorisMatchOperators are the full-text search operators.
// https://doris.apache.org/docs/sql-manual/basic-element/operators/conditional-operators/full-text-search-operators
var dorisMatchOperators = newStrSet(
	"MATCH",
	"MATCH_ALL",
	"MATCH_ANY",
	"MATCH_PHRASE",
	"MATCH_PHRASE_EDGE",
	"MATCH_PHRASE_PREFIX",
	"MATCH_REGEXP",
)

// dorisParseRange parses a full-text search predicate, x [NOT] MATCH[_<kind>] y [USING ANALYZER
// name], before the usual range predicates. The operators are recognized by their text, so one
// that has no operand after it (an implicit alias spelled like it) parses as before. The MySQL
// range parser handles the rest.
func dorisParseRange(p *Parser, this *Expr, parseIs bool) *Expr {
	if this == nil {
		this = p.parseBitwise()
	}
	index := p.index
	negate := p.match(TK_NOT)

	if p.curr.ok() && p.curr.Type == TK_VAR && dorisMatchOperators.Has(upperText(p.curr)) {
		p.advance(1)
		var kind any
		if _, k, _ := strings.Cut(upperText(p.prev), "_"); k != "" {
			kind = k
		}
		expression := p.parseBitwise()
		if expression == nil {
			p.retreat(index)
			return mysqlParseRange(p, this, parseIs)
		}

		var analyzer *Expr
		if p.matchTextSeq("USING", "ANALYZER") {
			analyzer = p.parseString()
			if analyzer == nil {
				analyzer = p.parseIdVar(false, nil)
			}
			if analyzer == nil {
				p.raiseError("Expected an analyzer name after USING ANALYZER", nil)
			}
		}

		this = p.expression(New(KMatch, "this", this, "expression", expression, "kind", kind, "analyzer", analyzer))
		if negate {
			this = p.expression(New(KNot, "this", this))
		}
	} else if negate {
		p.retreat(index)
	}

	return mysqlParseRange(p, this, parseIs)
}

// dorisParsePartitionProperty parses PARTITION BY: the MySQL forms, followed by an optional list
// of dynamic FROM ... TO ... INTERVAL partitions, or a plain PARTITION BY expression when the
// MySQL forms do not match.
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

// dorisParsePartitionRangeValue parses a range partition: the MySQL forms plus Doris's
// VALUES [(...), (...)).
func dorisParsePartitionRangeValue(p *Parser) *Expr {
	expr := mysqlParsePartitionRangeValue(p)

	if expr.IsA(KPartition) {
		return expr
	}

	p.matchTextSeq("VALUES")
	name := expr

	p.match(TK_L_BRACKET)
	values := parseCSVAny(p, func() []*Expr {
		return p.parseWrappedCSV(func() *Expr { return p.parseExpression() }, TK_COMMA, false)
	}, TK_COMMA, func([]*Expr) bool { return false })

	p.match(TK_R_BRACKET)
	p.match(TK_R_PAREN)

	// expressions holds a list of value lists
	valuesAny := make([]any, len(values))
	for i, v := range values {
		valuesAny[i] = v
	}
	partRange := p.expression(New(KPartitionRange, "this", name, "expressions", valuesAny))
	return p.expression(New(KPartition, "expressions", []*Expr{partRange}))
}

func dorisParseBuildProperty(p *Parser) *Expr {
	return p.expression(New(KBuildProperty, "this", p.parseVar(false, nil, true)))
}

// dorisParseRefreshProperty parses REFRESH <method> [ON MANUAL|COMMIT|SCHEDULE] [EVERY n unit]
// [STARTS 'timestamp']. Absent parts are stored as false, except unit, which is nil.
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
	G.methods[KMatch] = dorisMatchSQL
	G.methods[KPartitionedByProperty] = dorisPartitionedbypropertySQL
}

func dorisMatchSQL(g *Generator, e *Expr) string {
	op := "MATCH"
	if kind := e.Text("kind"); kind != "" {
		op += "_" + kind
	}
	sql := g.binary(e, op)
	if analyzer := g.sqlKey(e, "analyzer"); analyzer != "" {
		return sql + " USING ANALYZER " + analyzer
	}
	return sql
}

// dorisUniquekeypropertySQL renders KEY (...) in materialized view definitions and UNIQUE KEY (...)
// elsewhere.
func dorisUniquekeypropertySQL(g *Generator, e *Expr, prefix string) string {
	createStmt := e.FindAncestor(KCreate)
	if createStmt != nil {
		props := createStmt.ArgE("properties")
		if props == nil {
			// a CREATE without properties is rejected; the message keeps its historical wording
			panic(&ValueError{Msg: "'NoneType' object has no attribute 'find'"})
		}
		if props.Find(KMaterializedProperty) != nil {
			return g.baseUniquekeypropertySQL(e, "KEY")
		}
	}

	return g.baseUniquekeypropertySQL(e, "UNIQUE KEY")
}

// dorisPartitionrangeSQL renders a range partition: VALUES LESS THAN (v) for a single value and
// VALUES [(...), (...)) otherwise.
func dorisPartitionrangeSQL(g *Generator, e *Expr) string {
	name := g.sqlKey(e, "this")
	// expressions holds expressions or lists of expressions
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

func dorisPartitionbyrangepropertydynamicSQL(g *Generator, e *Expr) string {
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

func dorisPartitionedbypropertySQL(g *Generator, e *Expr) string {
	this := e.This()
	if this.IsA(KSchema) {
		return "PARTITION BY (" + g.expressions(this, exprsOpts{flat: true}) + ")"
	}
	return "PARTITION BY (" + g.sql(this) + ")"
}

// dorisTableSQL renders a table reference without AS before its alias, except when the nearest
// UPDATE, DELETE or SELECT ancestor is a SELECT.
func dorisTableSQL(g *Generator, e *Expr, sep string) string {
	ancestor := e.FindAncestor(KUpdate, KDelete, KSelect)
	if !ancestor.IsA(KSelect) {
		sep = " "
	}
	return g.baseTableSQL(e, sep)
}
