package sqlengine

import (
	"strings"
	"unicode"
)

func init() { registerCustomizer("databricks", customizeDatabricks) }

func customizeDatabricks(d *Dialect) {
	customizeDatabricksParser(d)
	customizeDatabricksGenerator(d)
}

func customizeDatabricksParser(d *Dialect) {
	P := d.P

	F := P.FUNCTIONS
	F["IFF"] = fromArgList(KIf)
	F["GETDATE"] = fromArgList(KCurrentTimestamp)
	F["DATEDIFF"] = buildDateDelta(KDateDiff, nil, "DAY", false)
	F["DATE_DIFF"] = buildDateDelta(KDateDiff, nil, "DAY", false)
	F["NOW"] = fromArgList(KCurrentTimestamp)
	F["TO_DATE"] = buildFormattedTime(KTsOrDsToDate, "", nil)
	F["UNIFORM"] = func(args []*Expr, d *Dialect) *Expr {
		return New(KUniform, "this", seqGet(args, 0), "expression", seqGet(args, 1), "seed", seqGet(args, 2))
	}

	P.NO_PAREN_FUNCTION_PARSERS["CURDATE"] = databricksParseCurdate

	P.STATEMENT_PARSERS[TK_OPTIMIZE] = databricksParseOptimize

	FP := P.FUNCTION_PARSERS
	FP["REGR_AVGX"] = func(p *Parser) *Expr { return p.parseDistinctArgFunction(KRegrAvgx, 1) }
	FP["REGR_AVGY"] = func(p *Parser) *Expr { return p.parseDistinctArgFunction(KRegrAvgy, 0) }
	FP["REGR_SXX"] = func(p *Parser) *Expr { return p.parseDistinctArgFunction(KRegrSxx, 1) }
	FP["REGR_SXY"] = func(p *Parser) *Expr { return p.parseDistinctArgFunction(KRegrSxy, 0) }
	FP["REGR_SYY"] = func(p *Parser) *Expr { return p.parseDistinctArgFunction(KRegrSyy, 1) }

	// `?::` is a non-strict cast and parses to TryCast. The Spark parser does not change
	// COLUMN_OPERATORS, so adding to the inherited table is enough.
	P.COLUMN_OPERATORS[TK_QDCOLON] = func(p *Parser, this, to *Expr) *Expr {
		return p.buildCast(false, "this", this, "to", to)
	}

	P.h.parsePrimaryKeyPart = databricksParsePrimaryKeyPart
	P.h.parseClusterProperty = databricksParseClusterProperty
}

func databricksParseCurdate(p *Parser) *Expr {
	// CURDATE, an alias for CURRENT_DATE, has optional parentheses
	if p.match(TK_L_PAREN) {
		p.matchRParen(nil)
	}
	return p.expression(New(KCurrentDate))
}

func databricksParsePrimaryKeyPart(p *Parser) *Expr {
	this := p.baseParsePrimaryKeyPart()
	if this != nil && p.matchTextSeq("TIMESERIES") {
		return p.expression(New(KTimeseriesKey, "this", this))
	}
	return this
}

func databricksParseClusterProperty(p *Parser) *Expr {
	if p.matchTexts("AUTO", "NONE") {
		return p.expression(New(KClusterProperty, "this", upperText(p.prev)))
	}
	return p.baseParseClusterProperty()
}

var databricksTimestampTypes = newDTypeSet(DT_DATETIME, DT_TIME, DT_TIMESTAMP, DT_TIMESTAMPNTZ, DT_TIMESTAMPLTZ, DT_TIMESTAMPTZ)

// databricksDropTimestampPrecision drops a timestamp precision Databricks rejects: it only
// supports 6 (microseconds) and 7 to 9 (nanoseconds), so e.g. TIMESTAMP(0) becomes TIMESTAMP.
func databricksDropTimestampPrecision(e *Expr) {
	exprs := e.Expressions()
	if !databricksTimestampTypes.Has(e.DTypeOf()) || len(exprs) == 0 {
		return
	}
	param := exprs[0]
	if param.IsA(KDataTypeParam) {
		param = param.This()
	}
	if param.IsInt() {
		if v, _ := param.toPyNumber(); v != nil {
			if p, _ := v.Int64(); p < 6 || p > 9 {
				e.Set("expressions", nil)
			}
		}
	}
}

func customizeDatabricksGenerator(d *Dialect) {
	T := d.G.TRANSFORMS

	sparkDatatypeSQL := d.G.h.datatypeSQL
	d.G.h.datatypeSQL = func(g *Generator, e *Expr) string {
		databricksDropTimestampPrecision(e)
		return sparkDatatypeSQL(g, e)
	}

	T[KCurrentVersion] = func(g *Generator, e *Expr) string { return "CURRENT_VERSION()" }
	T[KOptimize] = func(g *Generator, e *Expr) string { return "OPTIMIZE " + g.sqlKey(e, "this") }
	T[KDateAdd] = dateDeltaSQL("DATEADD", false)
	T[KDateDiff] = dateDeltaSQL("DATEDIFF", false)
	T[KDatetimeAdd] = func(g *Generator, e *Expr) string {
		return g.fn("TIMESTAMPADD", e.Arg("unit"), e.Arg("expression"), e.Arg("this"))
	}
	T[KDatetimeSub] = func(g *Generator, e *Expr) string {
		return g.fn(
			"TIMESTAMPADD",
			e.Arg("unit"),
			New(KMul, "this", e.Expression(), "expression", LiteralInt(-1)),
			e.Arg("this"),
		)
	}
	T[KDatetimeTrunc] = timestamptruncSQL("DATE_TRUNC", false)
	T[KGroupConcat] = func(g *Generator, e *Expr) string { return groupconcatSQL(g, e, "LISTAGG", ",", true, false) }
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		transformEliminateDistinctOn,
		transformUnnestToExplode,
		transformAnyToExists,
	}, nil)
	T[KJSONExtract] = func(g *Generator, e *Expr) string {
		return g.sqlKey(e, "this") + ":" + g.sqlKey(e, "expression")
	}
	T[KJSONPathRoot] = func(g *Generator, e *Expr) string {
		var grand *Expr
		if e.Parent() != nil {
			grand = e.Parent().Parent()
		}
		if grand.IsA(KJSONExtractScalar) {
			return "$"
		}
		return ""
	}
	T[KToChar] = func(g *Generator, e *Expr) string {
		if e.ArgB("is_numeric") {
			return g.castSQL(New(KCast, "this", e.This(), "to", New(KDataType, "this", "STRING")), "")
		}
		return g.functionFallbackSQL(e)
	}
	T[KCurrentCatalog] = func(g *Generator, e *Expr) string { return "CURRENT_CATALOG()" }
	delete(T, KRegexpLike)
	delete(T, KTryCast)
	T[KRegrAvgx] = databricksRegrSQL
	T[KRegrSxx] = databricksRegrSQL
	T[KRegrSyy] = databricksRegrSQL

	d.G.methods[KUniform] = databricksUniformSQL

	h := &d.G.h
	h.createSQL = databricksCreateSQL
	h.columndefSQL = databricksColumndefSQL
	h.timeserieskeySQL = databricksTimeserieskeySQL
	h.jsonpathSQL = databricksJsonpathSQL
	h.clusterpropertySQL = databricksClusterpropertySQL
}

func databricksCreateSQL(g *Generator, e *Expr) string {
	body := e.Expression()
	if body != nil && !body.IsA(KReturn) && e.KindText() == "FUNCTION" {
		isTable := false
		for p := range e.FindAll(KReturnsProperty) {
			if p.ArgB("is_table") {
				isTable = true
				break
			}
		}
		if isTable {
			e.Set("expression", New(KReturn, "this", body))
		}
	}
	return g.baseCreateSQL(e)
}

func databricksColumndefSQL(g *Generator, e *Expr, sep string) string {
	constraint := e.Find(KGeneratedAsIdentityColumnConstraint)
	kind := e.ArgE("kind")
	if constraint != nil && kind.IsA(KDataType) {
		if dt, ok := kind.Arg("this").(DType); ok && DataType_INTEGER_TYPES.Has(dt) {
			// only BIGINT generated identity constraints are supported
			e.Set("kind", NewDataType(DT_BIGINT))
		}
	}

	return hiveColumndefSQL(g, e, sep)
}

func databricksTimeserieskeySQL(g *Generator, e *Expr) string {
	return g.sqlKey(e, "this") + " TIMESERIES"
}

func databricksJsonpathSQL(g *Generator, e *Expr) string {
	path := g.baseJsonpathSQL(e)

	if e.Parent().IsA(KJSONExtractScalar) {
		return g.d.S.QUOTE_START + g.escapeStr(path, true, "", "", false) + g.d.S.QUOTE_END
	}

	return path
}

func databricksUniformSQL(g *Generator, e *Expr) string {
	gen := e.ArgE("gen")
	seed := e.Arg("seed")

	// From Snowflake UNIFORM(min, max, gen) as RANDOM(), RANDOM(seed), or constant value -> Extract seed
	// (the gen's "this" may be a raw string, e.g. for a literal gen; g.fn renders it verbatim)
	if gen != nil {
		seed = gen.Arg("this")
	}

	return g.fn("UNIFORM", e.Arg("this"), e.Arg("expression"), seed)
}

func databricksRegrSQL(g *Generator, e *Expr) string {
	name := e.Kind().SQLName()
	x := e.Expression()
	if x.IsA(KDistinct) {
		args := []any{New(KDistinct, "expressions", []*Expr{e.This()})}
		args = append(args, dhExprsToAny(x.Expressions())...)
		return g.fn(name, args...)
	}
	return g.fn(name, e.Arg("this"), x)
}

func databricksClusterpropertySQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	if this == "" {
		this = "(" + g.expressions(e, exprsOpts{flat: true}) + ")"
	}
	return "CLUSTER BY " + this
}

// databricksParseOptimize parses OPTIMIZE <table name>; any other form, e.g. one with a WHERE or
// ZORDER BY clause, falls back to a Command.
func databricksParseOptimize(p *Parser) *Expr {
	start := p.prev
	if n := p.tokensSize - p.index; n == 1 || n == 3 || n == 5 {
		tokens := p.tokens[p.index:p.tokensSize]
		isName := true
		for i, token := range tokens {
			if i%2 == 1 {
				isName = isName && token.Type == TK_DOT
			} else {
				// A name part that is not a quoted identifier can't contain whitespace
				isName = isName && p.s.ID_VAR_TOKENS.Has(token.Type) &&
					(token.Type == TK_IDENTIFIER || strings.IndexFunc(token.Text, unicode.IsSpace) < 0)
			}
		}
		if isName {
			return p.expression(New(KOptimize, "this", p.parseTableParts(true, false, false, false)))
		}
	}

	command := p.parseAsCommand(start)
	command.Set("this", upperText(start))
	return p.expression(command)
}
