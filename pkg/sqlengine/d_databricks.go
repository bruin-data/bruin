package sqlengine

// Port of sqlglot/parsers/databricks.py (DatabricksParser) and sqlglot/generators/databricks.py
// (DatabricksGenerator).

func init() { registerCustomizer("databricks", customizeDatabricks) }

func customizeDatabricks(d *Dialect) {
	customizeDatabricksParser(d)
	customizeDatabricksGenerator(d)
}

// ---------------------------------------------------------------------------------------------
// DatabricksParser
// ---------------------------------------------------------------------------------------------

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

	FP := P.FUNCTION_PARSERS
	FP["REGR_AVGX"] = func(p *Parser) *Expr { return p.parseDistinctArgFunction(KRegrAvgx, 1) }
	FP["REGR_AVGY"] = func(p *Parser) *Expr { return p.parseDistinctArgFunction(KRegrAvgy, 0) }
	FP["REGR_SXX"] = func(p *Parser) *Expr { return p.parseDistinctArgFunction(KRegrSxx, 1) }
	FP["REGR_SXY"] = func(p *Parser) *Expr { return p.parseDistinctArgFunction(KRegrSxy, 0) }
	FP["REGR_SYY"] = func(p *Parser) *Expr { return p.parseDistinctArgFunction(KRegrSyy, 1) }

	// COLUMN_OPERATORS = {**parser.Parser.COLUMN_OPERATORS, QDCOLON: ...}; SparkParser does not
	// change COLUMN_OPERATORS, so editing the inherited table is equivalent.
	P.COLUMN_OPERATORS[TK_QDCOLON] = func(p *Parser, this, to *Expr) *Expr {
		return p.buildCast(false, "this", this, "to", to)
	}

	P.h.parsePrimaryKeyPart = databricksParsePrimaryKeyPart
	P.h.parseClusterProperty = databricksParseClusterProperty
}

// databricksParseCurdate mirrors DatabricksParser._parse_curdate.
func databricksParseCurdate(p *Parser) *Expr {
	// CURDATE, an alias for CURRENT_DATE, has optional parentheses
	if p.match(TK_L_PAREN) {
		p.matchRParen(nil)
	}
	return p.expression(New(KCurrentDate))
}

// databricksParsePrimaryKeyPart mirrors DatabricksParser._parse_primary_key_part.
func databricksParsePrimaryKeyPart(p *Parser) *Expr {
	this := p.baseParsePrimaryKeyPart()
	if this != nil && p.matchTextSeq("TIMESERIES") {
		return p.expression(New(KTimeseriesKey, "this", this))
	}
	return this
}

// databricksParseClusterProperty mirrors DatabricksParser._parse_cluster_property.
func databricksParseClusterProperty(p *Parser) *Expr {
	if p.matchTexts("AUTO", "NONE") {
		return p.expression(New(KClusterProperty, "this", upperText(p.prev)))
	}
	return p.baseParseClusterProperty()
}

// ---------------------------------------------------------------------------------------------
// DatabricksGenerator
// ---------------------------------------------------------------------------------------------

func customizeDatabricksGenerator(d *Dialect) {
	T := d.G.TRANSFORMS

	T[KCurrentVersion] = func(g *Generator, e *Expr) string { return "CURRENT_VERSION()" }
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

// databricksCreateSQL mirrors DatabricksGenerator.create_sql.
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

// databricksColumndefSQL mirrors DatabricksGenerator.columndef_sql.
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

// databricksTimeserieskeySQL mirrors DatabricksGenerator.timeserieskey_sql.
func databricksTimeserieskeySQL(g *Generator, e *Expr) string {
	return g.sqlKey(e, "this") + " TIMESERIES"
}

// databricksJsonpathSQL mirrors DatabricksGenerator.jsonpath_sql.
func databricksJsonpathSQL(g *Generator, e *Expr) string {
	e.Set("escape", nil)
	path := g.baseJsonpathSQL(e)

	if e.Parent().IsA(KJSONExtractScalar) {
		return g.d.S.QUOTE_START + path + g.d.S.QUOTE_END
	}

	return path
}

// databricksUniformSQL mirrors DatabricksGenerator.uniform_sql.
func databricksUniformSQL(g *Generator, e *Expr) string {
	gen := e.ArgE("gen")
	seed := e.Arg("seed")

	// From Snowflake UNIFORM(min, max, gen) as RANDOM(), RANDOM(seed), or constant value -> Extract seed
	// (gen.this may be a raw string, e.g. for a Literal gen; Generator.func renders it verbatim)
	if gen != nil {
		seed = gen.Arg("this")
	}

	return g.fn("UNIFORM", e.Arg("this"), e.Arg("expression"), seed)
}

// databricksRegrSQL mirrors DatabricksGenerator._regr_sql.
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

// databricksClusterpropertySQL mirrors DatabricksGenerator.clusterproperty_sql.
func databricksClusterpropertySQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	if this == "" {
		this = "(" + g.expressions(e, exprsOpts{flat: true}) + ")"
	}
	return "CLUSTER BY " + this
}
