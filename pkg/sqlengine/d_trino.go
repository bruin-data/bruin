package sqlengine

// Port of sqlglot/dialects/trino.py, sqlglot/parsers/trino.py and sqlglot/generators/trino.py
// (sqlglot v30.13.0). Trino inherits from Presto (d_presto*.go). Data-only class attributes
// live in the generated zz_*_settings.go files.

func init() { registerCustomizer("trino", customizeTrino) }

func customizeTrino(d *Dialect) {
	// ---- TrinoParser ----
	P := d.P

	P.FUNCTIONS["VERSION"] = fromArgList(KCurrentVersion)

	P.FUNCTION_PARSERS["TRIM"] = func(p *Parser) *Expr { return p.parseTrim() }
	P.FUNCTION_PARSERS["JSON_QUERY"] = trinoParseJSONQuery
	P.FUNCTION_PARSERS["JSON_VALUE"] = func(p *Parser) *Expr { return p.parseJsonValue() }
	P.FUNCTION_PARSERS["LISTAGG"] = func(p *Parser) *Expr { return p.parseStringAgg() }

	// ---- TrinoGenerator ----
	G := d.G

	T := G.TRANSFORMS
	T[KArraySum] = func(g *Generator, e *Expr) string {
		return "REDUCE(" + g.sqlKey(e, "this") + ", 0, (acc, x) -> acc + x, acc -> acc)"
	}
	T[KArrayUniqueAgg] = func(g *Generator, e *Expr) string {
		return "ARRAY_AGG(DISTINCT " + g.sqlKey(e, "this") + ")"
	}
	T[KCurrentVersion] = renameFunc("VERSION")
	T[KFromISO8601TimestampNanos] = renameFunc("FROM_ISO8601_TIMESTAMP_NANOS")
	T[KGroupConcat] = func(g *Generator, e *Expr) string {
		return groupconcatSQL(g, e, "LISTAGG", ",", true, true)
	}
	T[KLocationProperty] = func(g *Generator, e *Expr) string { return g.propertySQL(e) }
	T[KMerge] = mergeWithoutTargetSQL
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		transformEliminateQualify,
		transformEliminateDistinctOn,
		transformExplodeProjectionToUnnest(1),
		transformEliminateSemiAndAntiJoins,
		prestoAmendExplodedColumnTable,
	}, nil)
	T[KTimeStrToTime] = func(g *Generator, e *Expr) string { return timestrtotimeSQL(g, e, true) }
	T[KTrim] = func(g *Generator, e *Expr) string { return trimSQL(g, e, "") }

	G.methods[KJSONExtract] = trinoJSONExtractSQL
}

// trinoParseJSONQueryQuote mirrors TrinoParser._parse_json_query_quote.
func trinoParseJSONQueryQuote(p *Parser) *Expr {
	if !(p.matchTextSeq("KEEP", "QUOTES") || p.matchTextSeq("OMIT", "QUOTES")) {
		return nil
	}

	option := pyUpper(p.tokens[p.index-2].Text)
	return p.expression(New(
		KJSONExtractQuote,
		"option", option,
		"scalar", p.matchTextSeq("ON", "SCALAR", "STRING"),
	))
}

// trinoParseJSONQuery mirrors TrinoParser._parse_json_query.
func trinoParseJSONQuery(p *Parser) *Expr {
	this := p.parseBitwise()
	var expression any = false
	if p.match(TK_COMMA) {
		expression = p.parseBitwise()
	}
	option := p.parseVarFromOptions(p.s.JSON_QUERY_OPTIONS, false)
	quote := trinoParseJSONQueryQuote(p)
	onCondition := p.parseOnCondition()
	return p.expression(New(
		KJSONExtract,
		"this", this,
		"expression", expression,
		"option", option,
		"json_query", true,
		"quote", quote,
		"on_condition", onCondition,
	))
}

// trinoJSONExtractSQL mirrors TrinoGenerator.jsonextract_sql.
func trinoJSONExtractSQL(g *Generator, e *Expr) string {
	if !e.ArgB("json_query") {
		return prestoJSONExtractSQL(g, e)
	}

	jsonPath := g.sqlKey(e, "expression")
	option := g.sqlKey(e, "option")
	if option != "" {
		option = " " + option
	}

	quote := g.sqlKey(e, "quote")
	if quote != "" {
		quote = " " + quote
	}

	onCondition := g.sqlKey(e, "on_condition")
	if onCondition != "" {
		onCondition = " " + onCondition
	}

	return g.fn(
		"JSON_QUERY",
		e.Arg("this"),
		jsonPath+option+quote+onCondition,
	)
}
