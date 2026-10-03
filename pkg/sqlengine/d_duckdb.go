package sqlengine

// Port of sqlglot/dialects/duckdb.py and sqlglot/parsers/duckdb.py (sqlglot v30.13.0).
// The generator (sqlglot/generators/duckdb.py) lives in d_duckdb_gen*.go.
// Data-only class attributes live in the generated zz_*_settings.go files.

func init() { registerCustomizer("duckdb", customizeDuckDB) }

func customizeDuckDB(d *Dialect) {
	// ---- Dialect class ----
	setupDuckDBToJSONPath(d)

	// ---- DuckDBParser ----
	duckdbCustomizeParser(d)

	// ---- DuckDBGenerator ----
	duckdbCustomizeGenerator(d)
}

// duckdbVersionLT mirrors `dialect.version < (major, minor)` (3-tuple vs 2-tuple comparison).
func duckdbVersionLT(d *Dialect, major, minor int) bool {
	v := d.Version
	if v[0] != major {
		return v[0] < major
	}
	return v[1] < minor
}

// UnixToTime scale class constants (exp.UnixToTime.SECONDS / MILLIS / MICROS).
func duckdbUnixToTimeSeconds() *Expr { return LiteralInt(0) }
func duckdbUnixToTimeMillis() *Expr  { return LiteralInt(3) }
func duckdbUnixToTimeMicros() *Expr  { return LiteralInt(6) }

// ---------------------------------------------------------------------------------------------
// Module-level builders of sqlglot/parsers/duckdb.py
// ---------------------------------------------------------------------------------------------

func duckdbBuildSortArrayDesc(args []*Expr, _ *Dialect) *Expr {
	return New(KSortArray, "this", seqGet(args, 0), "asc", Boolean(false))
}

func duckdbBuildArrayPrepend(args []*Expr, _ *Dialect) *Expr {
	return New(KArrayPrepend, "this", seqGet(args, 1), "expression", seqGet(args, 0))
}

func duckdbBuildDateDiff(args []*Expr, _ *Dialect) *Expr {
	return New(KDateDiff, "this", seqGet(args, 2), "expression", seqGet(args, 1), "unit", seqGet(args, 0))
}

func duckdbBuildGenerateSeries(endExclusive bool) FuncBuilder {
	return func(args []*Expr, _ *Dialect) *Expr {
		// Check https://duckdb.org/docs/sql/functions/nested.html#range-functions
		if len(args) == 1 {
			// DuckDB uses 0 as a default for the series' start when it's omitted
			args = append([]*Expr{LiteralNumber("0")}, args...)
		}

		genSeries := FromArgList(KGenerateSeries, args)
		genSeries.Set("is_end_exclusive", endExclusive)

		// args.insert mutates the caller's list: validate arity against the mutated list.
		return WithValidateArgs(genSeries, args)
	}
}

func duckdbBuildMakeTimestamp(args []*Expr, _ *Dialect) *Expr {
	if len(args) == 1 {
		return New(KUnixToTime, "this", seqGet(args, 0), "scale", duckdbUnixToTimeMicros())
	}

	return New(
		KTimestampFromParts,
		"year", seqGet(args, 0),
		"month", seqGet(args, 1),
		"day", seqGet(args, 2),
		"hour", seqGet(args, 3),
		"min", seqGet(args, 4),
		"sec", seqGet(args, 5),
	)
}

func duckdbShowParser(this string) parseFn {
	return func(p *Parser) *Expr { return duckdbParseShowDuckDB(p, this) }
}

func duckdbConvertTextType(dtype *Expr) *Expr {
	dtype.Set("expressions", nil)
	return dtype
}

// ---------------------------------------------------------------------------------------------
// DuckDBParser
// ---------------------------------------------------------------------------------------------

func duckdbCustomizeParser(d *Dialect) {
	P := d.P

	// FUNCTIONS
	delete(P.FUNCTIONS, "DATE_SUB")
	delete(P.FUNCTIONS, "GLOB")
	F := P.FUNCTIONS
	F["ANY_VALUE"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KIgnoreNulls, "this", FromArgList(KAnyValue, args))
	}
	F["ARRAY_PREPEND"] = duckdbBuildArrayPrepend
	F["ARRAY_REVERSE_SORT"] = duckdbBuildSortArrayDesc
	F["ARRAY_INTERSECT"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KArrayIntersect, "expressions", args)
	}
	F["ARRAY_SORT"] = fromArgList(KSortArray)
	F["BIT_AND"] = fromArgList(KBitwiseAndAgg)
	F["BIT_OR"] = fromArgList(KBitwiseOrAgg)
	F["BIT_XOR"] = fromArgList(KBitwiseXorAgg)
	F["CURRENT_LOCALTIMESTAMP"] = fromArgList(KLocaltimestamp)
	F["DATEDIFF"] = duckdbBuildDateDiff
	F["DATE_DIFF"] = duckdbBuildDateDiff
	F["DATE_TRUNC"] = dateTruncToTime
	F["DATETRUNC"] = dateTruncToTime
	F["DECODE"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KDecode, "this", seqGet(args, 0), "charset", LiteralString("utf-8"))
	}
	F["EDITDIST3"] = fromArgList(KLevenshtein)
	F["ENCODE"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KEncode, "this", seqGet(args, 0), "charset", LiteralString("utf-8"))
	}
	F["EPOCH"] = fromArgList(KTimeToUnix)
	F["EPOCH_MS"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KUnixToTime, "this", seqGet(args, 0), "scale", duckdbUnixToTimeMillis())
	}
	F["GENERATE_SERIES"] = duckdbBuildGenerateSeries(false)
	F["GET_CURRENT_TIME"] = fromArgList(KCurrentTime)
	F["GET_BIT"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KGetbit, "this", seqGet(args, 0), "expression", seqGet(args, 1), "zero_is_msb", true)
	}
	F["JARO_WINKLER_SIMILARITY"] = fromArgList(KJarowinklerSimilarity)
	F["JSON"] = fromArgList(KParseJSON)
	F["JSON_ARRAY"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KJSONArray, "expressions", args)
	}
	F["JSON_EXTRACT_PATH"] = buildExtractJSONWithPath(KJSONExtract)
	F["JSON_EXTRACT_STRING"] = buildExtractJSONWithPath(KJSONExtractScalar)
	F["LIST"] = fromArgList(KArrayAgg)
	F["LIST_DISTINCT"] = fromArgList(KArrayDistinct)
	F["LIST_APPEND"] = fromArgList(KArrayAppend)
	F["LIST_CONCAT"] = buildArrayConcat
	F["LIST_CONTAINS"] = fromArgList(KArrayContains)
	F["LIST_COSINE_DISTANCE"] = fromArgList(KCosineDistance)
	F["LIST_DISTANCE"] = fromArgList(KEuclideanDistance)
	F["LIST_FILTER"] = fromArgList(KArrayFilter)
	F["LIST_HAS"] = fromArgList(KArrayContains)
	F["LIST_HAS_ANY"] = fromArgList(KArrayOverlaps)
	F["LIST_MAX"] = fromArgList(KArrayMax)
	F["LIST_MIN"] = fromArgList(KArrayMin)
	F["LIST_PREPEND"] = duckdbBuildArrayPrepend
	F["LIST_REVERSE_SORT"] = duckdbBuildSortArrayDesc
	F["LIST_SORT"] = fromArgList(KSortArray)
	F["LIST_TRANSFORM"] = fromArgList(KTransform)
	F["LIST_VALUE"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KArray, "expressions", args)
	}
	F["MAKE_DATE"] = fromArgList(KDateFromParts)
	F["MAKE_TIME"] = fromArgList(KTimeFromParts)
	F["MAKE_TIMESTAMP"] = duckdbBuildMakeTimestamp
	F["QUANTILE_CONT"] = fromArgList(KPercentileCont)
	F["QUANTILE_DISC"] = fromArgList(KPercentileDisc)
	F["RANGE"] = duckdbBuildGenerateSeries(true)
	F["REGEXP_EXTRACT"] = buildRegexpExtract(KRegexpExtract)
	F["REGEXP_EXTRACT_ALL"] = buildRegexpExtract(KRegexpExtractAll)
	F["REGEXP_MATCHES"] = fromArgList(KRegexpLike)
	F["REGEXP_REPLACE"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KRegexpReplace,
			"this", seqGet(args, 0),
			"expression", seqGet(args, 1),
			"replacement", seqGet(args, 2),
			"modifiers", seqGet(args, 3),
			"single_replace", true,
		)
	}
	F["SHA256"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KSHA2, "this", seqGet(args, 0), "length", LiteralInt(256))
	}
	F["STRFTIME"] = buildFormattedTime(KTimeToStr, "", nil)
	F["STRING_SPLIT"] = fromArgList(KSplit)
	F["STRING_SPLIT_REGEX"] = fromArgList(KRegexpSplit)
	F["STRING_TO_ARRAY"] = fromArgList(KSplit)
	F["STRPTIME"] = buildFormattedTime(KStrToTime, "", nil)
	F["STRUCT_PACK"] = fromArgList(KStruct)
	F["STR_SPLIT"] = fromArgList(KSplit)
	F["STR_SPLIT_REGEX"] = fromArgList(KRegexpSplit)
	F["TODAY"] = fromArgList(KCurrentDate)
	F["TIME_BUCKET"] = fromArgList(KDateBin)
	F["TO_TIMESTAMP"] = fromArgList(KUnixToTime)
	F["UNNEST"] = fromArgList(KExplode)
	F["VERSION"] = fromArgList(KCurrentVersion)
	F["XOR"] = binaryFromFunction(KBitwiseXor)

	// FUNCTION_PARSERS
	delete(P.FUNCTION_PARSERS, "DECODE")
	for _, name := range []string{"GROUP_CONCAT", "LISTAGG", "STRINGAGG"} {
		P.FUNCTION_PARSERS[name] = func(p *Parser) *Expr { return p.parseStringAgg() }
	}
	P.FUNCTION_PARSERS["APPROX_QUANTILE"] = func(p *Parser) *Expr {
		return p.parseDistinctArgFunction(KApproxQuantile, 0)
	}
	P.FUNCTION_PARSERS["QUANTILE"] = func(p *Parser) *Expr {
		return p.parseDistinctArgFunction(KQuantile, 0)
	}
	P.FUNCTION_PARSERS["QUANTILE_CONT"] = func(p *Parser) *Expr {
		return p.parseDistinctArgFunction(KPercentileCont, 0)
	}
	P.FUNCTION_PARSERS["QUANTILE_DISC"] = func(p *Parser) *Expr {
		return p.parseDistinctArgFunction(KPercentileDisc, 0)
	}

	// NO_PAREN_FUNCTION_PARSERS
	P.NO_PAREN_FUNCTION_PARSERS["MAP"] = duckdbParseMap
	P.NO_PAREN_FUNCTION_PARSERS["@"] = func(p *Parser) *Expr {
		return New(KAbs, "this", p.parseBitwise())
	}

	// PLACEHOLDER_PARSERS
	P.PLACEHOLDER_PARSERS[TK_PARAMETER] = func(p *Parser) *Expr {
		if p.match(TK_NUMBER) || p.matchSet(p.s.ID_VAR_TOKENS) {
			return p.expression(New(KPlaceholder, "this", p.prev.Text))
		}
		return nil
	}

	// RANGE_PARSERS
	P.RANGE_PARSERS[TK_DAMP] = binaryRangeParser(KArrayOverlaps, false)
	P.RANGE_PARSERS[TK_CARET_AT] = binaryRangeParser(KStartsWith, false)
	P.RANGE_PARSERS[TK_TILDE] = binaryRangeParser(KRegexpFullMatch, false)

	// TYPE_CONVERTERS is redefined (not merged with the parent's table).
	P.TYPE_CONVERTERS = map[DType]typeConverterFn{
		// https://duckdb.org/docs/sql/data_types/numeric
		DT_DECIMAL: buildDefaultDecimalType(18, 3),
		// https://duckdb.org/docs/sql/data_types/text
		DT_TEXT: duckdbConvertTextType,
	}

	// STATEMENT_PARSERS
	P.STATEMENT_PARSERS[TK_ATTACH] = func(p *Parser) *Expr { return duckdbParseAttachDetach(p, true) }
	P.STATEMENT_PARSERS[TK_DETACH] = func(p *Parser) *Expr { return duckdbParseAttachDetach(p, false) }
	P.STATEMENT_PARSERS[TK_FORCE] = duckdbParseForce
	P.STATEMENT_PARSERS[TK_INSTALL] = func(p *Parser) *Expr { return duckdbParseInstall(p, false) }
	P.STATEMENT_PARSERS[TK_SHOW] = func(p *Parser) *Expr { return p.parseShow() }

	// SET_PARSERS
	P.SET_PARSERS["VARIABLE"] = func(p *Parser) *Expr { return p.parseSetItemAssignment("VARIABLE") }

	// SHOW_PARSERS is redefined (not merged with the parent's table).
	P.SHOW_PARSERS = map[string]parseFn{
		"TABLES":     duckdbShowParser("TABLES"),
		"ALL TABLES": duckdbShowParser("ALL TABLES"),
	}

	// Method overrides
	P.h.parseFunctionProperties = duckdbParseFunctionProperties
	P.h.parseLambda = duckdbParseLambda
	P.h.parseExpression = duckdbParseExpression
	P.h.parseTable = duckdbParseTable
	P.h.parseTableSample = duckdbParseTableSample
	P.h.parseBracket = duckdbParseBracket
	P.h.parseStructTypes = duckdbParseStructTypes
	P.h.pivotColumnNames = duckdbPivotColumnNames
	P.h.parsePrimary = duckdbParsePrimary
}

func duckdbParseFunctionProperties(p *Parser) *Expr {
	if p.match(TK_TABLE) {
		return New(KProperties, "expressions", []*Expr{
			New(
				KReturnsProperty,
				"this", New(KSchema, "this", VarExpr("TABLE")),
				"is_table", true,
			),
		})
	}
	return p.baseParseFunctionProperties()
}

func duckdbParseLambda(p *Parser, alias bool) *Expr {
	index := p.index
	if !p.matchTextSeq("LAMBDA") {
		return p.baseParseLambda(alias)
	}

	expressions := p.parseCSV(p.parseLambdaArg, TK_COMMA)
	if !p.match(TK_COLON) {
		p.retreat(index)
		return nil
	}

	this := p.replaceLambda(p.parseAssignment(), expressions)
	return p.expression(New(KLambda, "this", this, "expressions", expressions, "colon", true))
}

func duckdbParseExpression(p *Parser) *Expr {
	// DuckDB supports prefix aliases, e.g. foo: 1
	if p.next.Type == TK_COLON {
		alias := p.parseIdVar(true, &p.s.ALIAS_TOKENS)
		p.match(TK_COLON)
		comments := append([]string{}, p.prevComments...)

		this := p.parseAssignment()
		if this != nil {
			// Moves the comment next to the alias in `alias: expr /* comment */`
			comments = append(comments, this.PopComments()...)
		}

		return p.expressionC(New(KAlias, "this", this, "alias", alias), comments)
	}

	return p.baseParseExpression()
}

func duckdbParseTable(p *Parser, schema bool, joins bool, aliasTokens *TokenSet, parseBracket bool, isDbReference bool, parsePartition bool, consumePipe bool) *Expr {
	// DuckDB supports prefix aliases, e.g. FROM foo: bar
	var alias *Expr
	var comments []string
	if p.next.Type == TK_COLON {
		tokens := aliasTokens
		if tokens == nil || *tokens == (TokenSet{}) {
			tokens = &p.s.TABLE_ALIAS_TOKENS
		}
		alias = p.parseTableAlias(tokens)
		p.match(TK_COLON)
		comments = append([]string{}, p.prevComments...)
	} else {
		alias = nil
		comments = []string{}
	}

	table := p.baseParseTable(schema, joins, aliasTokens, parseBracket, isDbReference, parsePartition, false)
	if table != nil && alias.IsA(KTableAlias) {
		// Moves the comment next to the alias in `alias: table /* comment */`
		comments = append(comments, table.PopComments()...)
		alias.Comments = append(alias.PopComments(), comments...)
		table.Set("alias", alias)
	}

	return table
}

func duckdbParseTableSample(p *Parser, asModifier bool) *Expr {
	// https://duckdb.org/docs/sql/samples.html
	sample := p.baseParseTableSample(asModifier)
	if sample != nil && !sample.ArgB("method") {
		if sample.ArgB("size") {
			sample.Set("method", VarExpr("RESERVOIR"))
		} else {
			sample.Set("method", VarExpr("SYSTEM"))
		}
	}

	return sample
}

func duckdbParseBracket(p *Parser, this *Expr) *Expr {
	bracket := p.baseParseBracket(this)

	if duckdbVersionLT(p.d, 1, 2) && bracket.IsA(KBracket) {
		// https://duckdb.org/2025/02/05/announcing-duckdb-120.html#breaking-changes
		bracket.Set("returns_list_for_maps", true)
	}

	return bracket
}

func duckdbParseMap(p *Parser) *Expr {
	if p.matchNoAdvance(TK_L_BRACE) {
		return p.expression(New(KToMap, "this", p.parseBracket(nil)))
	}

	args := p.parseWrappedCSV(p.parseAssignment, TK_COMMA, false)
	return p.expression(New(KMap, "keys", seqGet(args, 0), "values", seqGet(args, 1)))
}

func duckdbParseStructTypes(p *Parser, typeRequired bool) *Expr {
	return p.parseFieldDef()
}

func duckdbPivotColumnNames(p *Parser, aggregations []*Expr) []string {
	if len(aggregations) == 1 {
		return p.basePivotColumnNames(aggregations)
	}
	return pivotColumnNames(aggregations, MustDialect("duckdb"))
}

func duckdbParseAttachDetach(p *Parser, isAttach bool) *Expr {
	parseAttachOption := func() *Expr {
		this := p.parseVar(true, nil, false)
		expression := p.parseField(true, nil, false)
		return p.expression(New(KAttachOption, "this", this, "expression", expression))
	}

	p.match(TK_DATABASE)
	exists := p.parseExists(isAttach)
	this := p.parseAlias(p.parsePrimaryOrVar(), true)

	var expressions []*Expr
	if p.matchNoAdvance(TK_L_PAREN) {
		expressions = p.parseWrappedCSV(parseAttachOption, TK_COMMA, false)
	}

	if isAttach {
		var exprs any
		if expressions != nil {
			exprs = expressions
		}
		return p.expression(New(KAttach, "this", this, "exists", exists, "expressions", exprs))
	}
	return p.expression(New(KDetach, "this", this, "exists", exists))
}

func duckdbParseShowDuckDB(p *Parser, this string) *Expr {
	var from *Expr
	if p.match(TK_FROM) {
		from = p.parseTable(true, false, nil, false, false, false, false)
	}
	return p.expression(New(KShow, "this", this, "from_", from))
}

func duckdbParseForce(p *Parser) *Expr {
	// FORCE can only be followed by INSTALL or CHECKPOINT
	// In the case of CHECKPOINT, we fallback
	if !p.match(TK_INSTALL) {
		return p.parseAsCommand(p.prev)
	}

	return duckdbParseInstall(p, true)
}

func duckdbParseInstall(p *Parser, force bool) *Expr {
	this := p.parseIdVar(true, nil)
	var from *Expr
	if p.match(TK_FROM) {
		from = p.parseVarOrString(false)
	}
	return p.expression(New(KInstall, "this", this, "from_", from, "force", force))
}

func duckdbParsePrimary(p *Parser) *Expr {
	if p.matchPair(TK_HASH, TK_NUMBER) {
		return New(KPositionalColumn, "this", LiteralNumber(p.prev.Text))
	}

	return p.baseParsePrimary()
}
