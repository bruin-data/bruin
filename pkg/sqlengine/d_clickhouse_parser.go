package sqlengine

import "sync"

func clickhouseBuildDatetimeFormat(kind Kind) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		expr := buildFormattedTime(kind, "", nil)(args, d)

		timezone := seqGet(args, 2)
		if timezone != nil {
			expr.Set("zone", timezone)
		}

		return expr
	}
}

func clickhouseBuildCountIf(args []*Expr, _ *Dialect) *Expr {
	if len(args) == 1 {
		return New(KCountIf, "this", seqGet(args, 0))
	}

	return New(KCombinedAggFunc, "this", "countIf", "expressions", args)
}

func clickhouseBuildStrToDate(args []*Expr, _ *Dialect) *Expr {
	if len(args) == 3 {
		return New(KAnonymous, "this", "STR_TO_DATE", "expressions", args)
	}

	strtodate := FromArgList(KStrToDate, args)
	return CastExpr(strtodate, NewDataType(DT_DATETIME), true, nil)
}

func clickhouseBuildTimestampTrunc(unit string) FuncBuilder {
	return func(args []*Expr, _ *Dialect) *Expr {
		return New(KTimestampTrunc, "this", seqGet(args, 0), "unit", VarExpr(unit), "zone", seqGet(args, 1))
	}
}

func clickhouseBuildSplitByChar(args []*Expr, d *Dialect) *Expr {
	sep := seqGet(args, 0)
	if sep.IsA(KLiteral) {
		// only a string literal qualifies; numeric literals keep the anonymous call
		if sep.IsString() && len(sep.ThisS()) == 1 {
			return clickhouseBuildSplit(KSplit)(args, d)
		}
	}

	return New(KAnonymous, "this", "splitByChar", "expressions", args)
}

// clickhouseBuildSplit returns a builder for kind from ClickHouse's argument order:
// (separator, string[, limit]).
func clickhouseBuildSplit(kind Kind) FuncBuilder {
	return func(args []*Expr, _ *Dialect) *Expr {
		return New(kind, "this", seqGet(args, 1), "expression", seqGet(args, 0), "limit", seqGet(args, 2))
	}
}

// clickhouseBuildArrayLambdaFunc returns a builder for kind from ClickHouse's argument order
// (lambda, array). A call over several arrays, such as arrayMap((x, y) -> x + y, arr1, arr2),
// has no counterpart in other dialects and stays an anonymous call to name.
func clickhouseBuildArrayLambdaFunc(kind Kind, name string) FuncBuilder {
	return func(args []*Expr, _ *Dialect) *Expr {
		if len(args) > 2 {
			return New(KAnonymous, "this", name, "expressions", args)
		}

		return New(kind, "this", seqGet(args, 1), "expression", seqGet(args, 0))
	}
}

// clickhouseTimestampTruncUnits omits WEEK: toStartOfWeek takes an extra mode argument for the
// first day of the week.
var clickhouseTimestampTruncUnits = []string{
	"MICROSECOND",
	"MILLISECOND",
	"SECOND",
	"MINUTE",
	"HOUR",
	"DAY",
	"MONTH",
	"QUARTER",
	"YEAR",
}

type clickhouseAggParts struct {
	name   string
	suffix string // "" means no suffix
}

var (
	clickhouseAggFuncMappingOnce sync.Once
	clickhouseAggFuncMapping     map[string]clickhouseAggParts
	clickhouseAggSuffixes        []string
)

// clickhouseAggFuncMappingGet returns, built once, a map from every aggregate function name with
// zero or one combinator suffix to its parts, and the combinator suffixes (AGG_FUNCTIONS_SUFFIXES,
// sorted longest first).
func clickhouseAggFuncMappingGet() (map[string]clickhouseAggParts, []string) {
	clickhouseAggFuncMappingOnce.Do(func() {
		data := parserSettings_clickhouse()
		clickhouseAggSuffixes = data.AGG_FUNCTIONS_SUFFIXES
		m := map[string]clickhouseAggParts{}
		for _, sfx := range clickhouseAggSuffixes {
			for f := range data.AGG_FUNCTIONS {
				m[f+sfx] = clickhouseAggParts{f, sfx}
			}
		}
		for f := range data.AGG_FUNCTIONS {
			m[f] = clickhouseAggParts{f, ""}
		}
		clickhouseAggFuncMapping = m
	})
	return clickhouseAggFuncMapping, clickhouseAggSuffixes
}

// clickhouseResolveAgg splits an aggregate function name into its base name and its combinator
// suffixes (outermost last). ok is false when the name is not an aggregate function.
func clickhouseResolveAgg(name string) (string, []string, bool) {
	// ClickHouse allows chaining multiple combinators on aggregate functions.
	// See https://clickhouse.com/docs/sql-reference/aggregate-functions/combinators
	// N.B. this resolution allows any suffix stack, including ones that ClickHouse rejects
	// syntactically such as sumMergeMerge (due to repeated adjacent suffixes)
	mapping, suffixes := clickhouseAggFuncMappingGet()

	// Until we are able to identify a 1- or 0-suffix aggregate function by name,
	// repeatedly strip and queue suffixes (checking longer suffixes first). This loop only
	// runs for 2 or more suffixes, as the mapping already holds every 0- and 1-suffix name.
	var accumulated []string
	var parts clickhouseAggParts
	for {
		var ok bool
		parts, ok = mapping[name]
		if ok {
			break
		}
		found := false
		for _, suffix := range suffixes {
			if len(name) >= len(suffix) && name[len(name)-len(suffix):] == suffix && len([]rune(name)) != len([]rune(suffix)) {
				accumulated = append([]string{suffix}, accumulated...)
				name = name[:len(name)-len(suffix)]
				found = true
				break
			}
		}
		if !found {
			return "", nil, false
		}
	}

	if parts.suffix != "" {
		// this is a 1-suffix aggregate (either naturally or via repeated suffix
		// stripping). prepend the innermost suffix.
		accumulated = append([]string{parts.suffix}, accumulated...)
	}

	return parts.name, accumulated, true
}

func customizeClickHouseParser(d *Dialect) {
	s := d.P

	delete(s.FUNCTIONS, "TRANSFORM")
	delete(s.FUNCTIONS, "APPROX_TOP_SUM")
	regexpExtract := func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KRegexpExtract,
			"this", seqGet(args, 0),
			"expression", seqGet(args, 1),
			"group", seqGet(args, 2),
		)
	}
	for _, name := range []string{"REGEXPEXTRACT", "REGEXP_EXTRACT", "REGEXP_SUBSTR"} {
		s.FUNCTIONS[name] = regexpExtract
	}
	for _, unit := range clickhouseTimestampTruncUnits {
		s.FUNCTIONS["TOSTARTOF"+unit] = clickhouseBuildTimestampTrunc(unit)
	}
	for name, fn := range map[string]FuncBuilder{
		"ANY":               fromArgList(KAnyValue),
		"ARRAYCOMPACT":      fromArgList(KArrayCompact),
		"ARRAYCONCAT":       fromArgList(KArrayConcat),
		"ARRAYDISTINCT":     fromArgList(KArrayDistinct),
		"ARRAYEXCEPT":       fromArgList(KArrayExcept),
		"ARRAYSUM":          fromArgList(KArraySum),
		"ARRAYMAX":          fromArgList(KArrayMax),
		"ARRAYMIN":          fromArgList(KArrayMin),
		"ARRAYREVERSE":      fromArgList(KArrayReverse),
		"ARRAYSLICE":        fromArgList(KArraySlice),
		"ARRAYFILTER":       clickhouseBuildArrayLambdaFunc(KArrayFilter, "arrayFilter"),
		"ARRAYMAP":          clickhouseBuildArrayLambdaFunc(KTransform, "arrayMap"),
		"CURRENTDATABASE":   fromArgList(KCurrentDatabase),
		"CURRENTSCHEMAS":    fromArgList(KCurrentSchemas),
		"COUNTIF":           clickhouseBuildCountIf,
		"CITYHASH64":        fromArgList(KCityHash64),
		"COSINEDISTANCE":    fromArgList(KCosineDistance),
		"VERSION":           fromArgList(KCurrentVersion),
		"DATE_ADD":          buildDateDelta(KDateAdd, nil, "", false),
		"DATEADD":           buildDateDelta(KDateAdd, nil, "", false),
		"DATE_DIFF":         buildDateDelta(KDateDiff, nil, "", true),
		"DATEDIFF":          buildDateDelta(KDateDiff, nil, "", true),
		"DATE_FORMAT":       clickhouseBuildDatetimeFormat(KTimeToStr),
		"DATE_SUB":          buildDateDelta(KDateSub, nil, "", false),
		"DATESUB":           buildDateDelta(KDateSub, nil, "", false),
		"DATETRUNC":         fromArgList(KDateTrunc),
		"FORMATDATETIME":    clickhouseBuildDatetimeFormat(KTimeToStr),
		"HAS":               fromArgList(KArrayContains),
		"ILIKE":             dialectBuildLike(KILike, false),
		"JSONEXTRACTRAW":    buildJSONExtractPath(KJSONExtract, false, true, ""),
		"JSONEXTRACTSTRING": buildJSONExtractPath(KJSONExtractScalar, false, true, ""),
		"LENGTH": func(args []*Expr, _ *Dialect) *Expr {
			return New(KLength, "this", seqGet(args, 0), "binary", true)
		},
		"LIKE":          dialectBuildLike(KLike, false),
		"L2Distance":    fromArgList(KEuclideanDistance),
		"MAP":           func(args []*Expr, _ *Dialect) *Expr { return buildVarMap(args) },
		"MATCH":         fromArgList(KRegexpLike),
		"NOTLIKE":       dialectBuildLike(KLike, true),
		"PARSEDATETIME": clickhouseBuildDatetimeFormat(KParseDatetime),
		"RANDCANONICAL": fromArgList(KRand),
		"STR_TO_DATE":   clickhouseBuildStrToDate,
		"TIMESTAMP_SUB": buildDateDelta(KTimestampSub, nil, "", false),
		"TIMESTAMPSUB":  buildDateDelta(KTimestampSub, nil, "", false),
		"TIMESTAMP_ADD": buildDateDelta(KTimestampAdd, nil, "", false),
		"TIMESTAMPADD":  buildDateDelta(KTimestampAdd, nil, "", false),
		"TOMONDAY":      clickhouseBuildTimestampTrunc("WEEK"),
		"UNIQ":          fromArgList(KApproxDistinct),
		"MD5":           fromArgList(KMD5Digest),
		"SHA256": func(args []*Expr, _ *Dialect) *Expr {
			return New(KSHA2, "this", seqGet(args, 0), "length", LiteralInt(256))
		},
		"SHA512": func(args []*Expr, _ *Dialect) *Expr {
			return New(KSHA2, "this", seqGet(args, 0), "length", LiteralInt(512))
		},
		"SPLITBYCHAR":           clickhouseBuildSplitByChar,
		"SPLITBYREGEXP":         clickhouseBuildSplit(KRegexpSplit),
		"SPLITBYSTRING":         clickhouseBuildSplit(KSplit),
		"SUBSTRINGINDEX":        fromArgList(KSubstringIndex),
		"TOTYPENAME":            fromArgList(KTypeof),
		"TRIMBOTH":              fromArgList(KTrim),
		"TRIMLEFT":              func(args []*Expr, _ *Dialect) *Expr { return buildTrim(args, true, false) },
		"TRIMRIGHT":             func(args []*Expr, _ *Dialect) *Expr { return buildTrim(args, false, false) },
		"EDITDISTANCE":          fromArgList(KLevenshtein),
		"JAROWINKLERSIMILARITY": fromArgList(KJarowinklerSimilarity),
		"LEVENSHTEINDISTANCE":   fromArgList(KLevenshtein),
		"UTCTIMESTAMP":          fromArgList(KUtcTimestamp),
	} {
		s.FUNCTIONS[name] = fn
	}

	delete(s.FUNCTION_PARSERS, "MATCH")
	s.FUNCTION_PARSERS["ARRAYJOIN"] = func(p *Parser) *Expr {
		return p.expression(New(KExplode, "this", p.parseExpression()))
	}
	s.FUNCTION_PARSERS["GROUPCONCAT"] = func(p *Parser) *Expr { return p.parseGroupConcat() }
	s.FUNCTION_PARSERS["QUANTILE"] = func(p *Parser) *Expr { return clickhouseParseQuantile(p) }
	s.FUNCTION_PARSERS["MEDIAN"] = func(p *Parser) *Expr { return clickhouseParseQuantile(p) }
	s.FUNCTION_PARSERS["COLUMNS"] = func(p *Parser) *Expr { return clickhouseParseColumns(p) }
	s.FUNCTION_PARSERS["TUPLE"] = func(p *Parser) *Expr {
		return FromArgList(KStruct, p.parseFunctionArgs(true))
	}
	s.FUNCTION_PARSERS["AND"] = func(p *Parser) *Expr { return p.parseConnectorFunction(KAnd) }
	s.FUNCTION_PARSERS["OR"] = func(p *Parser) *Expr { return p.parseConnectorFunction(KOr) }
	s.FUNCTION_PARSERS["XOR"] = func(p *Parser) *Expr { return XorExpr(p.parseFunctionArgs(false)...) }
	// view(SELECT ...) is a table function whose only argument is a query
	s.FUNCTION_PARSERS["VIEW"] = func(p *Parser) *Expr {
		query := p.parseSelect(false, false, true, true, true, nil)
		return p.expression(New(KAnonymous, "this", "view", "expressions", []*Expr{query}))
	}

	delete(s.PROPERTY_PARSERS, "DYNAMIC")
	s.PROPERTY_PARSERS["ENGINE"] = noKwargsE(func(p *Parser) *Expr { return clickhouseParseEngineProperty(p) })
	s.PROPERTY_PARSERS["UUID"] = noKwargsE(func(p *Parser) *Expr {
		return p.expression(New(KUuidProperty, "this", p.parseString()))
	})

	delete(s.NO_PAREN_FUNCTION_PARSERS, "ANY")

	s.RANGE_PARSERS[TK_GLOBAL] = func(p *Parser, this *Expr) *Expr { return clickhouseParseGlobalIn(p, this) }

	delete(s.COLUMN_OPERATORS, TK_PLACEHOLDER)
	s.COLUMN_OPERATORS[TK_DOTCARET] = func(p *Parser, this, field *Expr) *Expr {
		return p.expression(New(KNestedJSONSelect, "this", this, "expression", field))
	}

	s.QUERY_MODIFIER_PARSERS[TK_SETTINGS] = func(p *Parser) (string, any) {
		p.advance(1)
		return "settings", p.parseCSV(func() *Expr { return p.parseAssignment() }, TK_COMMA)
	}
	s.QUERY_MODIFIER_PARSERS[TK_FORMAT] = func(p *Parser) (string, any) {
		p.advance(1)
		return "format", anyExpr(p.parseIdVar(true, nil))
	}

	s.CONSTRAINT_PARSERS["INDEX"] = func(p *Parser) *Expr { return clickhouseParseIndexConstraint(p, "") }
	s.CONSTRAINT_PARSERS["CODEC"] = func(p *Parser) *Expr { return p.parseCompress() }
	s.CONSTRAINT_PARSERS["ASSUME"] = func(p *Parser) *Expr { return clickhouseParseAssumeConstraint(p) }

	s.ALTER_PARSERS["MODIFY"] = func(p *Parser) any { return anyExpr(p.parseAlterTableAlter()) }
	s.ALTER_PARSERS["REPLACE"] = func(p *Parser) any { return anyExpr(clickhouseParseAlterTableReplace(p)) }

	s.PLACEHOLDER_PARSERS[TK_L_BRACE] = func(p *Parser) *Expr { return clickhouseParseQueryParameter(p) }

	s.STATEMENT_PARSERS[TK_DETACH] = func(p *Parser) *Expr { return clickhouseParseDetach(p) }

	s.h.parseCheckConstraint = clickhouseParseCheckConstraint
	s.h.parseAlterTableAlter = clickhouseParseAlterTableAlter
	s.h.parseUserDefinedFunctionExpression = clickhouseParseUserDefinedFunctionExpression
	s.h.parseTypes = clickhouseParseTypes
	s.h.parseExtract = clickhouseParseExtract
	s.h.parseAssignment = clickhouseParseAssignment
	s.h.parseBracket = clickhouseParseBracket
	s.h.parseTable = clickhouseParseTable
	s.h.parsePosition = clickhouseParsePosition
	s.h.parseCte = clickhouseParseCte
	s.h.parseJoinParts = clickhouseParseJoinParts
	s.h.parseJoin = clickhouseParseJoin
	s.h.parseFunction = clickhouseParseFunction
	s.h.parseGroupConcat = clickhouseParseGroupConcat
	s.h.parseWrappedIdVars = clickhouseParseWrappedIdVars
	s.h.parseColumnDef = clickhouseParseColumnDef
	s.h.parsePrimaryKey = clickhouseParsePrimaryKey
	s.h.parseOnProperty = clickhouseParseOnProperty
	s.h.parsePartition = clickhouseParsePartition
	s.h.parseDefiner = clickhouseParseDefiner
	s.h.parseConstraint = clickhouseParseConstraint
	s.h.parseAlias = clickhouseParseAlias
	s.h.parseExpression = clickhouseParseExpression
	s.h.parseValue = clickhouseParseValue
	s.h.parsePartitionedBy = clickhouseParsePartitionedBy
}

func clickhouseParseWrappedSelectOrAssignment(p *Parser) *Expr {
	return p.parseWrapped(func() *Expr {
		e := p.parseSelect(false, false, true, true, true, nil)
		if e == nil {
			e = p.parseAssignment()
		}
		return e
	}, true)
}

func clickhouseParseCheckConstraint(p *Parser) *Expr {
	return p.expression(New(KCheckColumnConstraint, "this", clickhouseParseWrappedSelectOrAssignment(p)))
}

func clickhouseParseAssumeConstraint(p *Parser) *Expr {
	return p.expression(New(KAssumeColumnConstraint, "this", clickhouseParseWrappedSelectOrAssignment(p)))
}

func clickhouseParseEngineProperty(p *Parser) *Expr {
	p.match(TK_EQ)
	return p.expression(New(KEngineProperty, "this", p.parseField(true, nil, true)))
}

// clickhouseParseUserDefinedFunctionExpression parses a CREATE FUNCTION body, which ClickHouse
// writes as a lambda.
// https://clickhouse.com/docs/en/sql-reference/statements/create/function
func clickhouseParseUserDefinedFunctionExpression(p *Parser) *Expr {
	return p.parseLambda(false)
}

func clickhouseParseTypes(p *Parser, checkFunc bool, schema bool, allowIdentifiers bool, withCollation bool) *Expr {
	dtype := p.baseParseTypes(checkFunc, schema, allowIdentifiers, withCollation)
	if dtype.IsA(KDataType) {
		if v, ok := dtype.Arg("nullable").(bool); !(ok && v) {
			// Mark every type as non-nullable which is ClickHouse's default, unless it's
			// already marked as nullable. This marker helps us transpile types from other
			// dialects to ClickHouse, so that we can e.g. produce `CAST(x AS Nullable(String))`
			// from `CAST(x AS TEXT)`. If there is a `NULL` value in `x`, the former would
			// fail in ClickHouse without the `Nullable` type constructor.
			dtype.Set("nullable", false)
		}
	}

	return dtype
}

// clickhouseParseExtract parses EXTRACT(part FROM x) as usual and extract(haystack, pattern) as an
// anonymous function.
func clickhouseParseExtract(p *Parser) *Expr {
	index := p.index
	this := p.parseBitwise()
	if p.match(TK_FROM) {
		p.retreat(index)
		return p.baseParseExtract()
	}

	// We return Anonymous here because extract and regexpExtract have different semantics,
	// so parsing extract(foo, bar) into RegexpExtract can potentially break queries. E.g.,
	// `extract('foobar', 'b')` works, but ClickHouse crashes for `regexpExtract('foobar', 'b')`.
	p.match(TK_COMMA)
	return p.expression(New(KAnonymous, "this", "extract", "expressions", []*Expr{this, p.parseBitwise()}))
}

// clickhouseParseAssignment parses an assignment and the ternary cond ? a : b. The ternary becomes
// an If whose false branch is false, not nil, when ": b" is absent.
func clickhouseParseAssignment(p *Parser) *Expr {
	this := p.baseParseAssignment()

	if p.match(TK_PLACEHOLDER) {
		trueExpr := p.parseAssignment()
		var falseExpr any = false
		if p.match(TK_COLON) {
			falseExpr = anyExpr(p.parseAssignment())
		}
		return p.expression(New(KIf, "this", this, "true", trueExpr, "false", falseExpr))
	}

	return this
}

// clickhouseParseQueryParameter parses a query parameter such as SELECT {abc: UInt32} or
// FROM {table: Identifier}. Without a type it rewinds and returns nil.
// https://clickhouse.com/docs/en/sql-reference/syntax#defining-and-using-query-parameters
func clickhouseParseQueryParameter(p *Parser) *Expr {
	index := p.index

	this := p.parseIdVar(true, nil)
	p.match(TK_COLON)
	var kind any
	if k := p.parseTypes(false, false, false, false); k != nil {
		kind = k
	} else if p.matchTextSeq("IDENTIFIER") {
		kind = "Identifier"
	}

	if kind == nil {
		p.retreat(index)
		return nil
	} else if !p.match(TK_R_BRACE) {
		p.raiseError("Expecting }", nil)
	}

	if this.IsA(KIdentifier) && !this.ArgB("quoted") {
		this = VarExpr(this.Name())
	}

	return p.expression(New(KPlaceholder, "this", this, "kind", kind))
}

// clickhouseParseBracket handles two ClickHouse forms on top of the base bracket parser: x[][]...
// becomes a JSONCast to nested arrays of JSON, and a {key: value} literal becomes a VarMap.
func clickhouseParseBracket(p *Parser, this *Expr) *Expr {
	if this != nil {
		var bracketJSONType *Expr

		for p.matchPair(TK_L_BRACKET, TK_R_BRACKET) {
			inner := bracketJSONType
			if inner == nil {
				inner = New(KDataType, "this", DT_JSON)
				inner.Set("dialect", p.d)
				inner.Set("nullable", false)
			}
			bracketJSONType = New(
				KDataType,
				"this", DT_ARRAY,
				"expressions", []*Expr{inner},
				"nested", true,
			)
		}

		if bracketJSONType != nil {
			return p.expression(New(KJSONCast, "this", this, "to", bracketJSONType))
		}
	}

	lBrace := p.matchNoAdvance(TK_L_BRACE)
	bracket := p.baseParseBracket(this)

	if lBrace && bracket.IsA(KStruct) {
		varmap := New(KVarMap, "keys", New(KArray), "values", New(KArray))
		for _, expression := range bracket.Expressions() {
			if !expression.IsA(KPropertyEQ) {
				break
			}

			varmap.ArgE("keys").Append("expressions", LiteralString(expression.Name()))
			varmap.ArgE("values").Append("expressions", expression.Expression())
		}

		return varmap
	}

	return bracket
}

// clickhouseParseGlobalIn parses the [NOT] IN that follows GLOBAL into an In flagged is_global.
// When no IN follows, it rewinds to GLOBAL and returns nil, so GLOBAL can start a join.
func clickhouseParseGlobalIn(p *Parser, this *Expr) *Expr {
	index := p.index - 1
	isNegated := p.match(TK_NOT)
	if !p.match(TK_IN) {
		p.retreat(index)
		return nil
	}

	inExpr := p.parseIn(this, false)
	inExpr.Set("is_global", true)
	if isNegated {
		return p.expression(New(KNot, "this", inExpr))
	}
	return inExpr
}

func clickhouseParseTable(p *Parser, schema bool, joins bool, aliasTokens *TokenSet, parseBracket bool, isDbReference bool, parsePartition bool, consumePipe bool) *Expr {
	this := p.baseParseTable(schema, joins, aliasTokens, parseBracket, isDbReference, false, false)

	if this.IsA(KTable) {
		inner := this.This()
		alias := this.ArgE("alias")

		if inner.IsA(KGenerateSeries) && alias != nil && len(alias.ArgL("columns")) == 0 {
			alias.Set("columns", []*Expr{ToIdentifier("generate_series", nil)})
		}
	}

	if p.match(TK_FINAL) {
		this = p.expression(New(KFinal, "this", this))
	}

	return this
}

// clickhouseParsePosition parses POSITION with the haystack as the first argument.
func clickhouseParsePosition(p *Parser, haystackFirst bool) *Expr {
	return p.baseParsePosition(true)
}

// clickhouseParseCte parses a CTE in either form: WITH name AS (subquery) or the scalar
// WITH expression AS name.
// https://clickhouse.com/docs/en/sql-reference/statements/select/with/
func clickhouseParseCte(p *Parser) *Expr {
	cte := p.tryParseExpr(func() *Expr { return p.baseParseCte() }, false)

	if cte == nil {
		this := p.parseAssignment()
		alias := p.parseTableAlias(nil)
		cte = p.expression(New(KCTE, "this", this, "alias", alias, "scalar", true))
	}

	return cte
}

// clickhouseParseJoinParts reads the optional GLOBAL or NATURAL, side and kind tokens of a join
// and returns them as (method, side, kind). The kind may come before or after the side (ANY LEFT
// JOIN, LEFT ANY JOIN).
func clickhouseParseJoinParts(p *Parser) (*Token, *Token, *Token) {
	var method, kindPre, side, kind *Token
	if p.match(TK_GLOBAL) || p.match(TK_NATURAL) {
		method = p.prev
	}

	if p.matchSet(p.s.JOIN_KINDS) {
		kindPre = p.prev
	}
	if p.matchSet(p.s.JOIN_SIDES) {
		side = p.prev
	}
	if p.matchSet(p.s.JOIN_KINDS) {
		kind = p.prev
	}

	second := side
	if second == nil {
		second = kind
	}
	third := kindPre
	if third == nil {
		third = kind
	}
	return method, second, third
}

func clickhouseParseJoin(p *Parser, skipJoinToken bool, parseBracket bool, aliasTokens *TokenSet) *Expr {
	join := p.baseParseJoin(skipJoinToken, true, aliasTokens)
	if join != nil {
		if join.Text("method") == "GLOBAL" {
			join.Set("method", nil)
			join.Set("global_", "GLOBAL")
		}

		// tbl ARRAY JOIN arr <-- this should be a `Column` reference, not a `Table`
		// https://clickhouse.com/docs/en/sql-reference/statements/select/array-join
		if join.KindText() == "ARRAY" {
			for table := range join.FindAll(KTable) {
				table.Replace(chunkCTableToColumn(table, true))
			}
		}
	}

	return join
}

// clickhouseParseFunction parses a function call and turns aggregate names with combinator
// suffixes or a parameter list, such as sumIf(x) or quantile(0.9)(x), into the combined and
// parameterized aggregate kinds.
func clickhouseParseFunction(p *Parser, functions map[string]FuncBuilder, anonymous bool, optionalParens bool, anyToken bool) *Expr {
	expr := p.baseParseFunction(functions, anonymous, optionalParens, anyToken)

	fn := expr
	if expr.IsA(KWindow) {
		fn = expr.This()
	}

	var suffixes []string
	resolved := false
	if fn.IsA(KAnonymous) {
		_, suffixes, resolved = clickhouseResolveAgg(fn.Name())
	}

	if resolved {
		anonFunc := fn
		params := clickhouseParseFuncParams(p, anonFunc)

		var expClass Kind
		if len(suffixes) > 0 {
			if len(params) > 0 {
				expClass = KCombinedParameterizedAgg
			} else {
				expClass = KCombinedAggFunc
			}
		} else {
			if len(params) > 0 {
				expClass = KParameterizedAgg
			} else {
				expClass = KAnonymousAggFunc
			}
		}

		instance := New(expClass, "this", anonFunc.Arg("this"), "expressions", anonFunc.ArgL("expressions"))
		if len(params) > 0 {
			instance.Set("params", params)
		}
		fn = p.expression(instance)

		if expr.IsA(KWindow) {
			// The window's func was parsed as Anonymous in base parser, fix its
			// type to be ClickHouse style CombinedAnonymousAggFunc / AnonymousAggFunc
			expr.Set("this", fn)
		} else if len(params) > 0 {
			// Params kept the base parseFunction from parsing the following window (if any),
			// as they stand between the function call and the window spec
			expr = p.parseWindow(fn, false)
		} else {
			expr = fn
		}
	}

	return expr
}

// clickhouseParseFuncParams parses the second argument list of a parameterized call such as
// quantile(0.9)(x), after ")(" or "(". It returns nil when there is none.
func clickhouseParseFuncParams(p *Parser, this *Expr) []*Expr {
	if p.matchPair(TK_R_PAREN, TK_L_PAREN) {
		return p.parseCSV(func() *Expr { return p.parseLambda(false) }, TK_COMMA)
	}

	if p.match(TK_L_PAREN) {
		params := p.parseCSV(func() *Expr { return p.parseLambda(false) }, TK_COMMA)
		p.matchRParen(this)
		return params
	}

	return nil
}

func clickhouseParseGroupConcat(p *Parser) *Expr {
	args := p.parseCSV(func() *Expr { return p.parseLambda(false) }, TK_COMMA)
	params := clickhouseParseFuncParams(p, nil)

	if len(params) > 0 {
		// groupConcat(sep [, limit])(expr)
		separator := seqGet(args, 0)
		limit := seqGet(args, 1)
		this := seqGet(params, 0)
		if limit != nil {
			this = New(KLimit, "this", this, "expression", limit)
		}
		return p.expression(New(KGroupConcat, "this", this, "separator", separator))
	}

	// groupConcat(expr)
	return p.expression(New(KGroupConcat, "this", seqGet(args, 0)))
}

// clickhouseParseQuantile parses quantile(level)(x) and median(x); the level defaults to 0.5.
func clickhouseParseQuantile(p *Parser) *Expr {
	this := p.parseLambda(false)
	params := clickhouseParseFuncParams(p, nil)
	if len(params) > 0 {
		return p.expression(New(KQuantile, "this", params[0], "quantile", this))
	}
	return p.expression(New(KQuantile, "this", this, "quantile", LiteralNumber("0.5")))
}

// clickhouseParseWrappedIdVars parses an identifier list whose parentheses are always optional.
func clickhouseParseWrappedIdVars(p *Parser, optional bool) []*Expr {
	return p.baseParseWrappedIdVars(true)
}

func clickhouseParseColumnDef(p *Parser, this *Expr, computedColumn bool) *Expr {
	if p.match(TK_DOT) {
		return New(KDot, "this", this, "expression", p.parseIdVar(true, nil))
	}

	return p.baseParseColumnDef(this, computedColumn)
}

func clickhouseParsePrimaryKey(p *Parser, wrappedOptional bool, inProps bool, namedPrimaryKey bool) *Expr {
	return p.baseParsePrimaryKey(wrappedOptional || inProps, inProps, namedPrimaryKey)
}

func clickhouseParseOnProperty(p *Parser) *Expr {
	index := p.index
	if p.matchTextSeq("CLUSTER") {
		this := p.parseString()
		if this == nil {
			this = p.parseIdVar(true, nil)
		}
		if this != nil {
			return p.expression(New(KOnCluster, "this", this))
		}
		p.retreat(index)
	}
	return nil
}

func clickhouseParseIndexConstraint(p *Parser, kind string) *Expr {
	this := p.parseIdVar(true, nil)
	expression := p.parseAssignment()

	var indexType any = false
	if p.matchTextSeq("TYPE") {
		it := p.parseFunction(nil, false, true, false)
		if it == nil {
			it = p.parseVar(false, nil, false)
		}
		indexType = anyExpr(it)
	}

	var granularity any = false
	if p.matchTextSeq("GRANULARITY") {
		granularity = anyExpr(p.parseTerm())
	}

	return p.expression(New(
		KIndexColumnConstraint,
		"this", this,
		"expression", expression,
		"index_type", indexType,
		"granularity", granularity,
	))
}

// clickhouseParsePartition parses PARTITION <expressions> or PARTITION ID '<id>'.
// https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#how-to-set-partition-expression
func clickhouseParsePartition(p *Parser) *Expr {
	if !p.match(TK_PARTITION) {
		return nil
	}

	var expressions []*Expr
	if p.matchTextSeq("ID") {
		expressions = []*Expr{p.expression(New(KPartitionId, "this", p.parseString()))}
	} else {
		expressions = p.parseExpressions()
	}

	return p.expression(New(KPartition, "expressions", expressions))
}

func clickhouseParseAlterTableReplace(p *Parser) *Expr {
	partition := p.parsePartition()

	if partition == nil || !p.match(TK_FROM) {
		return nil
	}

	return p.expression(New(KReplacePartition, "expression", partition, "source", p.parseTableParts(false, false, false, false)))
}

func clickhouseParseAlterTableAlter(p *Parser) *Expr {
	// MODIFY forms other than MODIFY COLUMN (SQL SECURITY, ORDER BY, TTL, COMMENT, ...) are
	// parsed as properties
	if upperText(p.prev) == "MODIFY" && !p.matchNoAdvance(TK_COLUMN) {
		if properties := p.parseProperties(false); properties != nil {
			return p.expression(New(KAlterModifySqlSecurity, "expressions", properties.Expressions()))
		}
		return nil
	}

	// MODIFY COLUMN is ALTER COLUMN, but only as the whole action
	// https://clickhouse.com/docs/sql-reference/statements/alter/column#modify-column
	alter := p.baseParseAlterTableAlter()
	if p.curr.ok() {
		return nil
	}
	return alter
}

func clickhouseParseDefiner(p *Parser) *Expr {
	p.match(TK_EQ)
	if p.match(TK_CURRENT_USER) {
		return New(KDefinerProperty, "this", New(KVar, "this", upperText(p.prev)))
	}
	return New(KDefinerProperty, "this", p.parseString())
}

func clickhouseParseProjectionDef(p *Parser) *Expr {
	if !p.match(TK_PROJECTION) {
		return nil
	}

	this := p.parseIdVar(true, nil)
	expression := p.parseWrapped(func() *Expr { return p.parseStatement() }, false)
	return p.expression(New(KProjectionDef, "this", this, "expression", expression))
}

func clickhouseParseConstraint(p *Parser) *Expr {
	if c := p.baseParseConstraint(); c != nil {
		return c
	}
	return clickhouseParseProjectionDef(p)
}

func clickhouseParseAlias(p *Parser, this *Expr, explicit bool) *Expr {
	// In clickhouse "SELECT <expr> APPLY(...)" is a query modifier,
	// so "APPLY" shouldn't be parsed as <expr>'s alias. However, "SELECT <expr> apply" is a valid alias
	if p.matchPairNoAdvance(TK_APPLY, TK_L_PAREN) {
		return this
	}

	return p.baseParseAlias(this, explicit)
}

func clickhouseParseExpression(p *Parser) *Expr {
	this := p.baseParseExpression()

	// Clickhouse allows "SELECT <expr> [APPLY(func)] [...]]" modifier
	for p.matchPair(TK_APPLY, TK_L_PAREN) {
		this = New(KApply, "this", this, "expression", p.parseVar(true, nil, false))
		p.match(TK_R_PAREN)
	}

	return this
}

func clickhouseParseColumns(p *Parser) *Expr {
	this := p.expression(New(KColumns, "this", p.parseLambda(false)))

	for p.next.ok() && p.matchTextSeq(")", "APPLY", "(") {
		p.match(TK_R_PAREN)
		this = New(KApply, "this", this, "expression", p.parseVar(true, nil, false))
	}
	return this
}

func clickhouseParseValue(p *Parser, values bool) *Expr {
	value := p.baseParseValue(values)
	if value == nil {
		return nil
	}

	// In Clickhouse "SELECT * FROM VALUES (1, 2, 3)" generates a table with a single column, in contrast
	// to other dialects. For this case, we canonicalize the values into a tuple-of-tuples AST if it's not already one.
	// In INSERT INTO statements the same clause actually references multiple columns (opposite semantics),
	// but the final result is not altered by the extra parentheses.
	// Note: Clickhouse allows VALUES([structure], value, ...) so the branch checks for the last expression
	// A single-value tuple is generated as "(x)", which is parsed back into a Paren
	// rather than a Tuple, so it's unwrapped here to keep this rewrite idempotent
	expressions := value.Expressions()
	if values {
		if len(expressions) == 0 {
			// an empty list is an error; the message keeps its historical wording
			panic(&ValueError{Msg: "list index out of range"})
		}
		if !expressions[len(expressions)-1].IsA(KTuple) {
			wrapped := make([]*Expr, len(expressions))
			for i, expr := range expressions {
				wrapped[i] = p.expression(New(KTuple, "expressions", []*Expr{expr.Unnest()}))
			}
			value.Set("expressions", wrapped)
		}
	}

	return value
}

// clickhouseParsePartitionedBy accepts any expression as the partition key:
// https://clickhouse.com/docs/engines/table-engines/mergetree-family/custom-partitioning-key
func clickhouseParsePartitionedBy(p *Parser) *Expr {
	return p.expression(New(KPartitionedByProperty, "this", p.parseAssignment()))
}

func clickhouseParseDetach(p *Parser) *Expr {
	var kind any = false
	if p.matchSet(p.s.DB_CREATABLES) {
		kind = upperText(p.prev)
	}
	exists := p.parseExists(false)
	this := p.parseTableParts(false, false, false, false)

	var cluster *Expr
	if p.match(TK_ON) {
		cluster = p.parseOnProperty()
	}
	permanent := p.matchTextSeq("PERMANENTLY")
	sync := p.matchTextSeq("SYNC")

	return p.expression(New(
		KDetach,
		"this", this,
		"kind", kind,
		"exists", exists,
		"cluster", cluster,
		"permanent", permanent,
		"sync", sync,
	))
}
