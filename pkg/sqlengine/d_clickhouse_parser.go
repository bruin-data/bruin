package sqlengine

import (
	"fmt"
	"sync"
)

// Port of sqlglot/parsers/clickhouse.py (ClickHouseParser).

// ---------------------------------------------------------------------------------------------
// Module-level builders
// ---------------------------------------------------------------------------------------------

// clickhouseBuildDatetimeFormat mirrors _build_datetime_format(expr_type).
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

// clickhouseBuildCountIf mirrors _build_count_if.
func clickhouseBuildCountIf(args []*Expr, _ *Dialect) *Expr {
	if len(args) == 1 {
		return New(KCountIf, "this", seqGet(args, 0))
	}

	return New(KCombinedAggFunc, "this", "countIf", "expressions", args)
}

// clickhouseBuildStrToDate mirrors _build_str_to_date.
func clickhouseBuildStrToDate(args []*Expr, _ *Dialect) *Expr {
	if len(args) == 3 {
		return New(KAnonymous, "this", "STR_TO_DATE", "expressions", args)
	}

	strtodate := FromArgList(KStrToDate, args)
	return CastExpr(strtodate, NewDataType(DT_DATETIME), true, nil)
}

// clickhouseBuildTimestampTrunc mirrors _build_timestamp_trunc(unit).
func clickhouseBuildTimestampTrunc(unit string) FuncBuilder {
	return func(args []*Expr, _ *Dialect) *Expr {
		return New(KTimestampTrunc, "this", seqGet(args, 0), "unit", VarExpr(unit), "zone", seqGet(args, 1))
	}
}

// clickhouseBuildSplitByChar mirrors _build_split_by_char.
func clickhouseBuildSplitByChar(args []*Expr, d *Dialect) *Expr {
	sep := seqGet(args, 0)
	if sep.IsA(KLiteral) {
		// sep.to_py() is a str only for string literals
		if sep.IsString() && len(sep.ThisS()) == 1 {
			return clickhouseBuildSplit(KSplit)(args, d)
		}
	}

	return New(KAnonymous, "this", "splitByChar", "expressions", args)
}

// clickhouseBuildSplit mirrors _build_split(exp_class).
func clickhouseBuildSplit(kind Kind) FuncBuilder {
	return func(args []*Expr, _ *Dialect) *Expr {
		return New(kind, "this", seqGet(args, 1), "expression", seqGet(args, 0), "limit", seqGet(args, 2))
	}
}

// clickhouseTimestampTruncUnits mirrors TIMESTAMP_TRUNC_UNITS.
// Skip the 'week' unit since ClickHouse's toStartOfWeek
// uses an extra mode argument to specify the first day of the week
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
	suffix string // "" means None
}

var (
	clickhouseAggFuncMappingOnce sync.Once
	clickhouseAggFuncMapping     map[string]clickhouseAggParts
	clickhouseAggSuffixes        []string
)

// clickhouseAggFuncMappingGet returns AGG_FUNC_MAPPING (memoized examples of all 0- and 1-suffix
// aggregate function names) and AGG_FUNCTIONS_SUFFIXES (sorted longest-first).
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

// clickhouseResolveAgg mirrors ClickHouseParser._resolve_clickhouse_agg. It returns the aggregate
// function name and the (outermost-last) list of combinator suffixes, or ok=false (None).
func clickhouseResolveAgg(name string) (string, []string, bool) {
	// ClickHouse allows chaining multiple combinators on aggregate functions.
	// See https://clickhouse.com/docs/sql-reference/aggregate-functions/combinators
	// N.B. this resolution allows any suffix stack, including ones that ClickHouse rejects
	// syntactically such as sumMergeMerge (due to repeated adjacent suffixes)
	mapping, suffixes := clickhouseAggFuncMappingGet()

	// Until we are able to identify a 1- or 0-suffix aggregate function by name,
	// repeatedly strip and queue suffixes (checking longer suffixes first). This loop only
	// runs for 2 or more suffixes, as AGG_FUNC_MAPPING memoizes all 0- and 1-suffix
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

	// We now have a 0- or 1-suffix aggregate
	if parts.suffix != "" {
		// this is a 1-suffix aggregate (either naturally or via repeated suffix
		// stripping). prepend the innermost suffix.
		accumulated = append([]string{parts.suffix}, accumulated...)
	}

	return parts.name, accumulated, true
}

// ---------------------------------------------------------------------------------------------
// Parser customization
// ---------------------------------------------------------------------------------------------

func customizeClickHouseParser(d *Dialect) {
	s := d.P

	// FUNCTIONS
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
		"ANY":           fromArgList(KAnyValue),
		"ARRAYCOMPACT":  fromArgList(KArrayCompact),
		"ARRAYCONCAT":   fromArgList(KArrayConcat),
		"ARRAYDISTINCT": fromArgList(KArrayDistinct),
		"ARRAYEXCEPT":   fromArgList(KArrayExcept),
		"ARRAYSUM":      fromArgList(KArraySum),
		"ARRAYMAX":      fromArgList(KArrayMax),
		"ARRAYMIN":      fromArgList(KArrayMin),
		"ARRAYREVERSE":  fromArgList(KArrayReverse),
		"ARRAYSLICE":    fromArgList(KArraySlice),
		"ARRAYFILTER": func(args []*Expr, _ *Dialect) *Expr {
			return New(KArrayFilter, "this", seqGet(args, 1), "expression", seqGet(args, 0))
		},
		"ARRAYMAP": func(args []*Expr, _ *Dialect) *Expr {
			return New(KTransform, "this", seqGet(args, 1), "expression", seqGet(args, 0))
		},
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
		"JSONEXTRACTSTRING": buildJSONExtractPath(KJSONExtractScalar, false, false, ""),
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
		"EDITDISTANCE":          fromArgList(KLevenshtein),
		"JAROWINKLERSIMILARITY": fromArgList(KJarowinklerSimilarity),
		"LEVENSHTEINDISTANCE":   fromArgList(KLevenshtein),
		"UTCTIMESTAMP":          fromArgList(KUtcTimestamp),
	} {
		s.FUNCTIONS[name] = fn
	}

	// FUNCTION_PARSERS
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
	s.FUNCTION_PARSERS["AND"] = func(p *Parser) *Expr { return AndExpr(p.parseFunctionArgs(false)...) }
	s.FUNCTION_PARSERS["OR"] = func(p *Parser) *Expr { return OrExpr(p.parseFunctionArgs(false)...) }
	s.FUNCTION_PARSERS["XOR"] = func(p *Parser) *Expr { return XorExpr(p.parseFunctionArgs(false)...) }

	// PROPERTY_PARSERS
	delete(s.PROPERTY_PARSERS, "DYNAMIC")
	s.PROPERTY_PARSERS["ENGINE"] = noKwargsE(func(p *Parser) *Expr { return clickhouseParseEngineProperty(p) })
	s.PROPERTY_PARSERS["UUID"] = noKwargsE(func(p *Parser) *Expr {
		return p.expression(New(KUuidProperty, "this", p.parseString()))
	})

	// NO_PAREN_FUNCTION_PARSERS
	delete(s.NO_PAREN_FUNCTION_PARSERS, "ANY")

	// RANGE_PARSERS
	s.RANGE_PARSERS[TK_GLOBAL] = func(p *Parser, this *Expr) *Expr { return clickhouseParseGlobalIn(p, this) }

	// COLUMN_OPERATORS
	delete(s.COLUMN_OPERATORS, TK_PLACEHOLDER)
	s.COLUMN_OPERATORS[TK_DOTCARET] = func(p *Parser, this, field *Expr) *Expr {
		return p.expression(New(KNestedJSONSelect, "this", this, "expression", field))
	}

	// QUERY_MODIFIER_PARSERS
	s.QUERY_MODIFIER_PARSERS[TK_SETTINGS] = func(p *Parser) (string, any) {
		p.advance(1)
		return "settings", p.parseCSV(func() *Expr { return p.parseAssignment() }, TK_COMMA)
	}
	s.QUERY_MODIFIER_PARSERS[TK_FORMAT] = func(p *Parser) (string, any) {
		p.advance(1)
		return "format", anyExpr(p.parseIdVar(true, nil))
	}

	// CONSTRAINT_PARSERS
	s.CONSTRAINT_PARSERS["INDEX"] = func(p *Parser) *Expr { return clickhouseParseIndexConstraint(p, "") }
	s.CONSTRAINT_PARSERS["CODEC"] = func(p *Parser) *Expr { return p.parseCompress() }
	s.CONSTRAINT_PARSERS["ASSUME"] = func(p *Parser) *Expr { return clickhouseParseAssumeConstraint(p) }

	// ALTER_PARSERS
	s.ALTER_PARSERS["MODIFY"] = func(p *Parser) any { return anyExpr(clickhouseParseAlterTableModify(p)) }
	s.ALTER_PARSERS["REPLACE"] = func(p *Parser) any { return anyExpr(clickhouseParseAlterTableReplace(p)) }

	// PLACEHOLDER_PARSERS
	s.PLACEHOLDER_PARSERS[TK_L_BRACE] = func(p *Parser) *Expr { return clickhouseParseQueryParameter(p) }

	// STATEMENT_PARSERS
	s.STATEMENT_PARSERS[TK_DETACH] = func(p *Parser) *Expr { return clickhouseParseDetach(p) }

	// Method overrides
	s.h.parseCheckConstraint = clickhouseParseCheckConstraint
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

// ---------------------------------------------------------------------------------------------
// Methods
// ---------------------------------------------------------------------------------------------

// clickhouseParseWrappedSelectOrAssignment mirrors _parse_wrapped_select_or_assignment.
func clickhouseParseWrappedSelectOrAssignment(p *Parser) *Expr {
	return p.parseWrapped(func() *Expr {
		e := p.parseSelect(false, false, true, true, true, nil)
		if e == nil {
			e = p.parseAssignment()
		}
		return e
	}, true)
}

// clickhouseParseCheckConstraint mirrors _parse_check_constraint.
func clickhouseParseCheckConstraint(p *Parser) *Expr {
	return p.expression(New(KCheckColumnConstraint, "this", clickhouseParseWrappedSelectOrAssignment(p)))
}

// clickhouseParseAssumeConstraint mirrors _parse_assume_constraint.
func clickhouseParseAssumeConstraint(p *Parser) *Expr {
	return p.expression(New(KAssumeColumnConstraint, "this", clickhouseParseWrappedSelectOrAssignment(p)))
}

// clickhouseParseEngineProperty mirrors _parse_engine_property.
func clickhouseParseEngineProperty(p *Parser) *Expr {
	p.match(TK_EQ)
	return p.expression(New(KEngineProperty, "this", p.parseField(true, nil, true)))
}

// clickhouseParseUserDefinedFunctionExpression mirrors _parse_user_defined_function_expression.
// https://clickhouse.com/docs/en/sql-reference/statements/create/function
func clickhouseParseUserDefinedFunctionExpression(p *Parser) *Expr {
	return p.parseLambda(false)
}

// clickhouseParseTypes mirrors _parse_types.
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

// clickhouseParseExtract mirrors _parse_extract.
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

// clickhouseParseAssignment mirrors _parse_assignment.
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

// clickhouseParseQueryParameter mirrors _parse_query_parameter.
// Parse a placeholder expression like SELECT {abc: UInt32} or FROM {table: Identifier}
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

// clickhouseParseBracket mirrors _parse_bracket.
func clickhouseParseBracket(p *Parser, this *Expr) *Expr {
	if this != nil {
		var bracketJSONType *Expr

		for p.matchPair(TK_L_BRACKET, TK_R_BRACKET) {
			inner := bracketJSONType
			if inner == nil {
				// exp.DType.JSON.into_expr(dialect=self.dialect, nullable=False)
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

// clickhouseParseGlobalIn mirrors _parse_global_in.
func clickhouseParseGlobalIn(p *Parser, this *Expr) *Expr {
	isNegated := p.match(TK_NOT)
	var inExpr *Expr
	if p.match(TK_IN) {
		inExpr = p.parseIn(this, false)
		inExpr.Set("is_global", true)
	}
	if isNegated {
		return p.expression(New(KNot, "this", inExpr))
	}
	return inExpr
}

// clickhouseParseTable mirrors _parse_table.
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

// clickhouseParsePosition mirrors _parse_position.
func clickhouseParsePosition(p *Parser, haystackFirst bool) *Expr {
	return p.baseParsePosition(true)
}

// clickhouseParseCte mirrors _parse_cte.
// https://clickhouse.com/docs/en/sql-reference/statements/select/with/
func clickhouseParseCte(p *Parser) *Expr {
	// WITH <identifier> AS <subquery expression>
	cte := p.tryParseExpr(func() *Expr { return p.baseParseCte() }, false)

	if cte == nil {
		// WITH <expression> AS <identifier>
		this := p.parseAssignment()
		alias := p.parseTableAlias(nil)
		cte = p.expression(New(KCTE, "this", this, "alias", alias, "scalar", true))
	}

	return cte
}

// clickhouseParseJoinParts mirrors _parse_join_parts.
func clickhouseParseJoinParts(p *Parser) (*Token, *Token, *Token) {
	var isGlobal, kindPre, side, kind *Token
	if p.match(TK_GLOBAL) {
		isGlobal = p.prev
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
	return isGlobal, second, third
}

// clickhouseParseJoin mirrors _parse_join.
func clickhouseParseJoin(p *Parser, skipJoinToken bool, parseBracket bool, aliasTokens *TokenSet) *Expr {
	join := p.baseParseJoin(skipJoinToken, true, aliasTokens)
	if join != nil {
		method := join.Arg("method")
		join.Set("method", nil)
		join.Set("global_", method)

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

// clickhouseParseFunction mirrors _parse_function.
func clickhouseParseFunction(p *Parser, functions map[string]FuncBuilder, anonymous bool, optionalParens bool, anyToken bool) *Expr {
	expr := p.baseParseFunction(functions, anonymous, optionalParens, anyToken)

	fn := expr
	if expr.IsA(KWindow) {
		fn = expr.This()
	}

	// Aggregate functions can be split in 2 parts: <func_name><suffix[es]>
	var suffixes []string
	resolved := false
	if fn.IsA(KAnonymous) {
		switch name := fn.Arg("this").(type) {
		case string:
			_, suffixes, resolved = clickhouseResolveAgg(name)
		case *Expr:
			// Python calls name.endswith(...) on the (non-str) expression and raises AttributeError,
			// e.g. for quoted function names such as `f`(x).
			panic(&ValueError{Msg: fmt.Sprintf("'%s' object has no attribute 'endswith'", name.Kind().Name())})
		}
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
			// Params have blocked super()._parse_function() from parsing the following window
			// (if that exists) as they're standing between the function call and the window spec
			expr = p.parseWindow(fn, false)
		} else {
			expr = fn
		}
	}

	return expr
}

// clickhouseParseFuncParams mirrors _parse_func_params. A nil result is Python None.
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

// clickhouseParseGroupConcat mirrors _parse_group_concat.
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

// clickhouseParseQuantile mirrors _parse_quantile.
func clickhouseParseQuantile(p *Parser) *Expr {
	this := p.parseLambda(false)
	params := clickhouseParseFuncParams(p, nil)
	if len(params) > 0 {
		return p.expression(New(KQuantile, "this", params[0], "quantile", this))
	}
	return p.expression(New(KQuantile, "this", this, "quantile", LiteralNumber("0.5")))
}

// clickhouseParseWrappedIdVars mirrors _parse_wrapped_id_vars.
func clickhouseParseWrappedIdVars(p *Parser, optional bool) []*Expr {
	return p.baseParseWrappedIdVars(true)
}

// clickhouseParseColumnDef mirrors _parse_column_def.
func clickhouseParseColumnDef(p *Parser, this *Expr, computedColumn bool) *Expr {
	if p.match(TK_DOT) {
		return New(KDot, "this", this, "expression", p.parseIdVar(true, nil))
	}

	return p.baseParseColumnDef(this, computedColumn)
}

// clickhouseParsePrimaryKey mirrors _parse_primary_key.
func clickhouseParsePrimaryKey(p *Parser, wrappedOptional bool, inProps bool, namedPrimaryKey bool) *Expr {
	return p.baseParsePrimaryKey(wrappedOptional || inProps, inProps, namedPrimaryKey)
}

// clickhouseParseOnProperty mirrors _parse_on_property.
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

// clickhouseParseIndexConstraint mirrors _parse_index_constraint. kind "" means None.
func clickhouseParseIndexConstraint(p *Parser, kind string) *Expr {
	// INDEX name1 expr TYPE type1(args) GRANULARITY value
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

// clickhouseParsePartition mirrors _parse_partition.
// https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#how-to-set-partition-expression
func clickhouseParsePartition(p *Parser) *Expr {
	if !p.match(TK_PARTITION) {
		return nil
	}

	var expressions []*Expr
	if p.matchTextSeq("ID") {
		// Corresponds to the PARTITION ID <string_value> syntax
		expressions = []*Expr{p.expression(New(KPartitionId, "this", p.parseString()))}
	} else {
		expressions = p.parseExpressions()
	}

	return p.expression(New(KPartition, "expressions", expressions))
}

// clickhouseParseAlterTableReplace mirrors _parse_alter_table_replace.
func clickhouseParseAlterTableReplace(p *Parser) *Expr {
	partition := p.parsePartition()

	if partition == nil || !p.match(TK_FROM) {
		return nil
	}

	return p.expression(New(KReplacePartition, "expression", partition, "source", p.parseTableParts(false, false, false, false)))
}

// clickhouseParseAlterTableModify mirrors _parse_alter_table_modify.
func clickhouseParseAlterTableModify(p *Parser) *Expr {
	if properties := p.parseProperties(false); properties != nil {
		return p.expression(New(KAlterModifySqlSecurity, "expressions", properties.Expressions()))
	}
	return nil
}

// clickhouseParseDefiner mirrors _parse_definer.
func clickhouseParseDefiner(p *Parser) *Expr {
	p.match(TK_EQ)
	if p.match(TK_CURRENT_USER) {
		return New(KDefinerProperty, "this", New(KVar, "this", upperText(p.prev)))
	}
	return New(KDefinerProperty, "this", p.parseString())
}

// clickhouseParseProjectionDef mirrors _parse_projection_def.
func clickhouseParseProjectionDef(p *Parser) *Expr {
	if !p.match(TK_PROJECTION) {
		return nil
	}

	this := p.parseIdVar(true, nil)
	expression := p.parseWrapped(func() *Expr { return p.parseStatement() }, false)
	return p.expression(New(KProjectionDef, "this", this, "expression", expression))
}

// clickhouseParseConstraint mirrors _parse_constraint.
func clickhouseParseConstraint(p *Parser) *Expr {
	if c := p.baseParseConstraint(); c != nil {
		return c
	}
	return clickhouseParseProjectionDef(p)
}

// clickhouseParseAlias mirrors _parse_alias.
func clickhouseParseAlias(p *Parser, this *Expr, explicit bool) *Expr {
	// In clickhouse "SELECT <expr> APPLY(...)" is a query modifier,
	// so "APPLY" shouldn't be parsed as <expr>'s alias. However, "SELECT <expr> apply" is a valid alias
	if p.matchPairNoAdvance(TK_APPLY, TK_L_PAREN) {
		return this
	}

	return p.baseParseAlias(this, explicit)
}

// clickhouseParseExpression mirrors _parse_expression.
func clickhouseParseExpression(p *Parser) *Expr {
	this := p.baseParseExpression()

	// Clickhouse allows "SELECT <expr> [APPLY(func)] [...]]" modifier
	for p.matchPair(TK_APPLY, TK_L_PAREN) {
		this = New(KApply, "this", this, "expression", p.parseVar(true, nil, false))
		p.match(TK_R_PAREN)
	}

	return this
}

// clickhouseParseColumns mirrors _parse_columns.
func clickhouseParseColumns(p *Parser) *Expr {
	this := p.expression(New(KColumns, "this", p.parseLambda(false)))

	for p.next.ok() && p.matchTextSeq(")", "APPLY", "(") {
		p.match(TK_R_PAREN)
		this = New(KApply, "this", this, "expression", p.parseVar(true, nil, false))
	}
	return this
}

// clickhouseParseValue mirrors _parse_value.
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
	expressions := value.Expressions()
	if values {
		if len(expressions) == 0 {
			// Python: expressions[-1] raises IndexError
			panic(&ValueError{Msg: "list index out of range"})
		}
		if !expressions[len(expressions)-1].IsA(KTuple) {
			wrapped := make([]*Expr, len(expressions))
			for i, expr := range expressions {
				wrapped[i] = p.expression(New(KTuple, "expressions", []*Expr{expr}))
			}
			value.Set("expressions", wrapped)
		}
	}

	return value
}

// clickhouseParsePartitionedBy mirrors _parse_partitioned_by.
// ClickHouse allows custom expressions as partition key
// https://clickhouse.com/docs/engines/table-engines/mergetree-family/custom-partitioning-key
func clickhouseParsePartitionedBy(p *Parser) *Expr {
	return p.expression(New(KPartitionedByProperty, "this", p.parseAssignment()))
}

// clickhouseParseDetach mirrors _parse_detach.
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
