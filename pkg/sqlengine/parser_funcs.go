package sqlengine

// Ports of sqlglot.parser.Parser methods for brackets, special function syntaxes (CASE, IF, CAST,
// EXTRACT, TRIM, JSON/XML functions, ...), windows, aliases and primitive values
// (parser.py L7714-L8720).

// _parse_bracket_key_value (parser.py L7714)
func (p *Parser) baseParseBracketKeyValue(isMap bool) *Expr {
	return p.parseSlice(p.parseAlias(p.parseDisjunction(), true))
}

// _parse_odbc_datetime_literal (parser.py L7717)
//
// Parses a datetime column in ODBC format. We parse the column into the corresponding
// types, for example `{d'yyyy-mm-dd'}` will be parsed as a `Date` column, exactly the
// same as we did for `DATE('yyyy-mm-dd')`.
func (p *Parser) parseOdbcDatetimeLiteral() *Expr {
	p.match(TK_VAR)
	key := pyLower(p.prev.Text)
	expClass, ok := p.s.ODBC_DATETIME_LITERALS[key]
	if !ok {
		// Python raises a KeyError here.
		panic(&ValueError{Msg: pyRepr(key)})
	}
	expression := p.expression(New(expClass, "this", p.parseString()))
	if !p.match(TK_R_BRACE) {
		p.raiseError("Expected }", nil)
	}
	return expression
}

// _parse_bracket (parser.py L7733)
func (p *Parser) baseParseBracket(this *Expr) *Expr {
	if !p.matchSet(p.s.BRACKETS) {
		return this
	}

	parseMap := false
	if p.s.MAP_KEYS_ARE_ARBITRARY_EXPRESSIONS {
		mapToken := chunkETokenAt(p.tokens, p.index-2)
		parseMap = mapToken != nil && upperText(mapToken) == "MAP"
	}

	bracketKind := p.prev.Type
	if bracketKind == TK_L_BRACE && p.curr.ok() && p.curr.Type == TK_VAR {
		if _, ok := p.s.ODBC_DATETIME_LITERALS[pyLower(p.curr.Text)]; ok {
			return p.parseOdbcDatetimeLiteral()
		}
	}

	expressions := p.parseCSV(func() *Expr {
		return p.parseBracketKeyValue(bracketKind == TK_L_BRACE)
	}, TK_COMMA)

	if bracketKind == TK_L_BRACKET && !p.match(TK_R_BRACKET) {
		p.raiseError("Expected ]", nil)
	} else if bracketKind == TK_L_BRACE && !p.match(TK_R_BRACE) {
		p.raiseError("Expected }", nil)
	}

	// https://duckdb.org/docs/sql/data_types/struct.html#creating-structs
	if bracketKind == TK_L_BRACE {
		this = p.expression(New(KStruct, "expressions", p.kvToPropEq(expressions, parseMap)))
	} else if this == nil {
		this = chunkEBuildArrayConstructor(KArray, expressions, bracketKind, p.d)
	} else {
		if constructorType, ok := p.s.ARRAY_CONSTRUCTORS[pyUpper(this.Name())]; ok {
			return chunkEBuildArrayConstructor(constructorType, expressions, bracketKind, p.d)
		}

		expressions = chunkEApplyIndexOffset(this, expressions, -p.d.S.INDEX_OFFSET, p.d)
		bracket := New(KBracket, "this", this, "expressions", expressions)
		this = p.expressionC(bracket, this.PopComments())
	}

	p.addComments(this)
	return p.parseBracket(this)
}

// _parse_slice (parser.py L7792)
func (p *Parser) parseSlice(this *Expr) *Expr {
	if !p.match(TK_COLON) {
		return this
	}

	var end *Expr
	if p.matchPairNoAdvance(TK_DASH, TK_COLON) {
		p.advance(1)
		end = New(KNeg, "this", LiteralNumber("1"))
	} else {
		end = p.parseAssignment()
	}
	var step *Expr
	if p.match(TK_COLON) {
		step = p.parseUnary()
	}
	return p.expression(New(KSlice, "this", this, "expression", end, "step", step))
}

// _parse_case (parser.py L7804)
func (p *Parser) parseCase() *Expr {
	if p.matchNoAdvance(TK_DOT) {
		// Avoid raising on valid expressions like case.*, supported by, e.g., spark & snowflake
		p.retreat(p.index - 1)
		return nil
	}

	ifs := []*Expr{}
	var default_ *Expr

	comments := p.prevComments
	expression := p.parseDisjunction()

	for p.match(TK_WHEN) {
		this := p.parseDisjunction()
		p.match(TK_THEN)
		then := p.parseDisjunction()
		ifs = append(ifs, p.expression(New(KIf, "this", this, "true", then)))
	}

	if p.match(TK_ELSE) {
		default_ = p.parseDisjunction()
	}

	if !p.match(TK_END) {
		if default_.IsA(KInterval) && pyUpper(exprSQL(default_.This())) == "END" {
			default_ = chunkEColumn("interval")
		} else {
			p.raiseError("Expected END after CASE", p.prev)
		}
	}

	return p.expressionC(New(KCase, "this", expression, "ifs", ifs, "default", default_), comments)
}

// _parse_if (parser.py L7835)
func (p *Parser) baseParseIf() *Expr {
	var this *Expr
	if p.match(TK_L_PAREN) {
		args := p.parseCSV(func() *Expr {
			return p.parseAlias(p.parseAssignment(), true)
		}, TK_COMMA)
		this = p.validateExpression(chunkEFromArgList(KIf, args), args)
		p.matchRParen(nil)
	} else {
		index := p.index - 1

		if p.s.NO_PAREN_IF_COMMANDS && index == 0 {
			return p.parseAsCommand(p.prev)
		}

		condition := p.parseDisjunction()

		if condition == nil {
			p.retreat(index)
			return nil
		}

		p.match(TK_THEN)
		true_ := p.parseDisjunction()
		var false_ *Expr
		if p.match(TK_ELSE) {
			false_ = p.parseDisjunction()
		}
		p.match(TK_END)
		this = p.expression(New(KIf, "this", condition, "true", true_, "false", false_))
	}

	return this
}

// _parse_next_value_for (parser.py L7862)
func (p *Parser) parseNextValueFor() *Expr {
	if !p.matchTextSeq("VALUE", "FOR") {
		p.retreat(p.index - 1)
		return nil
	}

	this := p.parseColumn()
	// self._match(TokenType.OVER) and self._parse_wrapped(...) -> False when unmatched
	var order any = false
	if p.match(TK_OVER) {
		order = p.parseWrapped(func() *Expr { return p.parseOrder(nil, false) }, false)
	}
	return p.expression(New(KNextValueFor, "this", this, "order", order))
}

// _parse_extract (parser.py L7874)
func (p *Parser) baseParseExtract() *Expr {
	this := p.parseFunction(nil, false, true, false)
	if this == nil {
		this = p.parseVarOrString(true)
	}

	if p.match(TK_FROM) {
		return p.expression(New(KExtract, "this", this, "expression", p.parseBitwise()))
	}

	if !p.match(TK_COMMA) {
		p.raiseError("Expected FROM or comma after EXTRACT", p.prev)
	}

	return p.expression(New(KExtract, "this", this, "expression", p.parseBitwise()))
}

// _parse_gap_fill (parser.py L7885)
func (p *Parser) parseGapFill() *Expr {
	p.match(TK_TABLE)
	this := p.parseTable(false, false, nil, false, false, false, false)

	p.match(TK_COMMA)
	args := append([]*Expr{this}, p.parseCSV(func() *Expr { return p.parseLambda(false) }, TK_COMMA)...)

	gapFill := chunkEFromArgList(KGapFill, args)
	return p.validateExpression(gapFill, args)
}

// _parse_char (parser.py L7895)
func (p *Parser) parseChar() *Expr {
	expressions := p.parseCSV(p.parseAssignment, TK_COMMA)
	// self._match(TokenType.USING) and self._parse_charset_name() -> False when unmatched
	var charset any = false
	if p.match(TK_USING) {
		charset = p.parseCharsetName()
	}
	return p.expression(New(KChr, "expressions", expressions, "charset", charset))
}

var chunkECharsetNameTokens = newTokenSet(TK_BINARY, TK_IDENTIFIER)

// _parse_charset_name (parser.py L7903)
//
// Parse a charset name after USING or CHARACTER SET. Dialects that need to preserve quoting
// for specific name shapes override this.
func (p *Parser) baseParseCharsetName() *Expr {
	return p.parseVar(false, &chunkECharsetNameTokens, false)
}

// _parse_cast (parser.py L7912)
//
// `safe` is None (CAST) or True (SAFE_CAST / TRY_CAST) in sqlglot, so false maps to None.
func (p *Parser) parseCast(strict bool, safe bool) *Expr {
	var safeV any
	if safe {
		safeV = true
	}

	this := p.parseAssignment()

	if !p.match(TK_ALIAS) {
		if p.match(TK_COMMA) {
			return p.expression(New(KCastToStrType, "this", this, "to", p.parseString()))
		}

		p.raiseError("Expected AS after CAST", nil)
	}

	var fmtE *Expr
	to := p.parseTypes(false, false, true, true)

	var default_ *Expr
	if p.match(TK_DEFAULT) {
		default_ = p.parseBitwise()
		p.matchTextSeq("ON", "CONVERSION", "ERROR")
	}

	if p.matchAny(TK_FORMAT, TK_COMMA) {
		fmtString := p.parseWrapped(p.parseString, true)
		fmtE = p.parseAtTimeZone(fmtString)

		if to == nil {
			to = NewDataType(DT_UNKNOWN)
		}
		if DataType_TEMPORAL_TYPES.Has(to.DTypeOf()) {
			kind := KStrToTime
			if to.DTypeOf() == DT_DATE {
				kind = KStrToDate
			}
			s := ""
			if fmtString != nil {
				s = fmtString.ThisS()
			}
			mapping := p.d.S.FORMAT_MAPPING
			if len(mapping) == 0 {
				mapping = p.d.S.TIME_MAPPING
			}
			formatted, ok := formatTime(s, mapping, p.d.formatTrie)
			if !ok {
				// exp.Literal.string(None) stringifies the None
				formatted = "None"
			}
			this = p.expression(New(kind, "this", this, "format", LiteralString(formatted), "safe", safeV))

			if fmtE.IsA(KAtTimeZone) && this.IsA(KStrToTime) {
				this.Set("zone", fmtE.Arg("zone"))
			}
			return this
		}
	} else if to == nil {
		p.raiseError("Expected TYPE after CAST", nil)
	} else if to.IsA(KIdentifier) {
		to = chunkEDataTypeFromStr(to.Name(), p.d, true)
	} else if to.DTypeOf() == DT_CHAR && p.match(TK_CHARACTER_SET) {
		dt := NewDataType(DT_CHARACTER_SET)
		dt.Set("kind", p.parseVarOrString(false))
		to = dt
	}

	return p.buildCast(
		strict,
		"this", this,
		"to", to,
		"format", fmtE,
		"safe", safeV,
		"action", p.parseVarFromOptions(p.s.CAST_ACTIONS, false),
		"default", default_,
	)
}

// _parse_string_agg (parser.py L7970)
func (p *Parser) parseStringAgg() *Expr {
	var args []*Expr
	if p.match(TK_DISTINCT) {
		args = []*Expr{p.expression(New(KDistinct, "expressions", []*Expr{p.parseDisjunction()}))}
		if p.match(TK_COMMA) {
			args = append(args, p.parseCSV(p.parseDisjunction, TK_COMMA)...)
		}
	} else {
		args = p.parseCSV(p.parseDisjunction, TK_COMMA)
	}

	var onOverflow *Expr
	if p.matchTextSeq("ON", "OVERFLOW") {
		// trino: LISTAGG(expression [, separator] [ON OVERFLOW overflow_behavior])
		if p.matchTextSeq("ERROR") {
			onOverflow = VarExpr("ERROR")
		} else {
			p.matchTextSeq("TRUNCATE")
			this := p.parseString()
			withCount := p.matchTextSeq("WITH", "COUNT") || !p.matchTextSeq("WITHOUT", "COUNT")
			onOverflow = p.expression(New(KOverflowTruncateBehavior, "this", this, "with_count", withCount))
		}
	}

	index := p.index
	if !p.match(TK_R_PAREN) && len(args) > 0 {
		// postgres: STRING_AGG([DISTINCT] expression, separator [ORDER BY expression1 {ASC | DESC} [, ...]])
		// bigquery: STRING_AGG([DISTINCT] expression [, separator] [ORDER BY key [{ASC | DESC}] [, ... ]] [LIMIT n])
		// The order is parsed through `this` as a canonicalization for WITHIN GROUPs
		args[0] = p.parseLimit(p.parseOrder(args[0], false), false, false)
		return p.expression(New(KGroupConcat, "this", args[0], "separator", seqGet(args, 1)))
	}

	// Checks if we can parse an order clause: WITHIN GROUP (ORDER BY <order_by_expression_list> [ASC | DESC]).
	// This is done "manually", instead of letting _parse_window parse it into an exp.WithinGroup node, so that
	// the STRING_AGG call is parsed like in MySQL / SQLite and can thus be transpiled more easily to them.
	if !p.matchTextSeq("WITHIN", "GROUP") {
		p.retreat(index)
		return p.validateExpression(chunkEFromArgList(KGroupConcat, args), args)
	}

	// The corresponding match_r_paren will be called in parse_function (caller)
	p.matchLParen(nil)

	return p.expression(New(
		KGroupConcat,
		"this", p.parseOrder(seqGet(args, 0), false),
		"separator", seqGet(args, 1),
		"on_overflow", onOverflow,
	))
}

// _parse_convert (parser.py L8024)
//
// `safe` is None (CONVERT) or True (TRY_CONVERT) in sqlglot, so false maps to None.
func (p *Parser) baseParseConvert(strict bool, safe bool) *Expr {
	var safeV any
	if safe {
		safeV = true
	}

	this := p.parseBitwise()

	var to *Expr
	if p.match(TK_USING) {
		to = NewDataType(DT_CHARACTER_SET)
		to.Set("kind", p.parseCharsetName())
	} else if p.match(TK_COMMA) {
		to = p.parseTypes(false, false, true, false)
	}

	return p.buildCast(strict, "this", this, "to", to, "safe", safeV)
}

// _parse_xml_element (parser.py L8036)
func (p *Parser) parseXmlElement() *Expr {
	var evalname any
	var this *Expr
	if p.matchTextSeq("EVALNAME") {
		evalname = true
		this = p.parseBitwise()
	} else {
		p.matchTextSeq("NAME")
		this = p.parseIdVar(true, nil)
	}

	// self._match(TokenType.COMMA) and self._parse_csv(...) -> False when unmatched
	var expressions any = false
	if p.match(TK_COMMA) {
		expressions = p.parseCSV(p.parseBitwise, TK_COMMA)
	}

	return p.expression(New(KXMLElement, "this", this, "expressions", expressions, "evalname", evalname))
}

// _parse_xml_table (parser.py L8053)
func (p *Parser) parseXmlTable() *Expr {
	var namespaces, passing, columns any

	if p.matchTextSeq("XMLNAMESPACES", "(") {
		namespaces = p.parseXmlNamespace()
		p.matchTextSeq(")", ",")
	}

	this := p.parseString()

	if p.matchTextSeq("PASSING") {
		// The BY VALUE keywords are optional and are provided for semantic clarity
		p.matchTextSeq("BY", "VALUE")
		passing = p.parseCSV(p.parseColumn, TK_COMMA)
	}

	byRef := p.matchTextSeq("RETURNING", "SEQUENCE", "BY", "REF")

	if p.matchTextSeq("COLUMNS") {
		columns = p.parseCSV(p.parseFieldDef, TK_COMMA)
	}

	return p.expression(New(KXMLTable,
		"this", this, "namespaces", namespaces, "passing", passing, "columns", columns, "by_ref", byRef))
}

// _parse_xml_namespace (parser.py L8080)
func (p *Parser) parseXmlNamespace() []*Expr {
	namespaces := []*Expr{}

	for {
		var uri *Expr
		if p.match(TK_DEFAULT) {
			uri = p.parseString()
		} else {
			uri = p.parseAlias(p.parseString(), false)
		}
		namespaces = append(namespaces, p.expression(New(KXMLNamespace, "this", uri)))
		if !p.match(TK_COMMA) {
			break
		}
	}

	return namespaces
}

// _parse_decode (parser.py L8094)
func (p *Parser) parseDecode() *Expr {
	args := p.parseCSV(p.parseDisjunction, TK_COMMA)

	if len(args) < 3 {
		return p.expression(New(KDecode, "this", seqGet(args, 0), "charset", seqGet(args, 1)))
	}

	return p.expression(New(KDecodeCase, "expressions", args))
}

// _parse_json_key_value (parser.py L8102)
func (p *Parser) parseJsonKeyValue() *Expr {
	p.matchTextSeq("KEY")
	key := p.parseColumn()
	p.matchSet(p.s.JSON_KEY_VALUE_SEPARATOR_TOKENS)
	p.matchTextSeq("VALUE")
	value := p.parseBitwise()

	if key == nil && value == nil {
		return nil
	}
	return p.expression(New(KJSONKeyValue, "this", key, "expression", value))
}

// _parse_format_json (parser.py L8113)
func (p *Parser) parseFormatJson(this *Expr) *Expr {
	if this == nil || !p.matchTextSeq("FORMAT", "JSON") {
		return this
	}

	return p.expression(New(KFormatJson, "this", this))
}

// _parse_on_condition (parser.py L8119)
func (p *Parser) parseOnCondition() *Expr {
	// MySQL uses "X ON EMPTY Y ON ERROR" (e.g. JSON_VALUE) while Oracle uses the opposite (e.g. JSON_EXISTS)
	tokens := p.s.ON_CONDITION_TOKENS.Sorted()
	var empty, errorV any
	if p.d.S.ON_CONDITION_EMPTY_BEFORE_ERROR {
		empty = p.parseOnHandling("EMPTY", tokens...)
		errorV = p.parseOnHandling("ERROR", tokens...)
	} else {
		errorV = p.parseOnHandling("ERROR", tokens...)
		empty = p.parseOnHandling("EMPTY", tokens...)
	}

	null := p.parseOnHandling("NULL", tokens...)

	if !truthy(empty) && !truthy(errorV) && !truthy(null) {
		return nil
	}

	return p.expression(New(KOnCondition, "empty", empty, "error", errorV, "null", null))
}

// _parse_on_handling (parser.py L8135)
//
// Returns nil (None), a string or an *Expr.
func (p *Parser) parseOnHandling(on string, values ...string) any {
	// Parses the "X ON Y" or "DEFAULT <expr> ON Y syntax, e.g. NULL ON NULL (Oracle, T-SQL, MySQL)
	for _, value := range values {
		if p.matchTextSeq(value, "ON", on) {
			return value + " ON " + on
		}
	}

	index := p.index
	if p.match(TK_DEFAULT) {
		defaultValue := p.parseBitwise()
		if p.matchTextSeq("ON", on) {
			if defaultValue == nil {
				return nil
			}
			return defaultValue
		}

		p.retreat(index)
	}

	return nil
}

// _parse_json_object (parser.py L8157)
func (p *Parser) baseParseJsonObject(agg bool) *Expr {
	star := p.parseStar()
	var expressions []*Expr
	if star != nil {
		expressions = []*Expr{star}
	} else {
		expressions = p.parseCSV(func() *Expr { return p.parseFormatJson(p.parseJsonKeyValue()) }, TK_COMMA)
	}
	nullHandling := p.parseOnHandling("NULL", "NULL", "ABSENT")

	var uniqueKeys any // None / True / False
	if p.matchTextSeq("WITH", "UNIQUE") {
		uniqueKeys = true
	} else if p.matchTextSeq("WITHOUT", "UNIQUE") {
		uniqueKeys = false
	}

	p.matchTextSeq("KEYS")

	// self._match_text_seq(...) and self._parse_...() -> False when unmatched
	var returnType any = false
	if p.matchTextSeq("RETURNING") {
		returnType = p.parseFormatJson(p.parseType(true, false))
	}
	var encoding any = false
	if p.matchTextSeq("ENCODING") {
		encoding = p.parseVar(false, nil, false)
	}

	kind := KJSONObject
	if agg {
		kind = KJSONObjectAgg
	}
	return p.expression(New(
		kind,
		"expressions", expressions,
		"null_handling", nullHandling,
		"unique_keys", uniqueKeys,
		"return_type", returnType,
		"encoding", encoding,
	))
}

// _parse_json_column_def (parser.py L8190)
//
// Note: this is currently incomplete; it only implements the "JSON_value_column" part
func (p *Parser) parseJsonColumnDef() *Expr {
	var this, kind *Expr
	var ordinality, nested any
	if !p.matchTextSeq("NESTED") {
		this = p.parseIdVar(true, nil)
		ordinality = p.matchPair(TK_FOR, TK_ORDINALITY)
		kind = p.parseTypes(false, false, false, false)
	} else {
		nested = true
	}

	formatJson := p.matchTextSeq("FORMAT", "JSON")
	// self._match_text_seq("PATH") and self._parse_string() -> False when unmatched
	var path any = false
	if p.matchTextSeq("PATH") {
		path = p.parseString()
	}
	var nestedSchema *Expr
	if nested != nil {
		nestedSchema = p.parseJsonSchema()
	}

	return p.expression(New(
		KJSONColumnDef,
		"this", this,
		"kind", kind,
		"path", path,
		"nested_schema", nestedSchema,
		"ordinality", ordinality,
		"format_json", formatJson,
	))
}

// _parse_json_schema (parser.py L8217)
func (p *Parser) parseJsonSchema() *Expr {
	p.matchTextSeq("COLUMNS")
	return p.expression(New(KJSONSchema,
		"expressions", p.parseWrappedCSV(p.parseJsonColumnDef, TK_COMMA, true)))
}

// _parse_json_table (parser.py L8225)
func (p *Parser) parseJsonTable() *Expr {
	this := p.parseFormatJson(p.parseBitwise())
	// self._match(TokenType.COMMA) and self._parse_string() -> False when unmatched
	var path any = false
	if p.match(TK_COMMA) {
		path = p.parseString()
	}
	errorHandling := p.parseOnHandling("ERROR", "ERROR", "NULL")
	emptyHandling := p.parseOnHandling("EMPTY", "ERROR", "NULL")
	schema := p.parseJsonSchema()

	return New(
		KJSONTable,
		"this", this,
		"schema", schema,
		"path", path,
		"error_handling", errorHandling,
		"empty_handling", emptyHandling,
	)
}

// _parse_match_against (parser.py L8240)
func (p *Parser) parseMatchAgainst() *Expr {
	var expressions []*Expr
	if p.matchTextSeq("TABLE") {
		// parse SingleStore MATCH(TABLE ...) syntax
		// https://docs.singlestore.com/cloud/reference/sql-reference/full-text-search-functions/match/
		expressions = []*Expr{}
		table := p.parseTable(false, false, nil, false, false, false, false)
		if table != nil {
			expressions = []*Expr{table}
		}
	} else {
		expressions = p.parseCSV(p.parseColumn, TK_COMMA)
	}

	p.matchTextSeq(")", "AGAINST", "(")

	this := p.parseString()

	var modifier any
	if p.matchTextSeq("IN", "NATURAL", "LANGUAGE", "MODE") {
		m := "IN NATURAL LANGUAGE MODE"
		if p.matchTextSeq("WITH", "QUERY", "EXPANSION") {
			m = m + " WITH QUERY EXPANSION"
		}
		modifier = m
	} else if p.matchTextSeq("IN", "BOOLEAN", "MODE") {
		modifier = "IN BOOLEAN MODE"
	} else if p.matchTextSeq("WITH", "QUERY", "EXPANSION") {
		modifier = "WITH QUERY EXPANSION"
	}

	return p.expression(New(KMatchAgainst, "this", this, "expressions", expressions, "modifier", modifier))
}

// _parse_open_json (parser.py L8271)
//
// https://learn.microsoft.com/en-us/sql/t-sql/functions/openjson-transact-sql?view=sql-server-ver16
func (p *Parser) parseOpenJson() *Expr {
	this := p.parseBitwise()
	// self._match(TokenType.COMMA) and self._parse_string() -> False when unmatched
	var path any = false
	if p.match(TK_COMMA) {
		path = p.parseString()
	}

	parseOpenJsonColumnDef := func() *Expr {
		this := p.parseField(true, nil, false)
		kind := p.parseTypes(false, false, true, false)
		path := p.parseString()
		asJSON := p.matchPair(TK_ALIAS, TK_JSON)

		return p.expression(New(KOpenJSONColumnDef, "this", this, "kind", kind, "path", path, "as_json", asJSON))
	}

	var expressions any // None
	if p.matchPair(TK_R_PAREN, TK_WITH) {
		p.matchLParen(nil)
		expressions = p.parseCSV(parseOpenJsonColumnDef, TK_COMMA)
	}

	return p.expression(New(KOpenJSON, "this", this, "path", path, "expressions", expressions))
}

// _parse_position (parser.py L8292)
func (p *Parser) baseParsePosition(haystackFirst bool) *Expr {
	args := p.parseCSV(p.parseBitwise, TK_COMMA)

	if p.match(TK_IN) {
		return p.expression(New(KStrPosition, "this", p.parseBitwise(), "substr", seqGet(args, 0)))
	}

	var haystack, needle *Expr
	if haystackFirst {
		haystack = seqGet(args, 0)
		needle = seqGet(args, 1)
	} else {
		haystack = seqGet(args, 1)
		needle = seqGet(args, 0)
	}

	return p.expression(New(KStrPosition, "this", haystack, "substr", needle, "position", seqGet(args, 2)))
}

// _parse_join_hint (parser.py L8311)
func (p *Parser) parseJoinHint(funcName string) *Expr {
	args := p.parseCSV(func() *Expr { return p.parseTable(false, false, nil, false, false, false, false) }, TK_COMMA)
	return New(KJoinHint, "this", pyUpper(funcName), "expressions", args)
}

// _parse_substring (parser.py L8315)
func (p *Parser) baseParseSubstring() *Expr {
	// Postgres supports the form: substring(string [from int] [for int])
	// (despite being undocumented, the reverse order also works)
	// https://www.postgresql.org/docs/9.1/functions-string.html @ Table 9-6

	args := p.parseCSV(p.parseBitwise, TK_COMMA)

	var start, length *Expr

	for p.curr.ok() {
		if p.match(TK_FROM) {
			start = p.parseBitwise()
		} else if p.match(TK_FOR) {
			if start == nil {
				start = LiteralInt(1)
			}
			length = p.parseBitwise()
		} else {
			break
		}
	}

	if start != nil {
		args = append(args, start)
	}
	if length != nil {
		args = append(args, length)
	}

	return p.validateExpression(chunkEFromArgList(KSubstring, args), args)
}

// _parse_trim (parser.py L8341)
func (p *Parser) parseTrim() *Expr {
	// https://www.w3resource.com/sql/character-functions/trim.php
	// https://docs.oracle.com/javadb/10.8.3.0/ref/rreftrimfunc.html

	var position any
	var collation, expression *Expr

	if p.matchTextSet(p.s.TRIM_TYPES) {
		position = upperText(p.prev)
	}

	this := p.parseBitwise()
	if p.matchAny(TK_FROM, TK_COMMA) {
		invertOrder := p.prev.Type == TK_FROM || p.s.TRIM_PATTERN_FIRST
		expression = p.parseBitwise()

		if invertOrder {
			this, expression = expression, this
		}
	}

	if p.match(TK_COLLATE) {
		collation = p.parseBitwise()
	}

	return p.expression(New(KTrim, "this", this, "position", position, "expression", expression, "collation", collation))
}

// _parse_window_clause (parser.py L8367)
func (p *Parser) parseWindowClause() []*Expr {
	if p.match(TK_WINDOW) {
		return p.parseCSV(p.parseNamedWindow, TK_COMMA)
	}
	return nil
}

// _parse_named_window (parser.py L8370)
func (p *Parser) parseNamedWindow() *Expr {
	return p.parseWindow(p.parseIdVar(true, nil), true)
}

// _parse_respect_or_ignore_nulls (parser.py L8373)
func (p *Parser) parseRespectOrIgnoreNulls(this *Expr) *Expr {
	if p.curr.Type == TK_VAR {
		if p.matchTextSeq("IGNORE", "NULLS") {
			return p.expression(New(KIgnoreNulls, "this", this))
		}
		if p.matchTextSeq("RESPECT", "NULLS") {
			return p.expression(New(KRespectNulls, "this", this))
		}
	}
	return this
}

// _parse_having_max (parser.py L8381)
func (p *Parser) parseHavingMax(this *Expr) *Expr {
	if p.match(TK_HAVING) {
		p.matchTexts("MAX", "MIN")
		max := upperText(p.prev) != "MIN"
		return p.expression(New(KHavingMax, "this", this, "expression", p.parseColumn(), "max", max))
	}

	return this
}

// _parse_window (parser.py L8391)
func (p *Parser) baseParseWindow(this *Expr, alias bool) *Expr {
	fn := this
	var comments []string
	if fn != nil {
		comments = fn.Comments
	}

	// T-SQL allows the OVER (...) syntax after WITHIN GROUP.
	// https://learn.microsoft.com/en-us/sql/t-sql/functions/percentile-disc-transact-sql?view=sql-server-ver16
	if p.matchTextSeq("WITHIN", "GROUP") {
		order := p.parseWrapped(func() *Expr { return p.parseOrder(nil, false) }, false)
		this = p.expression(New(KWithinGroup, "this", this, "expression", order))
	}

	if p.matchPair(TK_FILTER, TK_L_PAREN) {
		p.match(TK_WHERE)
		this = p.expression(New(KFilter, "this", this, "expression", p.parseWhere(true)))
		p.matchRParen(nil)
	}

	// SQL spec defines an optional [ { IGNORE | RESPECT } NULLS ] OVER
	// Some dialects choose to implement and some do not.
	// https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html

	// There is some code above in _parse_lambda that handles
	//   SELECT FIRST_VALUE(TABLE.COLUMN IGNORE|RESPECT NULLS) OVER ...

	// The below changes handle
	//   SELECT FIRST_VALUE(TABLE.COLUMN) IGNORE|RESPECT NULLS OVER ...

	// Oracle allows both formats
	//   (https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/img_text/first_value.html)
	//   and Snowflake chose to do the same for familiarity
	//   https://docs.snowflake.com/en/sql-reference/functions/first_value.html#usage-notes
	if this.IsA(KAggFunc) {
		ignoreRespect := this.Find(KIgnoreNulls, KRespectNulls)

		if ignoreRespect != nil && ignoreRespect != this {
			ignoreRespect.Replace(ignoreRespect.This())
			this = p.expression(New(ignoreRespect.Kind(), "this", this))
		}
	}

	this = p.parseRespectOrIgnoreNulls(this)

	// bigquery select from window x AS (partition by ...)
	var over any
	if alias {
		over = nil
		p.match(TK_ALIAS)
	} else if !p.matchSet(p.s.WINDOW_BEFORE_PAREN_TOKENS) {
		return this
	} else {
		over = upperText(p.prev)
	}

	if len(comments) > 0 && fn != nil {
		fn.PopComments()
	}

	if !p.match(TK_L_PAREN) {
		return p.expressionC(New(KWindow, "this", this, "alias", p.parseIdVar(false, nil), "over", over), comments)
	}

	windowAlias := p.parseIdVar(false, &p.s.WINDOW_ALIAS_TOKENS)

	var first any // None / True / False
	if p.match(TK_FIRST) {
		first = true
	}
	if p.matchTextSeq("LAST") {
		first = false
	}

	partition, order := p.parsePartitionAndOrder()
	kind := ""
	if p.matchAny(TK_ROWS, TK_RANGE) || p.matchTextSeq("GROUPS") {
		kind = p.prev.Text
	}

	var spec *Expr
	if kind != "" {
		p.match(TK_BETWEEN)
		start := p.parseWindowSpec()

		var end windowSpec // {} -> value/side are None
		if p.match(TK_AND) {
			end = p.parseWindowSpec()
		}
		var exclude *Expr
		if p.matchTextSeq("EXCLUDE") {
			exclude = p.parseVarFromOptions(p.s.WINDOW_EXCLUDE_OPTIONS, true)
		}

		spec = p.expression(New(
			KWindowSpec,
			"kind", kind,
			"start", start.value,
			"start_side", chunkEWindowSide(start),
			"end", end.value,
			"end_side", chunkEWindowSide(end),
			"exclude", exclude,
		))
	}

	p.matchRParen(nil)

	window := p.expressionC(New(
		KWindow,
		"this", this,
		"partition_by", partition,
		"order", order,
		"spec", spec,
		"alias", windowAlias,
		"over", over,
		"first", first,
	), comments)

	// This covers Oracle's FIRST/LAST syntax: aggregate KEEP (...) OVER (...)
	if p.matchSetNoAdvance(p.s.WINDOW_BEFORE_PAREN_TOKENS) {
		return p.parseWindow(window, alias)
	}

	return window
}

// chunkEWindowSide returns the "side" entry of a window spec dict (None when absent).
func chunkEWindowSide(ws windowSpec) any {
	if ws.hasSide {
		return ws.side
	}
	return nil
}

// _parse_partition_and_order (parser.py L8504)
func (p *Parser) baseParsePartitionAndOrder() ([]*Expr, *Expr) {
	partition := p.parsePartitionBy()
	order := p.parseOrder(nil, false)
	return partition, order
}

// _parse_window_spec (parser.py L8509)
func (p *Parser) parseWindowSpec() windowSpec {
	p.match(TK_BETWEEN)

	var value any
	if p.matchTextSeq("UNBOUNDED") {
		value = "UNBOUNDED"
	} else if p.matchTextSeq("CURRENT", "ROW") {
		value = "CURRENT ROW"
	} else if v := p.parseBitwise(); v != nil {
		value = v
	}

	ws := windowSpec{value: value}
	if p.matchTextSet(p.s.WINDOW_SIDES) {
		ws.side = p.prev.Text
		ws.hasSide = true
	}
	return ws
}

// _parse_alias (parser.py L8521)
func (p *Parser) baseParseAlias(this *Expr, explicit bool) *Expr {
	// In some dialects, LIMIT and OFFSET can act as both identifiers and keywords (clauses)
	// so this section tries to parse the clause version and if it fails, it treats the token
	// as an identifier (alias)
	if p.canParseLimitOrOffset() {
		return this
	}

	// WINDOW is in ID_VAR_TOKENS, so it can be consumed as an implicit alias. Detect the
	// named-window clause shape (`WINDOW <ident> AS (...)`) and avoid swallowing it.
	if p.canParseNamedWindow() {
		return this
	}

	anyToken := p.match(TK_ALIAS)
	comments := p.prevComments
	// In sqlglot `comments` is the very list object of the previous token (unless it was already
	// consumed), so the `comments.extend(...)` below also mutates that token's comments.
	var commentsTok *Token
	if comments != nil && p.prev.ok() {
		commentsTok = p.prev
	}

	if explicit && !anyToken {
		return this
	}

	if p.match(TK_L_PAREN) {
		aliases := p.expressionC(New(
			KAliases,
			"this", this,
			"expressions", p.parseCSV(func() *Expr { return p.parseIdVar(anyToken, nil) }, TK_COMMA),
		), comments)
		p.matchRParen(aliases)
		return aliases
	}

	alias := p.parseIdVar(anyToken, &p.s.ALIAS_TOKENS)
	if alias == nil && p.s.STRING_ALIASES {
		alias = p.parseStringAsIdentifier()
	}

	if alias != nil {
		comments = append(comments, alias.PopComments()...)
		if commentsTok != nil {
			commentsTok.Comments = comments
		}
		this = p.expressionC(New(KAlias, "this", this, "alias", alias), comments)
		column := this.This()

		// Moves the comment next to the alias in `expr /* comment */ AS alias`
		if len(this.Comments) == 0 && column != nil && len(column.Comments) > 0 {
			this.Comments = column.PopComments()
		}
	}

	return this
}

// _parse_id_var (parser.py L8564)
func (p *Parser) baseParseIdVar(anyToken bool, tokens *TokenSet) *Expr {
	expression := p.parseIdentifier()
	if expression == nil {
		matched := anyToken && p.advanceAny(false) != nil
		if !matched {
			ts := p.s.ID_VAR_TOKENS
			if tokens != nil && *tokens != (TokenSet{}) {
				ts = *tokens
			}
			matched = p.matchSet(ts)
		}
		if matched {
			quoted := p.prev.Type == TK_STRING
			expression = p.identifierExpression(nil, triOf(quoted))
		}
	}

	return expression
}

// _parse_string (parser.py L8578)
func (p *Parser) parseString() *Expr {
	if fn, ok := p.s.STRING_PARSERS[p.curr.Type]; ok {
		p.advance(1)
		return fn(p, p.prev)
	}
	return p.parsePlaceholder()
}

// _parse_string_as_identifier (parser.py L8583)
func (p *Parser) parseStringAsIdentifier() *Expr {
	if !p.match(TK_STRING) {
		return nil
	}
	output := ToIdentifier(p.prev.Text, boolp(true))
	output.updatePositionsTok(p.prev)
	return output
}

// _parse_number (parser.py L8590)
func (p *Parser) parseNumber() *Expr {
	if fn, ok := p.s.NUMERIC_PARSERS[p.curr.Type]; ok {
		p.advance(1)
		return fn(p, p.prev)
	}
	return p.parsePlaceholder()
}

// _parse_identifier (parser.py L8595)
func (p *Parser) parseIdentifier() *Expr {
	if p.match(TK_IDENTIFIER) {
		return p.identifierExpression(nil, TriTrue)
	}
	return p.parsePlaceholder()
}

// _parse_var (parser.py L8600)
func (p *Parser) parseVar(anyToken bool, tokens *TokenSet, upper bool) *Expr {
	if (anyToken && p.advanceAny(false) != nil) ||
		p.match(TK_VAR) ||
		(tokens != nil && *tokens != (TokenSet{}) && p.matchSet(*tokens)) {
		text := p.prev.Text
		if upper {
			text = upperText(p.prev)
		}
		return p.expression(New(KVar, "this", text))
	}
	return p.parsePlaceholder()
}

// _parse_var_or_string (parser.py L8622)
func (p *Parser) parseVarOrString(upper bool) *Expr {
	if s := p.parseString(); s != nil {
		return s
	}
	return p.parseVar(true, nil, upper)
}

// _parse_primary_or_var (parser.py L8625)
func (p *Parser) parsePrimaryOrVar() *Expr {
	if e := p.parsePrimary(); e != nil {
		return e
	}
	return p.parseVar(true, nil, false)
}

// _parse_null (parser.py L8628)
func (p *Parser) parseNull() *Expr {
	if p.matchAny(TK_NULL, TK_UNKNOWN) {
		return p.s.PRIMARY_PARSERS[TK_NULL](p, p.prev)
	}
	return p.parsePlaceholder()
}

// _parse_star (parser.py L8640)
func (p *Parser) parseStar() *Expr {
	if p.match(TK_STAR) {
		return p.s.PRIMARY_PARSERS[TK_STAR](p, p.prev)
	}
	return p.parsePlaceholder()
}

// _parse_parameter (parser.py L8645)
func (p *Parser) baseParseParameter() *Expr {
	this := p.parseIdentifier()
	if this == nil {
		this = p.parsePrimaryOrVar()
	}
	return p.expression(New(KParameter, "this", this))
}

// _parse_placeholder (parser.py L8649)
func (p *Parser) parsePlaceholder() *Expr {
	if fn, ok := p.s.PLACEHOLDER_PARSERS[p.curr.Type]; ok {
		p.advance(1)
		placeholder := fn(p)
		if placeholder != nil {
			return placeholder
		}
		p.advance(-1)
	}
	return nil
}

// _parse_star_op (parser.py L8657)
func (p *Parser) parseStarOp(keywords ...string) []*Expr {
	if !p.matchTexts(keywords...) {
		return nil
	}
	if p.matchNoAdvance(TK_L_PAREN) {
		return p.parseWrappedCSV(p.parseExpression, TK_COMMA, false)
	}

	expression := p.parseAlias(p.parseDisjunction(), true)
	if expression != nil {
		return []*Expr{expression}
	}
	return nil
}

// _parse_select_or_expression (parser.py L8706)
func (p *Parser) parseSelectOrExpression(alias bool) *Expr {
	var this *Expr
	if alias {
		this = p.parseAlias(p.parseAssignment(), true)
	} else {
		this = p.parseAssignment()
	}
	if e := p.parseSetOperations(this); e != nil {
		return e
	}
	return p.parseSelect(false, false, true, true, true, nil)
}

// _parse_ddl_select (parser.py L8716)
func (p *Parser) parseDdlSelect() *Expr {
	return p.parseQueryModifiers(
		p.parseSetOperations(p.parseSelect(true, false, false, true, true, nil)),
	)
}

// ---------------------------------------------------------------------------------------------
// Chunk E helpers (ports of module-level sqlglot helpers; to be unified later).

// chunkETokenAt mirrors helper.seq_get(self._tokens, i), including Python negative indexing.
func chunkETokenAt(tokens []*Token, i int) *Token {
	if i < 0 {
		i += len(tokens)
	}
	if i < 0 || i >= len(tokens) {
		return nil
	}
	return tokens[i]
}

// chunkEColumn mirrors exp.column(col) for a plain string column name.
func chunkEColumn(col string) *Expr {
	return New(KColumn, "this", ToIdentifier(col, nil), "table", nil, "db", nil, "catalog", nil)
}

// chunkEFromArgList mirrors Func.from_arg_list.
func chunkEFromArgList(k Kind, args []*Expr) *Expr {
	argTypes := k.ArgTypes()
	var kv []any
	if k.isVarLenArgs() && len(argTypes) > 0 {
		// If this function supports variable length argument treat the last argument as such.
		nonVar := argTypes[:len(argTypes)-1]
		n := 0
		for ; n < len(args) && n < len(nonVar); n++ {
			kv = append(kv, nonVar[n].name, args[n])
		}
		rest := []*Expr{}
		if len(nonVar) < len(args) {
			rest = append(rest, args[len(nonVar):]...)
		}
		kv = append(kv, argTypes[len(argTypes)-1].name, rest)
	} else {
		for i := 0; i < len(args) && i < len(argTypes); i++ {
			kv = append(kv, argTypes[i].name, args[i])
		}
	}
	return New(k, kv...)
}

// chunkEBuildArrayConstructor mirrors parser.build_array_constructor.
func chunkEBuildArrayConstructor(k Kind, args []*Expr, bracketKind TokenType, d *Dialect) *Expr {
	arrayExp := New(k, "expressions", args)

	if k == KArray && d.S.HAS_DISTINCT_ARRAY_CONSTRUCTORS {
		arrayExp.Set("bracket_notation", bracketKind == TK_L_BRACKET)
	}

	return arrayExp
}

// chunkEDataTypeFromStr mirrors exp.DataType.from_str(dtype, dialect=d, udt=udt).
func chunkEDataTypeFromStr(dtype string, d *Dialect, udt bool) *Expr {
	if pyUpper(dtype) == "UNKNOWN" {
		return New(KDataType, "this", DT_UNKNOWN)
	}
	level := ErrorLevelIgnore
	result, err := d.ParseOneInto(KDataType, dtype, &ParseOptions{ErrorLevel: &level})
	if err != nil {
		if pe, ok := err.(*ParseError); ok {
			if udt {
				return New(KDataType, "this", DT_USERDEFINED, "kind", dtype)
			}
			panic(parsePanic{pe})
		}
		panic(err)
	}
	return result
}

// chunkEApplyIndexOffset mirrors exp.apply_index_offset (expressions/builders.py).
//
// NOTE: sqlglot uses optimizer.annotate_types and optimizer.simplify here. Neither is ported yet, so
// chunkEInferType / chunkESimplifyAddOffset only cover the common cases (integer literal arithmetic,
// casts, columns, arrays/maps/structs); types are not written back onto the nodes.
func chunkEApplyIndexOffset(this *Expr, expressions []*Expr, offset int, d *Dialect) []*Expr {
	return dhApplyIndexOffset(this, expressions, offset, d)
}
