package sqlengine

// Port of sqlglot/dialects/oracle.py, sqlglot/parsers/oracle.py and sqlglot/generators/oracle.py
// (sqlglot v30.13.0). Data-only class attributes live in the generated zz_*_settings.go files.

func init() { registerCustomizer("oracle", customizeOracle) }

func customizeOracle(d *Dialect) {
	// ---- Dialect class ----
	d.hooks.canQuote = oracleCanQuote

	// ---- OracleParser ----
	P := d.P

	delete(P.FUNCTIONS, "TO_BOOLEAN")
	P.FUNCTIONS["CONVERT"] = fromArgList(KConvertToCharset)
	P.FUNCTIONS["L2_DISTANCE"] = fromArgList(KEuclideanDistance)
	P.FUNCTIONS["NVL"] = func(args []*Expr, _ *Dialect) *Expr { return buildCoalesce(args, true, nil) }
	P.FUNCTIONS["SQUARE"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KPow, "this", seqGet(args, 0), "expression", LiteralInt(2))
	}
	P.FUNCTIONS["TO_CHAR"] = buildTimetostrOrTochar
	P.FUNCTIONS["TO_TIMESTAMP"] = oracleBuildToTimestamp
	P.FUNCTIONS["TO_DATE"] = buildFormattedTime(KStrToDate, "", nil)
	P.FUNCTIONS["TRUNC"] = func(args []*Expr, d *Dialect) *Expr {
		return buildTrunc(args, d, false, "DD", true, false)
	}

	P.NO_PAREN_FUNCTION_PARSERS["NEXT"] = func(p *Parser) *Expr { return p.parseNextValueFor() }
	P.NO_PAREN_FUNCTION_PARSERS["PRIOR"] = func(p *Parser) *Expr {
		return p.expression(New(KPrior, "this", p.parseBitwise()))
	}
	P.NO_PAREN_FUNCTION_PARSERS["SYSDATE"] = func(p *Parser) *Expr {
		return p.expression(New(KCurrentTimestamp, "sysdate", true))
	}
	P.NO_PAREN_FUNCTION_PARSERS["DBMS_RANDOM"] = oracleParseDbmsRandom

	delete(P.FUNCTION_PARSERS, "CONVERT")
	P.FUNCTION_PARSERS["JSON_ARRAY"] = oracleParseOracleJSONArray
	P.FUNCTION_PARSERS["JSON_ARRAYAGG"] = oracleParseOracleJSONArrayagg
	P.FUNCTION_PARSERS["JSON_EXISTS"] = oracleParseJSONExists
	P.FUNCTION_PARSERS["LISTAGG"] = func(p *Parser) *Expr { return p.parseStringAgg() }
	P.FUNCTION_PARSERS["TO_NUMBER"] = oracleParseToNumber

	P.PROPERTY_PARSERS["GLOBAL"] = noKwargsE(func(p *Parser) *Expr {
		if !p.matchTextSeq("TEMPORARY") {
			return nil
		}
		return p.expression(New(KTemporaryProperty, "this", "GLOBAL"))
	})
	P.PROPERTY_PARSERS["PRIVATE"] = noKwargsE(func(p *Parser) *Expr {
		if !p.matchTextSeq("TEMPORARY") {
			return nil
		}
		return p.expression(New(KTemporaryProperty, "this", "PRIVATE"))
	})
	P.PROPERTY_PARSERS["FORCE"] = noKwargsE(func(p *Parser) *Expr {
		return p.expression(New(KForceProperty))
	})

	P.QUERY_MODIFIER_PARSERS[TK_ORDER_SIBLINGS_BY] = func(p *Parser) (string, any) {
		return "order", anyExpr(p.parseOrder(nil, false))
	}
	P.QUERY_MODIFIER_PARSERS[TK_WITH] = func(p *Parser) (string, any) {
		return "options", ensureList(oracleParseQueryRestrictions(p))
	}

	// TYPE_LITERAL_PARSERS is redefined (not merged with the parent's table).
	P.TYPE_LITERAL_PARSERS = map[DType]typeLiteralParseFn{
		DT_DATE: func(p *Parser, this, _ *Expr) *Expr {
			return p.expression(New(KDateStrToDate, "this", this))
		},
		// https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/NLS_TIMESTAMP_FORMAT.html
		DT_TIMESTAMP: func(p *Parser, this, _ *Expr) *Expr {
			return oracleBuildToTimestampFmtStr(this, `"%Y-%m-%d %H:%M:%S.%f"`, p.d)
		},
	}

	P.h.parseHintFunctionCall = oracleParseHintFunctionCall
	P.h.parseInto = oracleParseInto
	P.h.parseConnectWithPrior = oracleParseConnectWithPrior
	P.h.parseColumnOps = oracleParseColumnOps
	P.h.parseInsertTable = oracleParseInsertTable

	// ---- OracleGenerator ----
	G := d.G

	// AFTER_HAVING_MODIFIER_TRANSFORMS = generator.AFTER_HAVING_MODIFIER_TRANSFORMS (module level)
	for _, k := range []string{"cluster", "distribute", "sort"} {
		delete(G.AFTER_HAVING_MODIFIER_TRANSFORMS, k)
	}
	keys := make([]string, 0, len(G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS))
	for _, k := range G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS {
		if k != "cluster" && k != "distribute" && k != "sort" {
			keys = append(keys, k)
		}
	}
	G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS = keys

	T := G.TRANSFORMS
	T[KGroupConcat] = func(g *Generator, e *Expr) string {
		return groupconcatSQL(g, e, "LISTAGG", ",", true, true)
	}
	T[KDateStrToDate] = func(g *Generator, e *Expr) string {
		return g.fn("TO_DATE", e.Arg("this"), LiteralString("YYYY-MM-DD"))
	}
	T[KDateTrunc] = func(g *Generator, e *Expr) string {
		return g.fn("TRUNC", e.Arg("this"), e.ArgE("unit"))
	}
	T[KEuclideanDistance] = renameFunc("L2_DISTANCE")
	T[KILike] = noIlikeSQL
	T[KLogicalOr] = renameFunc("MAX")
	T[KLogicalAnd] = renameFunc("MIN")
	T[KMod] = renameFunc("MOD")
	T[KRand] = renameFunc("DBMS_RANDOM.VALUE")
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		transformEliminateDistinctOn,
		transformEliminateQualify,
	}, nil)
	T[KStrPosition] = func(g *Generator, e *Expr) string {
		return strpositionSQL(g, e, "INSTR", true, true, true)
	}
	T[KStrToTime] = func(g *Generator, e *Expr) string {
		return g.fn("TO_TIMESTAMP", e.Arg("this"), oracleFormatTimeArg(g, e))
	}
	T[KStrToDate] = func(g *Generator, e *Expr) string {
		return g.fn("TO_DATE", e.Arg("this"), oracleFormatTimeArg(g, e))
	}
	T[KSubquery] = func(g *Generator, e *Expr) string { return g.subquerySQL(e, " ") }
	T[KSubstring] = renameFunc("SUBSTR")
	T[KTable] = func(g *Generator, e *Expr) string { return g.tableSQL(e, " ") }
	T[KTableSample] = func(g *Generator, e *Expr) string { return g.tablesampleSQL(e, "") }
	T[KTemporaryProperty] = func(_ *Generator, e *Expr) string {
		name := e.Name()
		if name == "" {
			name = "GLOBAL"
		}
		return name + " TEMPORARY"
	}
	T[KTimeToStr] = func(g *Generator, e *Expr) string {
		return g.fn("TO_CHAR", e.Arg("this"), oracleFormatTimeArg(g, e))
	}
	T[KToChar] = func(g *Generator, e *Expr) string { return g.functionFallbackSQL(e) }
	T[KTrim] = oracleTrimSQL
	T[KUnicode] = func(g *Generator, e *Expr) string {
		return "ASCII(UNISTR(" + g.sql(e.Arg("this")) + "))"
	}
	T[KUnixToTime] = func(g *Generator, e *Expr) string {
		return "TO_DATE('1970-01-01', 'YYYY-MM-DD') + (" + g.sqlKey(e, "this") + " / 86400)"
	}
	T[KUtcTimestamp] = renameFunc("UTC_TIMESTAMP")
	T[KUtcTime] = renameFunc("UTC_TIME")
	T[KSystimestamp] = func(_ *Generator, _ *Expr) string { return "SYSTIMESTAMP" }

	G.methods[KCurrentTimestamp] = oracleCurrenttimestampSQL
	G.methods[KCoalesce] = oracleCoalesceSQL
	G.methods[KIsAscii] = oracleIsasciiSQL

	G.h.offsetSQL = oracleOffsetSQL
	G.h.addColumnSQL = oracleAddColumnSQL
	G.h.queryoptionSQL = oracleQueryoptionSQL
	G.h.intoSQL = oracleIntoSQL
	G.h.hintSQL = oracleHintSQL
	G.h.intervalSQL = oracleIntervalSQL
	G.h.tonumberSQL = oracleTonumberSQL
	G.h.columndefSQL = oracleColumndefSQL
}

// ---------------------------------------------------------------------------------------------
// Dialect

// oracleCanQuote mirrors Oracle.can_quote.
func oracleCanQuote(d *Dialect, id *Expr, identify string) bool {
	// Disable quoting for pseudocolumns as it may break queries e.g
	// `WHERE "ROWNUM" = ...` does not work but `WHERE ROWNUM = ...` does
	return (id.ArgB("quoted") || !id.Parent().IsA(KPseudocolumn)) && d.baseCanQuote(id, identify)
}

// ---------------------------------------------------------------------------------------------
// Parser

// oracleBuildToTimestamp mirrors parsers/oracle.py:_build_to_timestamp.
func oracleBuildToTimestamp(args []*Expr, d *Dialect) *Expr {
	if len(args) == 1 {
		return New(KAnonymous, "this", "TO_TIMESTAMP", "expressions", args)
	}

	return buildFormattedTime(KStrToTime, "", nil)(args, d)
}

// oracleBuildToTimestampFmtStr mirrors _build_to_timestamp([this, '"<fmt>"'], dialect), where the
// format is a (quoted) Python string rather than an expression.
func oracleBuildToTimestampFmtStr(this *Expr, quotedFmt string, d *Dialect) *Expr {
	return New(KStrToTime, "this", this, "format", d.formatTimeStr(quotedFmt))
}

// _parse_to_number (parsers/oracle.py L105)
func oracleParseToNumber(p *Parser) *Expr {
	// https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/TO_NUMBER.html
	this := p.parseBitwise()

	var def *Expr
	if p.match(TK_DEFAULT) {
		def = p.parseBitwise()
		p.matchTextSeq("ON", "CONVERSION", "ERROR")
	}

	var fmt, nlsparam *Expr
	if p.match(TK_COMMA) {
		fmt = p.parseBitwise()
		if p.match(TK_COMMA) {
			nlsparam = p.parseBitwise()
		}
	}

	return p.expression(New(KToNumber, "this", this, "format", fmt, "nlsparam", nlsparam, "default", def))
}

// _parse_dbms_random (parsers/oracle.py L125)
func oracleParseDbmsRandom(p *Parser) *Expr {
	if p.matchTextSeq(".", "VALUE") {
		var lower, upper *Expr
		if p.matchNoAdvance(TK_L_PAREN) {
			lowerUpper := p.parseWrappedCSV(func() *Expr { return p.parseBitwise() }, TK_COMMA, false)
			if len(lowerUpper) == 2 {
				lower, upper = lowerUpper[0], lowerUpper[1]
			}
		}

		return New(KRand, "lower", lower, "upper", upper)
	}

	p.retreat(p.index - 1)
	return nil
}

// _parse_oracle_json_array (parsers/oracle.py L138)
func oracleParseOracleJSONArray(p *Parser) *Expr {
	expressions := p.parseCSV(func() *Expr { return p.parseFormatJson(p.parseBitwise()) }, TK_COMMA)
	return oracleParseJSONArray(p, KJSONArray, "expressions", expressions)
}

// _parse_oracle_json_arrayagg (parsers/oracle.py L144)
func oracleParseOracleJSONArrayagg(p *Parser) *Expr {
	this := p.parseFormatJson(p.parseBitwise())
	order := p.parseOrder(nil, false)
	return oracleParseJSONArray(p, KJSONArrayAgg, "this", this, "order", order)
}

// _parse_json_array (parsers/oracle.py L151). kwargs are key/value pairs evaluated by the caller.
func oracleParseJSONArray(p *Parser, kind Kind, kwargs ...any) *Expr {
	nullHandling := p.parseOnHandling("NULL", "NULL", "ABSENT")
	// self._match_text_seq("RETURNING") and self._parse_type() -> False when unmatched
	var returnType any = false
	if p.matchTextSeq("RETURNING") {
		returnType = p.parseType(true, false)
	}
	strict := p.matchTextSeq("STRICT")
	args := append([]any{"null_handling", nullHandling, "return_type", returnType, "strict", strict}, kwargs...)
	return p.expression(New(kind, args...))
}

// _parse_hint_function_call (parsers/oracle.py L161)
func oracleParseHintFunctionCall(p *Parser) *Expr {
	if !p.curr.ok() || !p.next.ok() || p.next.Type != TK_L_PAREN {
		return nil
	}

	name := p.curr.Text

	p.advance(2)
	args := oracleParseHintArgs(p)
	this := p.expression(New(KAnonymous, "this", name, "expressions", args))
	p.matchRParen(this)
	return this
}

// _parse_hint_args (parsers/oracle.py L173)
func oracleParseHintArgs(p *Parser) []*Expr {
	args := []*Expr{}
	result := p.parseVar(false, nil, false)

	for result != nil {
		args = append(args, result)
		result = p.parseVar(false, nil, false)
	}

	return args
}

// _parse_query_restrictions (parsers/oracle.py L183)
func oracleParseQueryRestrictions(p *Parser) *Expr {
	kind := p.parseVarFromOptions(p.s.QUERY_RESTRICTIONS, false)

	if kind == nil {
		return nil
	}

	// self._match(TokenType.CONSTRAINT) and self._parse_field() -> False when unmatched
	var expression any = false
	if p.match(TK_CONSTRAINT) {
		expression = p.parseField(false, nil, false)
	}
	return p.expression(New(KQueryOption, "this", kind, "expression", expression))
}

// _parse_json_exists (parsers/oracle.py L195)
func oracleParseJSONExists(p *Parser) *Expr {
	this := p.parseFormatJson(p.parseBitwise())
	p.match(TK_COMMA)
	path := p.d.toJSONPath(p.parseBitwise())
	// self._match_text_seq("PASSING") and self._parse_csv(...) -> False when unmatched
	var passing any = false
	if p.matchTextSeq("PASSING") {
		passing = p.parseCSV(func() *Expr { return p.parseAlias(p.parseBitwise(), false) }, TK_COMMA)
	}
	onCondition := p.parseOnCondition()
	return p.expression(New(
		KJSONExists,
		"this", this,
		"path", path,
		"passing", passing,
		"on_condition", onCondition,
	))
}

// _parse_into (parsers/oracle.py L208)
func oracleParseInto(p *Parser) *Expr {
	// https://docs.oracle.com/en/database/oracle/oracle-database/19/lnpls/SELECT-INTO-statement.html
	bulkCollect := p.match(TK_BULK_COLLECT_INTO)
	if !bulkCollect && !p.match(TK_INTO) {
		return nil
	}
	bc := bulkCollect

	index := p.index

	expressions := p.parseExpressions()
	if len(expressions) == 1 {
		p.retreat(index)
		p.match(TK_TABLE)
		return p.expression(New(
			KInto,
			"this", p.parseTable(true, false, nil, false, false, false, false),
			"bulk_collect", bc,
		))
	}

	return p.expression(New(KInto, "bulk_collect", bc, "expressions", expressions))
}

// _parse_connect_with_prior (parsers/oracle.py L226)
func oracleParseConnectWithPrior(p *Parser) *Expr {
	return p.parseAssignment()
}

// _parse_column_ops (parsers/oracle.py L229)
func oracleParseColumnOps(p *Parser, this *Expr) *Expr {
	this = p.baseParseColumnOps(this)

	if this == nil {
		return this
	}

	index := p.index

	// https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Interval-Exprs.html
	intervalSpan := p.tryParseExpr(func() *Expr { return p.parseIntervalSpan(this) }, false)
	if intervalSpan != nil && intervalSpan.ArgE("unit").IsA(KIntervalSpan) {
		return intervalSpan
	}

	p.retreat(index)
	return this
}

// _parse_insert_table (parsers/oracle.py L245)
func oracleParseInsertTable(p *Parser) *Expr {
	// Oracle does not use AS for INSERT INTO alias
	// https://docs.oracle.com/en/database/oracle/oracle-database/18/sqlrf/INSERT.html
	// Parse table parts without schema to avoid parsing the alias with its columns
	this := p.parseTableParts(true, false, false, false)

	if this.IsA(KTable) {
		aliasName := p.parseIdVar(false, nil)
		if aliasName != nil {
			this.Set("alias", New(KTableAlias, "this", aliasName))
		}

		this.Set("partition", p.parsePartition())

		// Now parse the schema (column list) if present
		return p.parseSchema(this)
	}

	return this
}

// ---------------------------------------------------------------------------------------------
// Generator

// oracleFormatTimeArg returns self.format_time(e) as a g.fn argument (nil for Python None).
func oracleFormatTimeArg(g *Generator, e *Expr) any {
	if s := g.formatTime(e, nil, nil); s != "" {
		return s
	}
	return nil
}

// _trim_sql (generators/oracle.py L14)
func oracleTrimSQL(g *Generator, e *Expr) string {
	position := e.ArgS("position")

	if position != "" {
		up := pyUpper(position)
		if up == "LEADING" || up == "TRAILING" {
			return g.trimSQL(e)
		}
	}

	return trimSQL(g, e, "")
}

// currenttimestamp_sql (generators/oracle.py L112)
func oracleCurrenttimestampSQL(g *Generator, e *Expr) string {
	if e.ArgB("sysdate") {
		return "SYSDATE"
	}

	this := e.This()
	if this != nil {
		return g.fn("CURRENT_TIMESTAMP", this)
	}
	return "CURRENT_TIMESTAMP"
}

// offset_sql (generators/oracle.py L119)
func oracleOffsetSQL(g *Generator, e *Expr) string {
	return g.baseOffsetSQL(e) + " ROWS"
}

// add_column_sql (generators/oracle.py L122)
func oracleAddColumnSQL(g *Generator, e *Expr) string {
	return "ADD " + g.sql(e)
}

// queryoption_sql (generators/oracle.py L125)
func oracleQueryoptionSQL(g *Generator, e *Expr) string {
	option := g.sqlKey(e, "this")
	value := g.sqlKey(e, "expression")
	if value != "" {
		value = " CONSTRAINT " + value
	}

	return option + value
}

// coalesce_sql (generators/oracle.py L132)
func oracleCoalesceSQL(g *Generator, e *Expr) string {
	funcName := "COALESCE"
	if e.ArgB("is_nvl") {
		funcName = "NVL"
	}
	return renameFunc(funcName)(g, e)
}

// into_sql (generators/oracle.py L136)
func oracleIntoSQL(g *Generator, e *Expr) string {
	into := "INTO"
	if e.ArgB("bulk_collect") {
		into = "BULK COLLECT INTO"
	}
	if e.This() != nil {
		return g.seg(into) + " " + g.sqlKey(e, "this")
	}

	return g.seg(into) + " " + g.expressions(e, exprsOpts{})
}

// hint_sql (generators/oracle.py L143)
func oracleHintSQL(g *Generator, e *Expr) string {
	var expressions []any

	var hints []any
	switch v := e.Arg("expressions").(type) {
	case []*Expr:
		for _, x := range v {
			hints = append(hints, x)
		}
	case []string:
		for _, x := range v {
			hints = append(hints, x)
		}
	case []any:
		hints = v
	}

	for _, h := range hints {
		if hint, ok := h.(*Expr); ok && hint.IsA(KAnonymous) {
			args := make([]any, 0, len(hint.Expressions()))
			for _, a := range hint.Expressions() {
				args = append(args, a)
			}
			formattedArgs := g.formatArgs(" ", args...)
			expressions = append(expressions, g.sqlKey(hint, "this")+"("+formattedArgs+")")
		} else {
			expressions = append(expressions, g.sql(h))
		}
	}

	return " /*+ " + pyStrip(g.expressions(nil, exprsOpts{sqls: expressions, hasSqls: true, sep: strp2(g.s.QUERY_HINT_SEP)})) + " */"
}

// isascii_sql (generators/oracle.py L155)
func oracleIsasciiSQL(g *Generator, e *Expr) string {
	return "NVL(REGEXP_LIKE(" + g.sql(e.Arg("this")) + ", '^[' || CHR(1) || '-' || CHR(127) || ']*$'), TRUE)"
}

// interval_sql (generators/oracle.py L158)
func oracleIntervalSQL(g *Generator, e *Expr) string {
	prefix := ""
	if e.This().IsA(KLiteral) {
		prefix = "INTERVAL "
	}
	return prefix + g.sqlKey(e, "this") + " " + g.sqlKey(e, "unit")
}

// tonumber_sql (generators/oracle.py L161)
func oracleTonumberSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	def := g.sqlKey(e, "default")
	if def != "" {
		this = this + " DEFAULT " + def + " ON CONVERSION ERROR"
	}

	return g.fn("TO_NUMBER", this, e.Arg("format"), e.Arg("nlsparam"))
}

// columndef_sql (generators/oracle.py L171)
func oracleColumndefSQL(g *Generator, e *Expr, sep string) string {
	paramConstraint := e.Find(KInOutColumnConstraint)
	if paramConstraint != nil {
		sep = " " + g.sql(paramConstraint) + " "
		paramConstraint.Pop()
	}
	return g.baseColumndefSQL(e, sep)
}
