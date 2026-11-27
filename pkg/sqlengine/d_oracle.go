package sqlengine

func init() { registerCustomizer("oracle", customizeOracle) }

func customizeOracle(d *Dialect) {
	d.hooks.canQuote = oracleCanQuote

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
	P.FUNCTIONS["TO_DATE"] = oracleBuildStrToTime(KStrToDate)
	P.FUNCTIONS["TRUNC"] = oracleBuildFormattedTrunc
	// Oracle's VARIANCE returns 0 instead of NULL for a single value, unlike VAR_SAMP
	P.FUNCTIONS["VARIANCE"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KVariance, "this", seqGet(args, 0), "single_value_returns_zero", true)
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
	P.FUNCTION_PARSERS["JSON_VALUE"] = func(p *Parser) *Expr { return p.parseJsonValue(true) }
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
	for _, name := range []string{"PCTFREE", "PCTUSED", "INITRANS", "MAXTRANS"} {
		P.PROPERTY_PARSERS[name] = noKwargsE(oracleParsePhysicalProperty)
	}
	for _, name := range []string{"LOGGING", "NOLOGGING"} {
		P.PROPERTY_PARSERS[name] = noKwargsE(func(p *Parser) *Expr {
			return p.expression(New(KPhysicalProperty, "this", upperText(p.prev)))
		})
	}
	P.PROPERTY_PARSERS["STORAGE"] = noKwargsE(oracleParseStorage)

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

	G := d.G

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
	T[KPivot] = transformPreprocess([]func(*Expr) *Expr{transformUnqualifyPivotFields}, nil)
	T[KGroupConcat] = func(g *Generator, e *Expr) string {
		return groupconcatSQL(g, e, "LISTAGG", ",", true, true)
	}
	T[KDateStrToDate] = func(g *Generator, e *Expr) string {
		return g.fn("TO_DATE", e.Arg("this"), LiteralString("YYYY-MM-DD"))
	}
	T[KDateTrunc] = oracleDateTruncSQL
	T[KEuclideanDistance] = renameFunc("L2_DISTANCE")
	// VARIANCE returns 0 for a single value, where VAR_SAMP returns NULL
	T[KVariance] = func(g *Generator, e *Expr) string {
		if e.ArgB("single_value_returns_zero") {
			return g.fn("VARIANCE", e.This())
		}
		if e.This().IsA(KDistinct) {
			g.unsupported("Oracle's VAR_SAMP does not support DISTINCT")
		}
		return g.fn("VAR_SAMP", e.This())
	}
	T[KILike] = noIlikeSQL
	T[KLogicalOr] = renameFunc("MAX")
	T[KLogicalAnd] = renameFunc("MIN")
	T[KMod] = renameFunc("MOD")
	T[KNthValue] = nthValueFromSQL
	T[KRand] = renameFunc("DBMS_RANDOM.VALUE")
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		transformEliminateDistinctOn,
		transformEliminateQualify,
		transformEliminateSemiAndAntiJoins,
	}, nil)
	T[KStrPosition] = func(g *Generator, e *Expr) string {
		return strpositionSQL(g, e, "INSTR", true, true, true)
	}
	T[KStrToTime] = func(g *Generator, e *Expr) string {
		return g.fn("TO_TIMESTAMP", e.Arg("this"), oracleFormatTimeArg(g, e), e.Arg("nlsparam"))
	}
	T[KStrToDate] = func(g *Generator, e *Expr) string {
		return g.fn("TO_DATE", e.Arg("this"), oracleFormatTimeArg(g, e), e.Arg("nlsparam"))
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
	T[KVariancePop] = renameFunc("VAR_POP")
	T[KApproxDistinct] = approxCountDistinctSQL
	T[KSystimestamp] = func(_ *Generator, _ *Expr) string { return "SYSTIMESTAMP" }

	G.methods[KCurrentTimestamp] = oracleCurrenttimestampSQL
	G.methods[KCoalesce] = oracleCoalesceSQL
	G.methods[KIsAscii] = oracleIsasciiSQL
	G.methods[KPhysicalProperty] = oraclePhysicalpropertySQL
	G.methods[KStorageProperty] = func(g *Generator, e *Expr) string {
		return "STORAGE (" + g.expressions(e, exprsOpts{sep: strp2(" "), flat: true}) + ")"
	}

	G.h.offsetSQL = oracleOffsetSQL
	G.h.versionSQL = oracleVersionSQL
	G.h.addColumnSQL = oracleAddColumnSQL
	G.h.queryoptionSQL = oracleQueryoptionSQL
	G.h.intoSQL = oracleIntoSQL
	G.h.hintSQL = oracleHintSQL
	G.h.intervalSQL = oracleIntervalSQL
	G.h.tonumberSQL = oracleTonumberSQL
	G.h.columndefSQL = oracleColumndefSQL
}

var oracleStorageOptions = []string{
	"INITIAL", "NEXT", "MINEXTENTS", "MAXEXTENTS", "MAXSIZE", "PCTINCREASE", "FREELISTS", "FREELIST",
	"BUFFER_POOL", "FLASH_CACHE", "CELL_FLASH_CACHE",
}

func oracleParsePhysicalProperty(p *Parser) *Expr {
	name := upperText(p.prev)
	value := p.parseNumber()
	if value == nil {
		p.raiseError("Expected value for "+name, nil)
	}
	return p.expression(New(KPhysicalProperty, "this", name, "value", value))
}

// oracleParseStorage parses STORAGE (option value ...): sizes may carry a K, M, G, T, P or E
// suffix, MAXEXTENTS and MAXSIZE may be UNLIMITED, FREELIST is followed by GROUPS and the cache
// options take KEEP, DEFAULT and RECYCLE (BUFFER_POOL) or NONE.
func oracleParseStorage(p *Parser) *Expr {
	parseOptions := func() []*Expr {
		var options []*Expr
		for p.matchTexts(oracleStorageOptions...) {
			name := upperText(p.prev)
			if name == "FREELIST" {
				if !p.matchTextSeq("GROUPS") {
					p.raiseError("Expected GROUPS after FREELIST", nil)
				}
				name = "FREELIST GROUPS"
			}

			var value *Expr
			switch {
			case name == "BUFFER_POOL" || name == "FLASH_CACHE" || name == "CELL_FLASH_CACHE":
				last := "NONE"
				if name == "BUFFER_POOL" {
					last = "RECYCLE"
				}
				value = p.parseVarFromOptions(OptionsType{"KEEP": {}, "DEFAULT": {}, last: {}}, true)
			case (name == "MAXEXTENTS" || name == "MAXSIZE") && p.matchTextSeq("UNLIMITED"):
				value = VarExpr("UNLIMITED")
			default:
				value = p.parseNumber()
				if value != nil && (name == "INITIAL" || name == "NEXT" || name == "MAXSIZE") &&
					p.matchTexts("K", "M", "G", "T", "P", "E") {
					value = VarExpr(value.Name() + upperText(p.prev))
				}
			}
			if value == nil {
				p.raiseError("Expected value for "+name, nil)
			}
			options = append(options, p.expression(New(KPhysicalProperty, "this", name, "value", value)))
		}
		return options
	}

	return p.expression(New(KStorageProperty, "expressions", p.parseWrappedList(parseOptions, false)))
}

func oraclePhysicalpropertySQL(g *Generator, e *Expr) string {
	if value := g.sqlKey(e, "value"); value != "" {
		return e.Name() + " " + value
	}
	return e.Name()
}

// oracleDateTruncSQL renders a DateTrunc as TRUNC(this, format), turning its date part into the
// matching format model (e.g. 'QUARTER' -> 'Q'). A week starting on Monday is 'IW', any other
// week 'DAY'.
func oracleDateTruncSQL(g *Generator, e *Expr) string {
	dhUnsupportedArgs(g, e, "zone")
	unit := e.ArgE("unit")
	if unit.IsA(KWeekStart) {
		if pyUpper(unit.This().Name()) == "MONDAY" {
			unit = LiteralString("IW")
		} else {
			g.weekstartName(unit)
			unit = LiteralString("DAY")
		}
	} else if unit.IsA(KLiteral, KVar) {
		name := pyUpper(unit.Name())
		if mapped, ok := oracleUnitToTruncFormat[name]; ok {
			unit = LiteralString(mapped)
		} else {
			g.unsupported("Unexpected interval unit: " + name)
		}
	}

	return g.fn("TRUNC", e.Arg("this"), unit)
}

func oracleCanQuote(d *Dialect, id *Expr, identify string) bool {
	// Disable quoting for pseudocolumns as it may break queries e.g
	// `WHERE "ROWNUM" = ...` does not work but `WHERE ROWNUM = ...` does
	return (id.ArgB("quoted") || !id.Parent().IsA(KPseudocolumn)) && d.baseCanQuote(id, identify)
}

var oracleTruncFormatToUnit = map[string]string{
	"CC":    "CENTURY",
	"SCC":   "CENTURY",
	"SYYYY": "YEAR",
	"SYYY":  "YEAR",
	"YYYY":  "YEAR",
	"YEAR":  "YEAR",
	"SYEAR": "YEAR",
	"YYY":   "YEAR",
	"YY":    "YEAR",
	"Y":     "YEAR",
	"IYYY":  "ISOYEAR",
	"IYY":   "ISOYEAR",
	"IY":    "ISOYEAR",
	"I":     "ISOYEAR",
	"Q":     "QUARTER",
	"MONTH": "MONTH",
	"MON":   "MONTH",
	"MM":    "MONTH",
	"RM":    "MONTH",
	"DAY":   "WEEK",
	"DY":    "WEEK",
	"D":     "WEEK",
	"IW":    "ISOWEEK",
	// These weeks are anchored to the first day of the month/year, not a fixed weekday.
	"W":    "WEEK_OF_MONTH",
	"WW":   "WEEK_OF_YEAR",
	"DDD":  "DAY",
	"DD":   "DAY",
	"J":    "DAY",
	"HH":   "HOUR",
	"HH12": "HOUR",
	"HH24": "HOUR",
	"MI":   "MINUTE",
	"SS":   "SECOND",
}

var oracleUnitToTruncFormat = map[string]string{
	"CENTURY":       "CC",
	"YEAR":          "YEAR",
	"YY":            "YEAR",
	"YYYY":          "YEAR",
	"ISOYEAR":       "IYYY",
	"QUARTER":       "Q",
	"MONTH":         "MONTH",
	"MM":            "MONTH",
	"MON":           "MONTH",
	"WEEK":          "IW",
	"WEEK_OF_MONTH": "W",
	"WEEK_OF_YEAR":  "WW",
	"ISOWEEK":       "IW",
	"DAY":           "DD",
	"HOUR":          "HH",
	"MINUTE":        "MI",
}

// oracleWeekDays lists the week days in ISO order, Monday (1) to Sunday (7).
var oracleWeekDays = []string{"MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"}

// oracleBuildFormattedTrunc builds TRUNC, turning a date TRUNC's format model into the date part
// it truncates to (e.g. 'MM' -> 'MONTH'). The week formats become a WeekStart: IW weeks start on
// Monday, DAY/DY/D weeks on the dialect's first week day.
func oracleBuildFormattedTrunc(args []*Expr, d *Dialect) *Expr {
	trunc := buildTrunc(args, d, false, "DD", true, false)
	if unit := trunc.ArgE("unit"); trunc.IsA(KDateTrunc) && unit.IsString() {
		mapped := oracleTruncFormatToUnit[pyUpper(unit.Name())]
		if mapped == "ISOWEEK" || (mapped == "WEEK" && d.S.WEEK_OFFSET != 0) {
			start := 1
			if mapped == "WEEK" {
				start = weekOffsetToDow(d.S.WEEK_OFFSET)
			}
			trunc.Set("unit", New(KWeekStart, "this", VarChecked(oracleWeekDays[start-1])))
		} else if mapped != "" {
			trunc.Set("unit", LiteralString(mapped))
		}
	}
	return trunc
}

func oracleBuildToTimestamp(args []*Expr, d *Dialect) *Expr {
	if len(args) == 1 {
		return New(KAnonymous, "this", "TO_TIMESTAMP", "expressions", args)
	}

	return oracleBuildStrToTime(KStrToTime)(args, d)
}

func oracleBuildStrToTime(kind Kind) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		expression := buildFormattedTime(kind, "", nil)(args, d)
		expression.Set("nlsparam", seqGet(args, 2))
		return expression
	}
}

// oracleBuildToTimestampFmtStr builds a StrToTime for this with a fixed format; quotedFmt is a
// double-quoted format string rather than an expression.
func oracleBuildToTimestampFmtStr(this *Expr, quotedFmt string, d *Dialect) *Expr {
	return New(KStrToTime, "this", this, "format", d.formatTimeStr(quotedFmt))
}

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

// oracleParseDbmsRandom parses DBMS_RANDOM.VALUE[(lower, upper)] into a Rand; for anything else
// it steps back over DBMS_RANDOM and returns nil.
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

func oracleParseOracleJSONArray(p *Parser) *Expr {
	expressions := p.parseCSV(func() *Expr { return p.parseFormatJson(p.parseBitwise()) }, TK_COMMA)
	return oracleParseJSONArray(p, KJSONArray, "expressions", expressions)
}

func oracleParseOracleJSONArrayagg(p *Parser) *Expr {
	this := p.parseFormatJson(p.parseBitwise())
	order := p.parseOrder(nil, false)
	return oracleParseJSONArray(p, KJSONArrayAgg, "this", this, "order", order)
}

func oracleParseJSONArray(p *Parser, kind Kind, kwargs ...any) *Expr {
	nullHandling := p.parseOnHandling("NULL", "NULL", "ABSENT")
	// false (not nil) when RETURNING is absent.
	var returnType any = false
	if p.matchTextSeq("RETURNING") {
		returnType = p.parseType(true, false)
	}
	strict := p.matchTextSeq("STRICT")
	args := append([]any{"null_handling", nullHandling, "return_type", returnType, "strict", strict}, kwargs...)
	return p.expression(New(kind, args...))
}

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

func oracleParseHintArgs(p *Parser) []*Expr {
	args := []*Expr{}
	result := p.parseVar(false, nil, false)

	for result != nil {
		args = append(args, result)
		result = p.parseVar(false, nil, false)
	}

	return args
}

func oracleParseQueryRestrictions(p *Parser) *Expr {
	kind := p.parseVarFromOptions(p.s.QUERY_RESTRICTIONS, false)

	if kind == nil {
		return nil
	}

	// false (not nil) when CONSTRAINT is absent.
	var expression any = false
	if p.match(TK_CONSTRAINT) {
		expression = p.parseField(false, nil, false)
	}
	return p.expression(New(KQueryOption, "this", kind, "expression", expression))
}

func oracleParseJSONExists(p *Parser) *Expr {
	this := p.parseFormatJson(p.parseBitwise())
	p.match(TK_COMMA)
	path := p.d.toJSONPath(p.parseBitwise())
	// false (not nil) when PASSING is absent.
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

func oracleParseConnectWithPrior(p *Parser) *Expr {
	return p.parseAssignment()
}

// oracleParseColumnOps extends the base column operators with interval expressions such as
// (a - b) DAY TO SECOND, keeping the result only when it is a full interval span.
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

func oracleParseInsertTable(p *Parser) *Expr {
	// Oracle does not use AS for INSERT INTO alias
	// https://docs.oracle.com/en/database/oracle/oracle-database/18/sqlrf/INSERT.html
	// Parse table parts without schema to avoid parsing the alias with its columns
	this := p.parseTableParts(true, false, false, false)

	if this.IsA(KTable) {
		this.Set("partition", p.parsePartition())

		aliasName := p.parseIdVar(false, nil)
		if aliasName != nil {
			this.Set("alias", New(KTableAlias, "this", aliasName))
		}

		return p.parseSchema(this)
	}

	return this
}

// oracleFormatTimeArg returns nil when e has no format, so g.fn drops the argument.
func oracleFormatTimeArg(g *Generator, e *Expr) any {
	if s := g.formatTime(e, nil, nil); s != "" {
		return s
	}
	return nil
}

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

// oracleVersionSQL renders a flashback query as AS OF SCN | TIMESTAMP value.
func oracleVersionSQL(g *Generator, e *Expr) string {
	if e.Text("kind") == "AS OF" {
		return "AS OF " + e.Name() + " " + g.sqlKey(e, "expression")
	}
	return g.baseVersionSQL(e)
}

func oracleOffsetSQL(g *Generator, e *Expr) string {
	return g.baseOffsetSQL(e) + " ROWS"
}

func oracleAddColumnSQL(g *Generator, e *Expr) string {
	return "ADD " + g.sql(e)
}

func oracleQueryoptionSQL(g *Generator, e *Expr) string {
	option := g.sqlKey(e, "this")
	value := g.sqlKey(e, "expression")
	if value != "" {
		value = " CONSTRAINT " + value
	}

	return option + value
}

func oracleCoalesceSQL(g *Generator, e *Expr) string {
	funcName := "COALESCE"
	if e.ArgB("is_nvl") {
		funcName = "NVL"
	}
	return renameFunc(funcName)(g, e)
}

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

// oracleHintSQL renders a /*+ ... */ hint; function-style hints keep space-separated arguments.
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

func oracleIsasciiSQL(g *Generator, e *Expr) string {
	return "NVL(REGEXP_LIKE(" + g.sql(e.Arg("this")) + ", '^[' || CHR(1) || '-' || CHR(127) || ']*$'), TRUE)"
}

func oracleIntervalSQL(g *Generator, e *Expr) string {
	prefix := ""
	if e.This().IsA(KLiteral) {
		prefix = "INTERVAL "
	}
	return prefix + g.sqlKey(e, "this") + " " + g.sqlKey(e, "unit")
}

func oracleTonumberSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	def := g.sqlKey(e, "default")
	if def != "" {
		this = this + " DEFAULT " + def + " ON CONVERSION ERROR"
	}

	return g.fn("TO_NUMBER", this, e.Arg("format"), e.Arg("nlsparam"))
}

// oracleColumndefSQL renders a column definition, moving an IN/OUT parameter mode between the name
// and the type.
func oracleColumndefSQL(g *Generator, e *Expr, sep string) string {
	paramConstraint := e.Find(KInOutColumnConstraint)
	if paramConstraint != nil {
		sep = " " + g.sql(paramConstraint) + " "
		paramConstraint.Pop()
	}
	return g.baseColumndefSQL(e, sep)
}
