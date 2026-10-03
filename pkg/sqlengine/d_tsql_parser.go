package sqlengine

// Port of sqlglot/parsers/tsql.py (TSQLParser and its module-level builders).

import (
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// tsqlFULL_FORMAT_TIME_MAPPING mirrors parsers/tsql.py FULL_FORMAT_TIME_MAPPING.
var tsqlFULL_FORMAT_TIME_MAPPING = map[string]string{
	"weekday": "%A",
	"dw":      "%A",
	"w":       "%A",
	"month":   "%B",
	"mm":      "%B",
	"m":       "%B",
}

// tsqlDATE_DELTA_INTERVAL mirrors parsers/tsql.py DATE_DELTA_INTERVAL.
var tsqlDATE_DELTA_INTERVAL = map[string]string{
	"year":    "year",
	"yyyy":    "year",
	"yy":      "year",
	"quarter": "quarter",
	"qq":      "quarter",
	"q":       "quarter",
	"month":   "month",
	"mm":      "month",
	"m":       "month",
	"week":    "week",
	"ww":      "week",
	"wk":      "week",
	"day":     "day",
	"dd":      "day",
	"d":       "day",
}

// tsqlDATE_FMT_RE mirrors parsers/tsql.py DATE_FMT_RE.
var tsqlDATE_FMT_RE = regexp.MustCompile("([dD]{1,2})|([mM]{1,2})|([yY]{1,4})|([hH]{1,2})|([sS]{1,2})")

// tsqlTRANSPILE_SAFE_NUMBER_FMT mirrors parsers/tsql.py TRANSPILE_SAFE_NUMBER_FMT (N = Numeric, C = Currency).
var tsqlTRANSPILE_SAFE_NUMBER_FMT = newStrSet("N", "C")

// tsqlOPTIONS mirrors parsers/tsql.py OPTIONS.
// Unsupported options:
// - OPTIMIZE FOR ( @variable_name { UNKNOWN | = <literal_constant> } [ , ...n ] )
// - TABLE HINT.
var tsqlOPTIONS = OptionsType{
	"DISABLE_OPTIMIZED_PLAN_FORCING":        {},
	"FAST":                                  {},
	"IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX": {},
	"LABEL":                                 {},
	"MAXDOP":                                {},
	"MAXRECURSION":                          {},
	"MAX_GRANT_PERCENT":                     {},
	"MIN_GRANT_PERCENT":                     {},
	"NO_PERFORMANCE_SPOOL":                  {},
	"QUERYTRACEON":                          {},
	"RECOMPILE":                             {},
	"CONCAT":                                {{"UNION"}},
	"DISABLE":                               {{"EXTERNALPUSHDOWN"}, {"SCALEOUTEXECUTION"}},
	"EXPAND":                                {{"VIEWS"}},
	"FORCE":                                 {{"EXTERNALPUSHDOWN"}, {"ORDER"}, {"SCALEOUTEXECUTION"}},
	"HASH":                                  {{"GROUP"}, {"JOIN"}, {"UNION"}},
	"KEEP":                                  {{"PLAN"}},
	"KEEPFIXED":                             {{"PLAN"}},
	"LOOP":                                  {{"JOIN"}},
	"MERGE":                                 {{"JOIN"}, {"UNION"}},
	"OPTIMIZE":                              {{"FOR", "UNKNOWN"}},
	"ORDER":                                 {{"GROUP"}},
	"PARAMETERIZATION":                      {{"FORCED"}, {"SIMPLE"}},
	"ROBUST":                                {{"PLAN"}},
	"USE":                                   {{"PLAN"}},
}

// tsqlFOR_XML_OPTIONS mirrors parsers/tsql.py FOR_XML_OPTIONS.
var tsqlFOR_XML_OPTIONS = OptionsType{
	"AUTO":     {},
	"EXPLICIT": {},
	"TYPE":     {},
	"ELEMENTS": {{"XSINIL"}, {"ABSENT"}},
	"BINARY":   {{"BASE64"}},
}

// tsqlFOR_JSON_OPTIONS mirrors parsers/tsql.py FOR_JSON_OPTIONS.
// FOR JSON { AUTO | PATH } [, ROOT [ ( 'name' ) ] ] [, INCLUDE_NULL_VALUES ] [, WITHOUT_ARRAY_WRAPPER ].
var tsqlFOR_JSON_OPTIONS = OptionsType{
	"AUTO":                  {},
	"PATH":                  {},
	"INCLUDE_NULL_VALUES":   {},
	"WITHOUT_ARRAY_WRAPPER": {},
}

// tsqlOPTIONS_THAT_REQUIRE_EQUAL mirrors parsers/tsql.py OPTIONS_THAT_REQUIRE_EQUAL.
var tsqlOPTIONS_THAT_REQUIRE_EQUAL = newStrSet("MAX_GRANT_PERCENT", "MIN_GRANT_PERCENT", "LABEL")

// tsqlDialect returns the TSQL dialect (Python `from sqlglot.dialects.tsql import TSQL`).
func tsqlDialect() *Dialect { return MustDialect("tsql") }

// tsqlLiteralStringOpt mirrors exp.Literal.string(value) where value may be None (str(None) == "None").
func tsqlLiteralStringOpt(s string, ok bool) *Expr {
	if !ok {
		return LiteralString("None")
	}
	return LiteralString(s)
}

// tsqlMergeMappings mirrors {**a, **b}.
func tsqlMergeMappings(a, b map[string]string) map[string]string {
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// tsqlBuildFormattedTime mirrors parsers/tsql.py _build_formatted_time.
func tsqlBuildFormattedTime(kind Kind, fullFormatMapping bool) FuncBuilder {
	return func(args []*Expr, _ *Dialect) *Expr {
		var fmtV *Expr = seqGet(args, 0)
		if fmtV != nil {
			mapping := tsqlDialect().S.TIME_MAPPING
			if fullFormatMapping {
				mapping = tsqlMergeMappings(mapping, tsqlFULL_FORMAT_TIME_MAPPING)
			}
			fmtV = tsqlLiteralStringOpt(formatTime(pyLower(fmtV.Name()), mapping, nil))
		}

		this := seqGet(args, 1)
		if this != nil {
			this = CastExpr(this, DT_DATETIME2, true, nil)
		}

		return New(kind, "this", this, "format", fmtV)
	}
}

// tsqlBuildFormat mirrors parsers/tsql.py _build_format.
func tsqlBuildFormat(args []*Expr, _ *Dialect) *Expr {
	this := seqGet(args, 0)
	fmtV := seqGet(args, 1)
	culture := seqGet(args, 2)

	numberFmt := fmtV != nil && (tsqlTRANSPILE_SAFE_NUMBER_FMT.Has(fmtV.Name()) || !tsqlDATE_FMT_RE.MatchString(fmtV.Name()))

	if numberFmt {
		return New(KNumberToStr, "this", this, "format", fmtV, "culture", culture)
	}

	if fmtV != nil {
		name := fmtV.Name()
		if len([]rune(name)) == 1 {
			fmtV = tsqlLiteralStringOpt(formatTime(name, tsqlDialect().S.FORMAT_TIME_MAPPING, nil))
		} else {
			fmtV = tsqlLiteralStringOpt(formatTime(name, tsqlDialect().S.TIME_MAPPING, nil))
		}
	}

	return New(KTimeToStr, "this", this, "format", fmtV, "culture", culture)
}

// tsqlBuildEomonth mirrors parsers/tsql.py _build_eomonth.
func tsqlBuildEomonth(args []*Expr, _ *Dialect) *Expr {
	date := New(KTsOrDsToDate, "this", seqGet(args, 0))
	monthLag := seqGet(args, 1)

	var this *Expr
	if monthLag == nil {
		this = date
	} else {
		unit := tsqlDATE_DELTA_INTERVAL["month"]
		var unitV *Expr
		if unit != "" {
			unitV = VarChecked(unit)
		}
		this = New(KDateAdd, "this", date, "expression", monthLag, "unit", unitV)
	}

	return New(KLastDay, "this", this)
}

// tsqlBuildHashbytes mirrors parsers/tsql.py _build_hashbytes.
func tsqlBuildHashbytes(args []*Expr, _ *Dialect) *Expr {
	if len(args) < 2 {
		panic(&ValueError{Msg: "not enough values to unpack (expected 2, got " + strconv.Itoa(len(args)) + ")"})
	}
	if len(args) > 2 {
		panic(&ValueError{Msg: "too many values to unpack (expected 2)"})
	}
	kindE, data := args[0], args[1]
	kind := ""
	if kindE.IsString() {
		kind = pyUpper(kindE.Name())
	}

	// args.pop(0): the caller validates the built expression against the shortened list.
	if kind == "MD5" {
		return WithValidateArgs(New(KMD5, "this", data), args[1:])
	}
	if kind == "SHA" || kind == "SHA1" {
		return WithValidateArgs(New(KSHA, "this", data), args[1:])
	}
	if kind == "SHA2_256" {
		return New(KSHA2, "this", data, "length", LiteralInt(256))
	}
	if kind == "SHA2_512" {
		return New(KSHA2, "this", data, "length", LiteralInt(512))
	}

	anyArgs := make([]any, len(args))
	for i, a := range args {
		anyArgs[i] = a
	}
	return dhFunc("HASHBYTES", anyArgs...)
}

// tsqlDEFAULT_START_DATE mirrors parsers/tsql.py DEFAULT_START_DATE.
var tsqlDEFAULT_START_DATE = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)

// tsqlIntToPy mirrors expression.to_py() for an integer literal (or a negated one).
func tsqlIntToPy(e *Expr) int {
	if e.IsA(KNeg) {
		return -tsqlIntToPy(e.This())
	}
	return dhPyIntValue(e.ThisS())
}

// tsqlBuildDateDelta mirrors parsers/tsql.py _build_date_delta.
func tsqlBuildDateDelta(kind Kind, unitMapping map[string]string, bigInt bool) FuncBuilder {
	return func(args []*Expr, _ *Dialect) *Expr {
		unit := seqGet(args, 0)
		if unit != nil && len(unitMapping) > 0 {
			name := unit.Name()
			if mapped, ok := unitMapping[pyLower(name)]; ok {
				name = mapped
			}
			unit = VarChecked(name)
		}

		startDate := seqGet(args, 1)
		if startDate != nil && startDate.IsNumber() {
			// Numeric types are valid DATETIME values
			if startDate.IsInt() {
				adds := tsqlDEFAULT_START_DATE.AddDate(0, 0, tsqlIntToPy(startDate))
				startDate = LiteralString(adds.Format("2006-01-02"))
			} else {
				// We currently don't handle float values, i.e. they're not converted to equivalent DATETIMEs.
				// This is not a problem when generating T-SQL code, it is when transpiling to other dialects.
				return New(kind, "this", seqGet(args, 2), "expression", startDate, "unit", unit, "big_int", bigInt)
			}
		}

		return New(
			kind,
			"this", New(KTimeStrToTime, "this", seqGet(args, 2)),
			"expression", New(KTimeStrToTime, "this", startDate),
			"unit", unit,
			"big_int", bigInt,
		)
	}
}

// tsqlBuildDatetimefromparts mirrors parsers/tsql.py _build_datetimefromparts.
// https://learn.microsoft.com/en-us/sql/t-sql/functions/datetimefromparts-transact-sql?view=sql-server-ver16#syntax
func tsqlBuildDatetimefromparts(args []*Expr, _ *Dialect) *Expr {
	return New(
		KTimestampFromParts,
		"year", seqGet(args, 0),
		"month", seqGet(args, 1),
		"day", seqGet(args, 2),
		"hour", seqGet(args, 3),
		"min", seqGet(args, 4),
		"sec", seqGet(args, 5),
		"milli", seqGet(args, 6),
	)
}

// tsqlBuildTimefromparts mirrors parsers/tsql.py _build_timefromparts.
// https://learn.microsoft.com/en-us/sql/t-sql/functions/timefromparts-transact-sql?view=sql-server-ver16#syntax
func tsqlBuildTimefromparts(args []*Expr, _ *Dialect) *Expr {
	return New(
		KTimeFromParts,
		"hour", seqGet(args, 0),
		"min", seqGet(args, 1),
		"sec", seqGet(args, 2),
		"fractions", seqGet(args, 3),
		"precision", seqGet(args, 4),
	)
}

// tsqlBuildWithArgAsText mirrors parsers/tsql.py _build_with_arg_as_text.
func tsqlBuildWithArgAsText(kind Kind) FuncBuilder {
	return func(args []*Expr, _ *Dialect) *Expr {
		this := seqGet(args, 0)

		if this != nil && !this.IsString() {
			this = CastExpr(this, DT_TEXT, true, nil)
		}

		expression := seqGet(args, 1)
		kv := []any{"this", this}

		if expression != nil {
			kv = append(kv, "expression", expression)
		}

		return New(kind, kv...)
	}
}

// tsqlBuildParsename mirrors parsers/tsql.py _build_parsename.
// https://learn.microsoft.com/en-us/sql/t-sql/functions/parsename-transact-sql?view=sql-server-ver16
func tsqlBuildParsename(args []*Expr, _ *Dialect) *Expr {
	// PARSENAME(...) will be stored into exp.SplitPart if:
	// - All args are literals
	// - The part index (2nd arg) is <= 4 (max valid value, otherwise TSQL returns NULL)
	allLiterals := true
	for _, arg := range args {
		if !arg.IsA(KLiteral) {
			allLiterals = false
			break
		}
	}
	if len(args) == 2 && allLiterals {
		this := args[0]
		partIndex := args[1]
		splitCount := len(strings.Split(this.Name(), "."))
		if splitCount <= 4 {
			return New(
				KSplitPart,
				"this", this,
				"delimiter", LiteralString("."),
				"part_index", LiteralNumber(tsqlPyNumSub(splitCount+1, partIndex)),
			)
		}
	}

	return New(KAnonymous, "this", "PARSENAME", "expressions", args)
}

// tsqlPyNumSub mirrors str(n - literal.to_py()) for an int n and a literal.
func tsqlPyNumSub(n int, lit *Expr) string {
	if lit.IsNumber() && lit.IsInt() {
		return strconv.Itoa(n - tsqlIntToPy(lit))
	}
	if lit.IsString() {
		panic(&ValueError{Msg: "unsupported operand type(s) for -: 'int' and 'str'"})
	}
	// Decimal arithmetic: n - Decimal(text)
	v := chunkDToPyNumber(lit)
	nv, _ := chunkDParseDecimal(strconv.Itoa(n))
	return tsqlDecimalSub(nv, v).String()
}

// tsqlDecimalSub computes a - b for Python Decimal values (exact, no rounding needed for literals).
func tsqlDecimalSub(a, b chunkDPyNum) chunkDPyNum {
	b.neg = !b.neg
	// align exponents
	exp := a.exp
	if b.exp < exp {
		exp = b.exp
	}
	toBig := func(v chunkDPyNum) *big.Int {
		x, _ := new(big.Int).SetString(v.coeff, 10)
		x.Mul(x, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(v.exp-exp)), nil))
		if v.neg {
			x.Neg(x)
		}
		return x
	}
	sum := new(big.Int).Add(toBig(a), toBig(b))
	out := chunkDPyNum{exp: exp}
	if sum.Sign() < 0 {
		out.neg = true
		sum.Neg(sum)
	}
	out.coeff = sum.String()
	return out
}

// tsqlBuildJSONQuery mirrors parsers/tsql.py _build_json_query.
func tsqlBuildJSONQuery(args []*Expr, d *Dialect) *Expr {
	if len(args) == 1 {
		// The default value for path is '$'. As a result, if you don't provide a
		// value for path, JSON_QUERY returns the input expression.
		args = append(args, LiteralString("$"))
		// args.append(...) is visible to the caller's validation.
		return WithValidateArgs(buildExtractJSONWithPath(KJSONExtract)(args, d), args)
	}

	return buildExtractJSONWithPath(KJSONExtract)(args, d)
}

// tsqlBuildDatetrunc mirrors parsers/tsql.py _build_datetrunc.
func tsqlBuildDatetrunc(args []*Expr, _ *Dialect) *Expr {
	unit := seqGet(args, 0)
	this := seqGet(args, 1)

	if this != nil && this.IsString() {
		this = CastExpr(this, DT_DATETIME2, true, nil)
	}

	return New(KTimestampTrunc, "this", this, "unit", unit)
}

// customizeTSQLParser applies the TSQLParser class body on top of the base Parser.
func customizeTSQLParser(d *Dialect) {
	P := d.P

	P.QUERY_MODIFIER_PARSERS[TK_OPTION] = func(p *Parser) (string, any) { return "options", tsqlParseOptions(p) }
	P.QUERY_MODIFIER_PARSERS[TK_FOR] = func(p *Parser) (string, any) {
		if f := tsqlParseFor(p); f != nil {
			return "for_", f
		}
		return "for_", nil
	}

	P.FUNCTIONS["ATN2"] = fromArgList(KAtan2)
	P.FUNCTIONS["CHARINDEX"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KStrPosition,
			"this", seqGet(args, 1),
			"substr", seqGet(args, 0),
			"position", seqGet(args, 2),
		)
	}
	P.FUNCTIONS["COUNT"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KCount, "this", seqGet(args, 0), "expressions", argsFrom(args, 1), "big_int", false)
	}
	P.FUNCTIONS["COUNT_BIG"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KCount, "this", seqGet(args, 0), "expressions", argsFrom(args, 1), "big_int", true)
	}
	P.FUNCTIONS["DATEADD"] = buildDateDelta(KDateAdd, tsqlDATE_DELTA_INTERVAL, "DAY", false)
	P.FUNCTIONS["DATEDIFF"] = tsqlBuildDateDelta(KDateDiff, tsqlDATE_DELTA_INTERVAL, false)
	P.FUNCTIONS["DATEDIFF_BIG"] = tsqlBuildDateDelta(KDateDiff, tsqlDATE_DELTA_INTERVAL, true)
	P.FUNCTIONS["DATENAME"] = tsqlBuildFormattedTime(KTimeToStr, true)
	P.FUNCTIONS["DATETIMEFROMPARTS"] = tsqlBuildDatetimefromparts
	P.FUNCTIONS["EOMONTH"] = tsqlBuildEomonth
	P.FUNCTIONS["FORMAT"] = tsqlBuildFormat
	P.FUNCTIONS["GETDATE"] = fromArgList(KCurrentTimestamp)
	P.FUNCTIONS["HASHBYTES"] = tsqlBuildHashbytes
	P.FUNCTIONS["ISNULL"] = func(args []*Expr, _ *Dialect) *Expr { return buildCoalesce(args, nil, true) }
	P.FUNCTIONS["JSON_QUERY"] = tsqlBuildJSONQuery
	P.FUNCTIONS["JSON_VALUE"] = buildExtractJSONWithPath(KJSONExtractScalar)
	P.FUNCTIONS["LEN"] = tsqlBuildWithArgAsText(KLength)
	P.FUNCTIONS["LEFT"] = tsqlBuildWithArgAsText(KLeft)
	P.FUNCTIONS["NEWID"] = fromArgList(KUuid)
	P.FUNCTIONS["RIGHT"] = tsqlBuildWithArgAsText(KRight)
	P.FUNCTIONS["PARSENAME"] = tsqlBuildParsename
	P.FUNCTIONS["REPLICATE"] = fromArgList(KRepeat)
	P.FUNCTIONS["SCHEMA_NAME"] = fromArgList(KCurrentSchema)
	P.FUNCTIONS["SQUARE"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KPow, "this", seqGet(args, 0), "expression", LiteralInt(2))
	}
	P.FUNCTIONS["SYSDATETIME"] = fromArgList(KCurrentTimestamp)
	P.FUNCTIONS["SUSER_NAME"] = fromArgList(KCurrentUser)
	P.FUNCTIONS["SUSER_SNAME"] = fromArgList(KCurrentUser)
	P.FUNCTIONS["SYSDATETIMEOFFSET"] = fromArgList(KCurrentTimestampLTZ)
	P.FUNCTIONS["SYSTEM_USER"] = fromArgList(KCurrentUser)
	P.FUNCTIONS["TIMEFROMPARTS"] = tsqlBuildTimefromparts
	P.FUNCTIONS["DATETRUNC"] = tsqlBuildDatetrunc

	P.STATEMENT_PARSERS[TK_DECLARE] = func(p *Parser) *Expr { return p.parseDeclare() }
	P.STATEMENT_PARSERS[TK_EXECUTE] = tsqlParseExecute

	P.RANGE_PARSERS[TK_DCOLON] = func(p *Parser, this *Expr) *Expr {
		expression := p.parseFunction(nil, false, true, false)
		if expression == nil {
			expression = p.parseVar(true, nil, false)
		}
		return p.expression(New(KScopeResolution, "this", this, "expression", expression))
	}

	P.NO_PAREN_FUNCTION_PARSERS["NEXT"] = func(p *Parser) *Expr { return p.parseNextValueFor() }

	P.FUNCTION_PARSERS["JSON_ARRAYAGG"] = func(p *Parser) *Expr {
		this := p.parseBitwise()
		order := p.parseOrder(nil, false)
		nullHandling := p.parseOnHandling("NULL", "NULL", "ABSENT")
		return p.expression(New(
			KJSONArrayAgg,
			"this", this,
			"order", order,
			"null_handling", nullHandling,
		))
	}
	P.FUNCTION_PARSERS["DATEPART"] = tsqlParseDatepart

	// The DCOLON (::) operator serves as a scope resolution (exp.ScopeResolution) operator in T-SQL
	P.COLUMN_OPERATORS[TK_DCOLON] = func(p *Parser, this, to *Expr) *Expr {
		if to.IsA(KDataType) && to.Arg("this") != DT_USERDEFINED {
			return p.expression(New(KCast, "this", this, "to", to))
		}
		return p.expression(New(KScopeResolution, "this", this, "expression", to))
	}

	P.h.parseAlterTableSet = tsqlParseAlterTableSet
	P.h.parseWrappedSelect = tsqlParseWrappedSelect
	P.h.parseDcolon = tsqlParseDcolon
	P.h.parseProjections = tsqlParseProjections
	P.h.parseCommitOrRollback = tsqlParseCommitOrRollback
	P.h.parseTransaction = tsqlParseTransaction
	P.h.parseReturns = tsqlParseReturns
	P.h.parseConvert = tsqlParseConvert
	P.h.parseColumnDef = tsqlParseColumnDef
	P.h.parseUserDefinedFunction = tsqlParseUserDefinedFunction
	P.h.parseInto = tsqlParseInto
	P.h.parseIdVar = tsqlParseIdVar
	P.h.parseTableParts = tsqlParseTableParts
	P.h.parseCreate = tsqlParseCreate
	P.h.parseIf = tsqlParseIf
	P.h.parseUnique = tsqlParseUnique
	P.h.parseUpdate = tsqlParseUpdate
	P.h.parsePartition = tsqlParsePartition
	P.h.parseAlterTableAlter = tsqlParseAlterTableAlter
	P.h.parsePrimaryKeyPart = tsqlParsePrimaryKeyPart
}

// tsqlParseExecute mirrors TSQLParser._parse_execute.
func tsqlParseExecute(p *Parser) *Expr {
	var returnStatus *Expr
	index := p.index
	if p.match(TK_PARAMETER) {
		param := p.parseParameter()
		if p.match(TK_EQ) {
			returnStatus = param
		} else {
			p.retreat(index)
		}
	}

	this := p.parseTable(true, false, nil, false, false, false, false)
	expressions := p.parseCSV(p.parseExpression, TK_COMMA)
	execute := p.expression(New(
		KExecute,
		"this", this,
		"expressions", expressions,
		"return_status", returnStatus,
	))

	if pyLower(execute.Name()) == "sp_executesql" {
		kv := make([]any, 0, 2*len(execute.args))
		for _, a := range execute.args {
			kv = append(kv, a.key, a.val)
		}
		execute = p.expression(New(KExecuteSql, kv...))
	}

	return execute
}

// tsqlParseDatepart mirrors TSQLParser._parse_datepart.
func tsqlParseDatepart(p *Parser) *Expr {
	this := p.parseVar(false, tsPtr(newTokenSet(TK_IDENTIFIER)), false)
	// self._match(TokenType.COMMA) and self._parse_bitwise() -> False when unmatched
	var expression any = false
	if p.match(TK_COMMA) {
		expression = p.parseBitwise()
	}
	name := mapDatePart(this, p.d)

	return p.expression(New(KExtract, "this", name, "expression", expression))
}

// tsqlParseAlterTableSet mirrors TSQLParser._parse_alter_table_set.
func tsqlParseAlterTableSet(p *Parser) *Expr {
	return p.parseWrapped(p.baseParseAlterTableSet, false)
}

// tsqlParseWrappedSelect mirrors TSQLParser._parse_wrapped_select.
func tsqlParseWrappedSelect(p *Parser, table bool) *Expr {
	if p.match(TK_MERGE) {
		comments := p.prevComments
		merge := p.parseMerge()
		merge.AddComments(comments, true)
		return merge
	}

	return p.baseParseWrappedSelect(table)
}

// tsqlParseDcolon mirrors TSQLParser._parse_dcolon.
func tsqlParseDcolon(p *Parser) *Expr {
	// We want to use _parse_types() if the first token after :: is a known type,
	// otherwise we could parse something like x::varchar(max) into a function
	if p.matchSetNoAdvance(p.s.TYPE_TOKENS) {
		return p.parseTypes(false, false, true, false)
	}

	if f := p.parseFunction(nil, false, true, false); f != nil {
		return f
	}
	return p.parseTypes(false, false, true, false)
}

// tsqlParseOptions mirrors TSQLParser._parse_options. Returns nil (None) or a []*Expr.
func tsqlParseOptions(p *Parser) any {
	if !p.match(TK_OPTION) {
		return nil
	}

	parseOption := func() *Expr {
		option := p.parseVarFromOptions(tsqlOPTIONS, true)
		if option == nil {
			return nil
		}

		p.match(TK_EQ)
		return p.expression(New(KQueryOption, "this", option, "expression", p.parsePrimaryOrVar()))
	}

	return p.parseWrappedCSV(parseOption, TK_COMMA, false)
}

// tsqlParseKeyValueOption mirrors TSQLParser._parse_key_value_option.
func tsqlParseKeyValueOption(p *Parser) *Expr {
	this := p.parsePrimaryOrVar()
	var expression *Expr
	if p.matchNoAdvance(TK_L_PAREN) {
		expression = p.parseWrapped(p.parseString, false)
	}

	return New(KXMLKeyValueOption, "this", this, "expression", expression)
}

// tsqlParseForClauseOption mirrors TSQLParser._parse_for_clause_option.
func tsqlParseForClauseOption(p *Parser, options OptionsType) *Expr {
	this := p.parseVarFromOptions(options, false)
	if this == nil {
		this = tsqlParseKeyValueOption(p)
	}
	return p.expression(New(KQueryOption, "this", this))
}

// tsqlParseFor mirrors TSQLParser._parse_for.
func tsqlParseFor(p *Parser) *Expr {
	if p.matchPair(TK_FOR, TK_XML) {
		return p.expression(New(
			KForClause,
			"kind", "XML",
			"expressions", p.parseCSV(func() *Expr { return tsqlParseForClauseOption(p, tsqlFOR_XML_OPTIONS) }, TK_COMMA),
		))
	}

	if p.matchPair(TK_FOR, TK_JSON) {
		return p.expression(New(
			KForClause,
			"kind", "JSON",
			"expressions", p.parseCSV(func() *Expr { return tsqlParseForClauseOption(p, tsqlFOR_JSON_OPTIONS) }, TK_COMMA),
		))
	}

	// FOR BROWSE — bare keyword, no options. BROWSE has no dedicated TokenType.
	if p.matchTextSeq("FOR", "BROWSE") {
		return p.expression(New(KForClause, "kind", "BROWSE"))
	}

	return nil
}

// tsqlParseProjections mirrors TSQLParser._parse_projections.
//
// T-SQL supports the syntax alias = expression in the SELECT's projection list,
// so we transform all parsed Selects to convert their EQ projections into Aliases.
//
// See: https://learn.microsoft.com/en-us/sql/t-sql/queries/select-clause-transact-sql?view=sql-server-ver16#syntax
func tsqlParseProjections(p *Parser) ([]*Expr, []*Expr) {
	projections, _ := p.baseParseProjections()
	out := make([]*Expr, len(projections))
	for i, projection := range projections {
		if projection.IsA(KEQ) && projection.This().IsA(KColumn) {
			out[i] = AliasExpr(projection.Expression(), projection.This().This(), nil, false)
		} else {
			out[i] = projection
		}
	}
	return out, nil
}

// tsqlParseCommitOrRollback mirrors TSQLParser._parse_commit_or_rollback.
//
// Applies to SQL Server and Azure SQL Database
// COMMIT [ { TRAN | TRANSACTION }
//
//	[ transaction_name | @tran_name_variable ] ]
//	[ WITH ( DELAYED_DURABILITY = { OFF | ON } ) ]
//
// ROLLBACK { TRAN | TRANSACTION }
//
//	[ transaction_name | @tran_name_variable
//	| savepoint_name | @savepoint_variable ]
func tsqlParseCommitOrRollback(p *Parser) *Expr {
	rollback := p.prev.Type == TK_ROLLBACK

	p.matchTexts("TRAN", "TRANSACTION")
	this := p.parseIdVar(true, nil)

	if rollback {
		return p.expression(New(KRollback, "this", this))
	}

	var durability any
	if p.matchPair(TK_WITH, TK_L_PAREN) {
		p.matchTextSeq("DELAYED_DURABILITY")
		p.match(TK_EQ)

		if p.matchTextSeq("OFF") {
			durability = false
		} else {
			p.match(TK_ON)
			durability = true
		}

		p.matchRParen(nil)
	}

	return p.expression(New(KCommit, "this", this, "durability", durability))
}

// tsqlParseTransaction mirrors TSQLParser._parse_transaction.
//
// Applies to SQL Server and Azure SQL Database
// BEGIN { TRAN | TRANSACTION }
// [ { transaction_name | @tran_name_variable }
// [ WITH MARK [ 'description' ] ]
// ].
func tsqlParseTransaction(p *Parser) *Expr {
	if p.matchTexts("TRAN", "TRANSACTION") {
		transaction := p.expression(New(KTransaction, "this", p.parseIdVar(true, nil)))
		if p.matchTextSeq("WITH", "MARK") {
			transaction.Set("mark", p.parseString())
		}

		return transaction
	}

	return p.parseAsCommand(p.prev)
}

// tsqlParseReturns mirrors TSQLParser._parse_returns.
func tsqlParseReturns(p *Parser) *Expr {
	table := p.parseIdVar(false, &p.s.RETURNS_TABLE_TOKENS)
	returns := p.baseParseReturns()
	returns.Set("table", table)
	return returns
}

// tsqlParseConvert mirrors TSQLParser._parse_convert.
func tsqlParseConvert(p *Parser, strict bool, safe bool) *Expr {
	this := p.parseTypes(false, false, true, false)
	p.match(TK_COMMA)
	args := append([]*Expr{this}, p.parseCSV(p.parseAssignment, TK_COMMA)...)
	convert := FromArgList(KConvert, args)
	if safe {
		convert.Set("safe", true)
	} else {
		convert.Set("safe", nil)
	}
	return convert
}

// tsqlParseColumnDef mirrors TSQLParser._parse_column_def.
func tsqlParseColumnDef(p *Parser, this *Expr, computedColumn bool) *Expr {
	this = p.baseParseColumnDef(this, computedColumn)
	if this == nil {
		return nil
	}
	if p.match(TK_EQ) {
		this.Set("default", p.parseDisjunction())
	}
	if p.matchTextSet(p.s.COLUMN_DEFINITION_MODES) {
		this.Set("output", p.prev.Text)
	}
	return this
}

// tsqlParseUserDefinedFunction mirrors TSQLParser._parse_user_defined_function.
func tsqlParseUserDefinedFunction(p *Parser, kind TokenType) *Expr {
	this := p.baseParseUserDefinedFunction(kind)

	if kind == TK_FUNCTION || this.IsA(KUserDefinedFunction) {
		return this
	}

	if kind == TK_PROCEDURE && this != nil {
		expressions := this.Expressions()
		if !(len(expressions) > 0 || p.matchAnyNoAdvance(TK_ALIAS, TK_WITH)) {
			expressions = p.parseCSV(p.parseFunctionParameter, TK_COMMA)
		}

		var spThis *Expr
		if this.IsA(KTable) {
			spThis = this
		} else {
			spThis = this.This()
		}
		return p.expression(New(
			KStoredProcedure,
			"this", spThis,
			"expressions", expressions,
			"wrapped", this.Arg("wrapped"),
		))
	}

	return p.expression(New(KUserDefinedFunction, "this", this))
}

// tsqlParseInto mirrors TSQLParser._parse_into.
func tsqlParseInto(p *Parser) *Expr {
	into := p.baseParseInto()

	var table *Expr
	if into.IsA(KInto) {
		table = into.Find(KTable)
	}
	if table.IsA(KTable) {
		tableIdentifier := table.This()
		if tableIdentifier.ArgB("temporary") {
			// Promote the temporary property from the Identifier to the Into expression
			into.Set("temporary", true)
		}
	}

	return into
}

// tsqlParseIdVar mirrors TSQLParser._parse_id_var.
func tsqlParseIdVar(p *Parser, anyToken bool, tokens *TokenSet) *Expr {
	isTemporary := p.match(TK_HASH)
	isGlobal := isTemporary && p.match(TK_HASH)

	this := p.baseParseIdVar(anyToken, tokens)
	if this != nil {
		if isGlobal {
			this.Set("global_", true)
		} else if isTemporary {
			this.Set("temporary", true)
		}
	}

	return this
}

// tsqlParseTableParts mirrors TSQLParser._parse_table_parts.
func tsqlParseTableParts(p *Parser, schema bool, isDbReference bool, wildcard bool, fast bool) *Expr {
	table := p.baseParseTableParts(schema, isDbReference, wildcard, fast)
	if table.IsA(KTable) && table.This().IsA(KIdentifier) {
		tableName := table.Name()
		if strings.HasPrefix(tableName, "#") {
			if strings.HasPrefix(tableName, "##") {
				table.This().Set("this", tableName[2:])
				table.This().Set("global_", true)
			} else {
				table.This().Set("this", tableName[1:])
				table.This().Set("temporary", true)
			}
		}
	}

	return table
}

// tsqlParseCreate mirrors TSQLParser._parse_create.
func tsqlParseCreate(p *Parser) *Expr {
	create := p.baseParseCreate()

	if create.IsA(KCreate) {
		var table *Expr
		if create.This().IsA(KSchema) {
			table = create.This().This()
		} else {
			table = create.This()
		}
		if table.IsA(KTable) && table.This() != nil && table.This().ArgB("temporary") {
			if !create.ArgB("properties") {
				create.Set("properties", New(KProperties, "expressions", []*Expr{}))
			}

			create.ArgE("properties").Append("expressions", New(KTemporaryProperty))
		}
	}

	return create
}

// tsqlParseIf mirrors TSQLParser._parse_if.
func tsqlParseIf(p *Parser) *Expr {
	this := p.parseCondition()
	trueE := p.parseBlock()

	// self._match(TokenType.ELSE) and self._parse_block() -> False when unmatched
	var falseE any = false
	if p.match(TK_ELSE) {
		falseE = p.parseBlock()
	}

	return p.expression(New(KIfBlock, "this", this, "true", trueE, "false", falseE))
}

// tsqlParseUnique mirrors TSQLParser._parse_unique.
func tsqlParseUnique(p *Parser) *Expr {
	var this *Expr
	if p.matchTexts("CLUSTERED", "NONCLUSTERED") {
		this = p.s.CONSTRAINT_PARSERS[upperText(p.prev)](p)
	} else {
		this = p.parseSchema(p.parseIdVar(false, nil))
	}

	return p.expression(New(KUniqueColumnConstraint, "this", this))
}

// tsqlParseUpdate mirrors TSQLParser._parse_update.
func tsqlParseUpdate(p *Parser) *Expr {
	expression := p.baseParseUpdate()
	expression.Set("options", tsqlParseOptions(p))
	return expression
}

// tsqlParsePartition mirrors TSQLParser._parse_partition.
func tsqlParsePartition(p *Parser) *Expr {
	if !p.matchTextSeq("WITH", "(", "PARTITIONS") {
		return nil
	}

	parseRange := func() *Expr {
		low := p.parseBitwise()
		var high *Expr
		if p.matchTextSeq("TO") {
			high = p.parseBitwise()
		}

		if high != nil {
			return p.expression(New(KPartitionRange, "this", low, "expression", high))
		}
		return low
	}

	partition := p.expression(New(KPartition, "expressions", p.parseWrappedCSV(parseRange, TK_COMMA, false)))

	p.matchRParen(nil)

	return partition
}

// tsqlParseAlterTableAlter mirrors TSQLParser._parse_alter_table_alter.
func tsqlParseAlterTableAlter(p *Parser) *Expr {
	expression := p.baseParseAlterTableAlter()

	if expression != nil {
		collation := expression.ArgE("collate")
		if collation.IsA(KColumn) && collation.This().IsA(KIdentifier) {
			identifier := collation.This()
			collation.Set("this", New(KVar, "this", identifier.Name()))
		}
	}

	return expression
}

// tsqlParsePrimaryKeyPart mirrors TSQLParser._parse_primary_key_part.
func tsqlParsePrimaryKeyPart(p *Parser) *Expr {
	return p.parseOrdered(nil)
}
