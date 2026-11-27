package sqlengine

import (
	"math/big"
	"regexp"
	"strings"
)

func init() { registerCustomizer("postgres", customizePostgres) }

func customizePostgres(d *Dialect) {
	customizePostgresParser(d)
	customizePostgresGenerator(d)
}

func postgresToInterval(interval any) *Expr {
	var s string
	switch x := interval.(type) {
	case string:
		s = x
	case *Expr:
		if x.IsA(KLiteral) {
			if !x.IsString() {
				panic(&ValueError{Msg: "Invalid interval string."})
			}
			s = x.ThisS()
		} else {
			s = exprSQL(x)
		}
	default:
		s = "None"
	}
	result := MaybeParse("INTERVAL "+s, KNone, "", nil)
	if !result.IsA(KInterval) {
		panic(&ValueError{Msg: "AssertionError"})
	}
	return result
}

// postgresSimpleIntervalRE matches a simple interval string such as '1 day', '+0.5 hours' or
// '.5 seconds': a signed number and a unit, with optional surrounding whitespace.
var postgresSimpleIntervalRE = func() *regexp.Regexp {
	s := `[` + chunkDPySpace + `]*`
	return regexp.MustCompile(`^` + s + `(?:[+-]?` + s + `[0-9]+(?:\.[0-9]*)?|\.[0-9]+)` + s + `[a-zA-Z]+` + s + `$`)
}()

// postgresBuildGenerateSeries builds GENERATE_SERIES, turning a simple interval string step such
// as '1 day' into an INTERVAL. Any other step is kept as written: a bare number string like '1'
// steps a numeric series and an INTERVAL is already one.
func postgresBuildGenerateSeries(args []*Expr, d *Dialect) *Expr {
	// Note: postgres allows calls with just two arguments -- the "step" argument defaults to 1
	if step := seqGet(args, 2); step != nil && step.IsString() && postgresSimpleIntervalRE.MatchString(step.ThisS()) {
		interval := postgresToInterval(step.Arg("this"))
		var fraction any
		if strings.Contains(interval.This().Name(), ".") {
			fraction = true
		}
		interval.Set("fraction_in_string", fraction)
		args[2] = interval
	}

	return FromArgList(KExplodingGenerateSeries, args)
}

var postgresMakeIntervalKwargs = map[string]string{
	"years":  "year",
	"months": "month",
	"weeks":  "week",
	"days":   "day",
	"hours":  "hour",
	"mins":   "minute",
	"secs":   "second",
}

// postgresBuildMakeInterval builds MAKE_INTERVAL: positional arguments fill years, months, weeks,
// days, hours, mins and secs in order, and named ones go to their own unit, e.g.
// MAKE_INTERVAL(days => 3) is 3 days.
func postgresBuildMakeInterval(args []*Expr, _ *Dialect) *Expr {
	e := New(KMakeInterval)
	for i, spec := range KMakeInterval.ArgTypes() {
		if i >= len(args) {
			break
		}
		key := spec.name
		if value := args[i]; value.IsA(KKwarg) {
			if k, ok := postgresMakeIntervalKwargs[pyLower(value.This().Name())]; ok {
				key = k
			}
		}
		e.Set(key, args[i])
	}
	return e
}

func postgresBuildToTimestamp(args []*Expr, d *Dialect) *Expr {
	// TO_TIMESTAMP accepts either a single double argument or (text, text)
	if len(args) == 1 {
		// https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-TABLE
		return FromArgList(KUnixToTime, args)
	}

	// https://www.postgresql.org/docs/current/functions-formatting.html
	return buildFormattedTime(KStrToTime, "", nil)(args, d)
}

func postgresBuildRegexpReplace(args []*Expr, d *Dialect) *Expr {
	// The signature of REGEXP_REPLACE is:
	// regexp_replace(source, pattern, replacement [, start [, N ]] [, flags ])
	//
	// Any one of `start`, `N` and `flags` can be column references, meaning that
	// unless we can statically see that the last argument is a non-integer string
	// (eg. not '0'), then it's not possible to construct the correct AST
	var regexpReplace *Expr
	if len(args) > 3 {
		last := args[len(args)-1]
		if !isPyInt(last.Name()) {
			if last.Type() == nil || dhIsType(last, DT_UNKNOWN, DT_NULL) {
				last = annotateTypes(last, d)
			}

			if dhIsType(last, DataType_TEXT_TYPES.Items()...) {
				regexpReplace = FromArgList(KRegexpReplace, args[:len(args)-1])
				regexpReplace.Set("modifiers", last)
			}
		}
	}

	if regexpReplace == nil {
		regexpReplace = FromArgList(KRegexpReplace, args)
	}
	regexpReplace.Set("single_replace", true)
	return regexpReplace
}

func postgresBuildLevenshteinLessEqual(args []*Expr, d *Dialect) *Expr {
	// Postgres has two signatures for levenshtein_less_equal function, but in both cases
	// max_dist is the last argument
	// levenshtein_less_equal(source, target, ins_cost, del_cost, sub_cost, max_d)
	// levenshtein_less_equal(source, target, max_d)
	if len(args) == 0 {
		panic(&ValueError{Msg: "pop from empty list"})
	}
	maxDist := args[len(args)-1]
	args = args[:len(args)-1]

	// Validate arity against the arguments without max_dist.
	return WithValidateArgs(New(
		KLevenshtein,
		"this", seqGet(args, 0),
		"expression", seqGet(args, 1),
		"ins_cost", seqGet(args, 2),
		"del_cost", seqGet(args, 3),
		"sub_cost", seqGet(args, 4),
		"max_dist", maxDist,
	), args)
}

func customizePostgresParser(d *Dialect) {
	P := d.P

	delete(P.PROPERTY_PARSERS, "INPUT")
	P.PROPERTY_PARSERS["SET"] = noKwargsE(func(p *Parser) *Expr {
		return p.expression(New(KSetConfigProperty, "this", p.parseSet(false, false)))
	})
	P.PROPERTY_PARSERS["TABLESPACE"] = noKwargsE(func(p *Parser) *Expr {
		return p.expression(New(KTableSpaceProperty, "this", p.parseIdVar(true, nil)))
	})

	P.PLACEHOLDER_PARSERS[TK_PLACEHOLDER] = func(p *Parser) *Expr {
		return p.expression(New(KPlaceholder, "jdbc", true))
	}
	P.PLACEHOLDER_PARSERS[TK_MOD] = func(p *Parser) *Expr { return postgresParseQueryParameter(p) }

	P.FUNCTIONS["ARRAY_PREPEND"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KArrayPrepend, "this", seqGet(args, 1), "expression", seqGet(args, 0))
	}
	P.FUNCTIONS["BIT_AND"] = fromArgList(KBitwiseAndAgg)
	P.FUNCTIONS["BIT_OR"] = fromArgList(KBitwiseOrAgg)
	P.FUNCTIONS["BIT_XOR"] = fromArgList(KBitwiseXorAgg)
	P.FUNCTIONS["BTRIM"] = fromArgList(KTrim)
	P.FUNCTIONS["MAKE_INTERVAL"] = postgresBuildMakeInterval
	P.FUNCTIONS["VERSION"] = fromArgList(KCurrentVersion)
	P.FUNCTIONS["DATE_TRUNC"] = buildTimestampTrunc
	P.FUNCTIONS["DIV"] = func(args []*Expr, d *Dialect) *Expr {
		return CastExpr(binaryFromFunction(KIntDiv)(args, d), DT_DECIMAL, true, nil)
	}
	P.FUNCTIONS["GENERATE_SERIES"] = postgresBuildGenerateSeries
	P.FUNCTIONS["GET_BIT"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KGetbit, "this", seqGet(args, 0), "expression", seqGet(args, 1), "zero_is_msb", true)
	}
	P.FUNCTIONS["JSON_EXTRACT_PATH"] = buildJSONExtractPath(KJSONExtract, true, false, "")
	P.FUNCTIONS["JSON_EXTRACT_PATH_TEXT"] = buildJSONExtractPath(KJSONExtractScalar, true, false, "")
	P.FUNCTIONS["LENGTH"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KLength, "this", seqGet(args, 0), "encoding", seqGet(args, 1))
	}
	P.FUNCTIONS["MAKE_TIME"] = fromArgList(KTimeFromParts)
	P.FUNCTIONS["MAKE_TIMESTAMP"] = fromArgList(KTimestampFromParts)
	P.FUNCTIONS["NOW"] = fromArgList(KCurrentTimestamp)
	P.FUNCTIONS["REGEXP_REPLACE"] = postgresBuildRegexpReplace
	P.FUNCTIONS["TO_CHAR"] = buildFormattedTime(KTimeToStr, "", nil)
	P.FUNCTIONS["TO_DATE"] = buildFormattedTime(KStrToDate, "", nil)
	P.FUNCTIONS["TO_TIMESTAMP"] = postgresBuildToTimestamp
	P.FUNCTIONS["UNNEST"] = fromArgList(KExplode)
	P.FUNCTIONS["SHA256"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KSHA2, "this", seqGet(args, 0), "length", LiteralInt(256))
	}
	P.FUNCTIONS["SHA384"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KSHA2, "this", seqGet(args, 0), "length", LiteralInt(384))
	}
	P.FUNCTIONS["SHA512"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KSHA2, "this", seqGet(args, 0), "length", LiteralInt(512))
	}
	P.FUNCTIONS["LEVENSHTEIN_LESS_EQUAL"] = postgresBuildLevenshteinLessEqual
	P.FUNCTIONS["JSON_OBJECT_AGG"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KJSONObjectAgg, "expressions", args)
	}
	P.FUNCTIONS["JSONB_OBJECT_AGG"] = fromArgList(KJSONBObjectAgg)
	P.FUNCTIONS["WIDTH_BUCKET"] = func(args []*Expr, _ *Dialect) *Expr {
		if len(args) == 2 {
			return New(KWidthBucket, "this", seqGet(args, 0), "threshold", seqGet(args, 1))
		}
		return FromArgList(KWidthBucket, args)
	}
	P.FUNCTIONS["UUID"] = func(args []*Expr, _ *Dialect) *Expr {
		if len(args) > 0 {
			return CastExpr(args[0], DT_UUID, true, nil)
		}
		return New(KUuid)
	}

	P.NO_PAREN_FUNCTION_PARSERS["VARIADIC"] = func(p *Parser) *Expr {
		return p.expression(New(KVariadic, "this", p.parseBitwise()))
	}

	P.FUNCTION_PARSERS["DATE_PART"] = func(p *Parser) *Expr { return postgresParseDatePart(p) }
	P.FUNCTION_PARSERS["JSON_AGG"] = func(p *Parser) *Expr {
		this := p.parseLambda(false)
		order := p.parseOrder(nil, false)
		return p.expression(New(KJSONArrayAgg, "this", this, "order", order))
	}
	P.FUNCTION_PARSERS["JSONB_EXISTS"] = func(p *Parser) *Expr { return postgresParseJSONBExists(p) }
	// The path stays a plain expression: Postgres JSON paths are SQL/JSON path strings
	P.FUNCTION_PARSERS["JSON_VALUE"] = func(p *Parser) *Expr { return p.parseJsonValue(false) }

	P.RANGE_PARSERS[TK_DAMP] = binaryRangeParser(KArrayOverlaps, false)
	P.RANGE_PARSERS[TK_DAT] = func(p *Parser, this *Expr) *Expr {
		return p.expression(New(KMatchAgainst, "this", p.parseBitwise(), "expressions", []*Expr{this}))
	}

	P.STATEMENT_PARSERS[TK_END] = func(p *Parser) *Expr { return p.parseCommitOrRollback() }

	// The `~` token is remapped from TILDE to RLIKE in Postgres due to the binary REGEXP LIKE operator
	P.UNARY_PARSERS[TK_RLIKE] = func(p *Parser) *Expr {
		return p.expression(New(KBitwiseNot, "this", p.parseUnary()))
	}

	// The JSON / JSONB operators are binary operators at the "any other operator" precedence tier
	// (below + and -, level with ||), not column operators
	for _, tok := range []TokenType{TK_ARROW, TK_DARROW, TK_HASH_ARROW, TK_DHASH_ARROW, TK_PLACEHOLDER} {
		delete(P.COLUMN_OPERATORS, tok)
	}
	P.JSON_OPERATORS = map[TokenType]columnOperatorFn{
		TK_ARROW: func(p *Parser, this, path *Expr) *Expr {
			return p.validateExpression(
				buildJSONExtractPath(KJSONExtract, true, p.s.JSON_ARROWS_REQUIRE_JSON_TYPE, "")([]*Expr{this, path}, p.d),
				nil,
			)
		},
		TK_DARROW: func(p *Parser, this, path *Expr) *Expr {
			return p.validateExpression(
				buildJSONExtractPath(KJSONExtractScalar, true, p.s.JSON_ARROWS_REQUIRE_JSON_TYPE, "")([]*Expr{this, path}, p.d),
				nil,
			)
		},
		TK_HASH_ARROW: func(p *Parser, this, path *Expr) *Expr {
			return p.expression(New(KJSONBExtract, "this", this, "expression", path))
		},
		TK_DHASH_ARROW: func(p *Parser, this, path *Expr) *Expr {
			return p.expression(New(KJSONBExtractScalar, "this", this, "expression", path))
		},
		TK_PLACEHOLDER: func(p *Parser, this, key *Expr) *Expr {
			return p.expression(New(KJSONBContainsTopKey, "this", this, "expression", key))
		},
	}

	P.h.parseFunctionParameter = postgresParseFunctionParameter
	P.h.parseUnique = postgresParseUnique
	P.h.parseConvert = postgresParseConvert
	P.h.parsePrimaryKey = postgresParsePrimaryKey
	P.h.parseGeneratedAsIdentity = postgresParseGeneratedAsIdentity
	P.h.parseUserDefinedType = postgresParseUserDefinedType
}

// postgresParseParameterMode reports the function parameter mode (IN, OUT, INOUT, VARIADIC) at
// the current token without consuming it; ok=false means there is no mode.
//
// Disambiguates between mode keywords and identifiers with the same name:
// - MODE TYPE      -> keyword is identifier (e.g., "out INT")
// - MODE NAME TYPE -> keyword is mode (e.g., "OUT x INT").
func postgresParseParameterMode(p *Parser) (TokenType, bool) {
	if !p.matchSetNoAdvance(p.s.ARG_MODE_TOKENS) || !p.next.ok() {
		return 0, false
	}

	modeToken := p.curr

	// MODE TYPE: a built-in type (not a UDT) after the keyword makes it an identifier.
	isFollowedByBuiltinType := p.tryParseExpr(func() *Expr {
		p.advance(1)
		return p.parseTypes(false, false, false, false)
	}, true)
	if isFollowedByBuiltinType != nil {
		return 0, false
	}

	// MODE NAME TYPE: the type can be built-in or user-defined (allowIdentifiers=true).
	if !p.s.ID_VAR_TOKENS.Has(p.next.Type) {
		return 0, false
	}

	isFollowedByAnyType := p.tryParseExpr(func() *Expr {
		p.advance(2)
		return p.parseTypes(false, false, true, false)
	}, true)

	if isFollowedByAnyType != nil {
		return modeToken.Type, true
	}

	return 0, false
}

func postgresCreateModeConstraint(p *Parser, paramMode TokenType) *Expr {
	return p.expression(New(
		KInOutColumnConstraint,
		"input_", paramMode == TK_IN || paramMode == TK_INOUT,
		"output", paramMode == TK_OUT || paramMode == TK_INOUT,
		"variadic", paramMode == TK_VARIADIC,
	))
}

func postgresParseFunctionParameter(p *Parser) *Expr {
	paramMode, hasMode := postgresParseParameterMode(p)

	if hasMode {
		p.advance(1)
	}

	paramName := p.parseIdVar(true, nil)
	columnDef := p.parseColumnDef(paramName, false)

	if hasMode && columnDef != nil {
		constraint := postgresCreateModeConstraint(p, paramMode)
		if !columnDef.ArgB("constraints") {
			columnDef.Set("constraints", []*Expr{})
		}
		// Prepend in place, without parent bookkeeping.
		constraints := append([]*Expr{constraint}, columnDef.ArgL("constraints")...)
		columnDef.SetArgRaw("constraints", constraints)
	}

	return columnDef
}

// postgresParseQueryParameter parses a %s or %(name)s query parameter placeholder.
func postgresParseQueryParameter(p *Parser) *Expr {
	var this *Expr
	if p.matchNoAdvance(TK_L_PAREN) {
		this = p.parseWrapped(func() *Expr { return p.parseIdVar(true, nil) }, false)
	}
	p.matchTextSeq("S")
	return p.expression(New(KPlaceholder, "this", this))
}

func postgresParseDatePart(p *Parser) *Expr {
	part := p.parseType(true, false)
	p.match(TK_COMMA)
	value := p.parseBitwise()

	if part != nil && part.IsA(KColumn, KLiteral) {
		part = VarChecked(pyUpper(part.Name()))
	}

	return p.expression(New(KExtract, "this", part, "expression", value))
}

func postgresParseConvert(p *Parser, _ bool, _ bool) *Expr {
	args := p.parseCSV(p.parseAssignment, TK_COMMA)
	return p.validateExpression(
		New(KConvertToCharset, "this", seqGet(args, 0), "source", seqGet(args, 1), "dest", seqGet(args, 2)),
		args,
	)
}

// postgresParseUnique parses UNIQUE [NULLS NOT DISTINCT] [(columns)] followed by index
// parameters (INCLUDE, WITH, USING INDEX TABLESPACE) and key constraint options.
func postgresParseUnique(p *Parser) *Expr {
	nulls := p.matchTextSeq("NULLS", "NOT", "DISTINCT")
	this := p.parseSchema(nil)
	include := p.parseIndexParams()
	options := p.parseKeyConstraintOptions()
	return p.expression(New(
		KUniqueColumnConstraint,
		"nulls", nulls,
		"this", this,
		"include", include,
		"options", options,
	))
}

func postgresParsePrimaryKey(p *Parser, wrappedOptional bool, inProps bool, namedPrimaryKey bool) *Expr {
	this := p.baseParsePrimaryKey(wrappedOptional, inProps, namedPrimaryKey)
	if this.IsA(KPrimaryKeyColumnConstraint) {
		this.Set("include", p.parseIndexParams())
		options, _ := this.Arg("options").([]string)
		this.Set("options", append(append([]string{}, options...), p.parseKeyConstraintOptions()...))
	}
	return this
}

func postgresParseJSONBExists(p *Parser) *Expr {
	this := p.parseBitwise()
	// path is false (not nil) when there is no comma: false passes the required-argument
	// validation.
	var path any = false
	if p.match(TK_COMMA) {
		path = anyExpr(p.d.toJSONPath(p.parseBitwise()))
	}
	return p.expression(New(KJSONBExists, "this", this, "path", path))
}

func postgresParseGeneratedAsIdentity(p *Parser) *Expr {
	this := p.baseParseGeneratedAsIdentity()

	if p.matchTextSeq("STORED") {
		this = p.expression(New(KComputedColumnConstraint, "this", this.Expression()))
	}

	return this
}

func postgresParseUserDefinedType(p *Parser, identifier *Expr) *Expr {
	udtType := identifier

	for p.match(TK_DOT) {
		part := p.parseIdVar(true, nil)
		if part != nil {
			udtType = New(KDot, "this", udtType, "expression", part)
		}
	}

	return DataTypeBuild(udtType, nil, true, true)
}

// postgresPyInt converts a string or number literal to an integer, truncating fractional
// numbers. It panics with a ValueError when the value cannot be converted; the message keeps its
// historical wording.
func postgresPyInt(e *Expr) *big.Int {
	text := e.ThisS()
	if e.IsString() {
		if isPyInt(text) {
			if i, ok := new(big.Int).SetString(strings.ReplaceAll(pyStrip(text), "_", ""), 10); ok {
				return i
			}
		}
		panic(&ValueError{Msg: "invalid literal for int() with base 10: " + pyRepr(text)})
	}
	v := chunkDToPyNumber(e)
	if v.isInt {
		return v.i
	}
	f, ok := new(big.Float).SetString(v.String())
	if !ok {
		panic(&ValueError{Msg: "cannot convert to int"})
	}
	i, _ := f.Int(nil)
	return i
}
