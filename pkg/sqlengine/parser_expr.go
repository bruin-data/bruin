package sqlengine

import (
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

// Port of sqlglot.parser.Parser (chunk D, part 1): the expression precedence chain
// (_parse_expression .. _parse_unary) and data types (_parse_type .. _parse_at_time_zone).

// pySpaceClass mirrors Python's Unicode `\s` inside a character class.
const chunkDPySpace = `\t\n\x0b\x0c\r\x1c-\x1f \x{85}\x{a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}`

// chunkDIntervalStringRE mirrors exp.INTERVAL_STRING_RE.
var chunkDIntervalStringRE = regexp.MustCompile(
	`[` + chunkDPySpace + `]*(-?[0-9]+(?:\.[0-9]+)?)[` + chunkDPySpace + `]*([a-zA-Z]+)[` + chunkDPySpace + `]*`,
)

// chunkDIntervalDayTimeRE mirrors exp.INTERVAL_DAY_TIME_RE (used with re.match, hence anchored).
var chunkDIntervalDayTimeRE = func() *regexp.Regexp {
	s := `[` + chunkDPySpace + `]`
	d := `\p{Nd}`
	return regexp.MustCompile(
		`^` + s + `*-?` + s + `*` + d + `+(?:\.` + d + `+)?` + s + `+(?:-?(?:` + d + `+:)?` + d + `+:` + d +
			`+(?:\.` + d + `+)?|-?(?:` + d + `+:){1,2}|:)` + s + `*`,
	)
}()

// chunkDTimeZoneRE mirrors sqlglot.parser.TIME_ZONE_RE (used with re.search).
var chunkDTimeZoneRE = regexp.MustCompile(`:.*?[a-zA-Z\+\-]`)

// chunkDMatchKey mirrors self._match_set(mapping) for token-keyed mappings.
func chunkDMatchKey[V any](p *Parser, m map[TokenType]V) bool {
	if !p.curr.ok() {
		return false
	}
	if _, ok := m[p.curr.Type]; ok {
		p.advance(1)
		return true
	}
	return false
}

// _parse_expression (parser.py L5828).
func (p *Parser) baseParseExpression() *Expr {
	return p.parseAlias(p.parseAssignment(), false)
}

// _parse_assignment (parser.py L5831).
func (p *Parser) baseParseAssignment() *Expr {
	this := p.parseDisjunction()
	if this == nil {
		if _, ok := p.s.ASSIGNMENT[p.next.Type]; ok {
			// This allows us to parse <non-identifier token> := <expr>
			var col any
			if p.advanceAny(true) != nil {
				col = ToIdentifier(p.prev.Text, nil)
			}
			this = New(KColumn, "this", col, "table", nil, "db", nil, "catalog", nil)
		}
	}

	for chunkDMatchKey(p, p.s.ASSIGNMENT) {
		if this.IsA(KColumn) && len(this.Parts()) == 1 {
			this = this.This()
		}

		comments := p.prevComments
		kind := p.s.ASSIGNMENT[p.prev.Type]
		this = p.expressionC(New(kind, "this", this, "expression", p.parseAssignment()), comments)
	}

	return this
}

// _parse_disjunction (parser.py L5853).
func (p *Parser) parseDisjunction() *Expr {
	this := p.parseConjunction()
	for chunkDMatchKey(p, p.s.DISJUNCTION) {
		comments := p.prevComments
		kind := p.s.DISJUNCTION[p.prev.Type]
		this = p.expressionC(New(kind, "this", this, "expression", p.parseConjunction()), comments)
	}
	return this
}

// _parse_conjunction (parser.py L5865).
func (p *Parser) parseConjunction() *Expr {
	this := p.parseEquality()
	for chunkDMatchKey(p, p.s.CONJUNCTION) {
		comments := p.prevComments
		kind := p.s.CONJUNCTION[p.prev.Type]
		this = p.expressionC(New(kind, "this", this, "expression", p.parseEquality()), comments)
	}
	return this
}

// _parse_equality (parser.py L5877).
func (p *Parser) parseEquality() *Expr {
	this := p.parseComparison()
	for chunkDMatchKey(p, p.s.EQUALITY) {
		comments := p.prevComments
		kind := p.s.EQUALITY[p.prev.Type]
		this = p.expressionC(New(kind, "this", this, "expression", p.parseComparison()), comments)
	}
	return this
}

// _parse_comparison (parser.py L5889).
func (p *Parser) parseComparison() *Expr {
	this := p.parseRange(nil)
	for chunkDMatchKey(p, p.s.COMPARISON) {
		comments := p.prevComments
		kind := p.s.COMPARISON[p.prev.Type]
		this = p.expressionC(New(kind, "this", this, "expression", p.parseRange(nil)), comments)
	}
	return this
}

// _parse_range (parser.py L5899).
func (p *Parser) parseRange(this *Expr) *Expr {
	if this == nil {
		this = p.parseBitwise()
	}

	for {
		negate := p.match(TK_NOT)
		if chunkDMatchKey(p, p.s.RANGE_PARSERS) {
			expression := p.s.RANGE_PARSERS[p.prev.Type](p, this)
			if expression == nil {
				return this
			}

			this = expression
		} else if p.match(TK_ISNULL) || (negate && p.match(TK_NULL)) {
			this = p.expression(New(KIs, "this", this, "expression", Null()))
		} else if p.match(TK_NOTNULL) {
			// Postgres supports ISNULL and NOTNULL for conditions.
			// https://blog.andreiavram.ro/postgresql-null-composite-type/
			this = p.expression(New(KIs, "this", this, "expression", Null()))
			this = p.expression(New(KNot, "this", this))
		} else {
			if negate {
				p.retreat(p.index - 1)
			}
			break
		}

		if negate {
			this = p.negateRange(this)
			if p.curr.ok() {
				_, isRange := p.s.RANGE_PARSERS[p.curr.Type]
				if p.curr.Type == TK_NOT || isRange {
					this = p.expression(New(KParen, "this", this))
				}
			}
		}
	}

	return this
}

// _negate_range (parser.py L5932).
func (p *Parser) baseNegateRange(this *Expr) *Expr {
	if this == nil {
		return this
	}

	expression := this
	if this.IsA(KEscape) {
		expression = this.This()
	}
	if expression.IsA(KLike, KILike) {
		expression.Set("negate", true)
		return this
	}

	return p.expression(New(KNot, "this", this))
}

// _parse_is (parser.py L5943).
func (p *Parser) parseIs(this *Expr) *Expr {
	index := p.index - 1
	negate := p.match(TK_NOT)

	if p.matchTextSeq("DISTINCT", "FROM") {
		klass := KNullSafeNEQ
		if negate {
			klass = KNullSafeEQ
		}
		return p.expression(New(klass, "this", this, "expression", p.parseBitwise()))
	}

	var expression *Expr
	if p.match(TK_JSON) {
		var kind any = false
		if p.matchTextSet(p.s.IS_JSON_PREDICATE_KIND) {
			kind = upperText(p.prev)
		}

		var with any
		if p.matchTextSeq("WITH") {
			with = true
		} else if p.matchTextSeq("WITHOUT") {
			with = false
		}

		unique := p.match(TK_UNIQUE)
		p.matchTextSeq("KEYS")
		expression = p.expression(New(KJSON, "this", kind, "with_", with, "unique", unique))
	} else {
		expression = p.parseNull()
		if expression == nil {
			expression = p.parseBitwise()
		}
		if expression == nil {
			p.retreat(index)
			return nil
		}
	}

	this = p.expression(New(KIs, "this", this, "expression", expression))
	if negate {
		this = p.expression(New(KNot, "this", this))
	}
	return p.parseColumnOps(this)
}

// _parse_in (parser.py L5976).
func (p *Parser) parseIn(this *Expr, alias bool) *Expr {
	unnest := p.parseUnnest(false)
	if unnest != nil {
		this = p.expression(New(KIn, "this", this, "unnest", unnest))
	} else if p.matchAny(TK_L_PAREN, TK_L_BRACKET) {
		matchedLParen := p.prev.Type == TK_L_PAREN
		expressions := p.parseCSV(func() *Expr { return p.parseSelectOrExpression(alias) }, TK_COMMA)

		if len(expressions) == 1 && expressions[0].IsA(KQuery) {
			query := expressions[0]
			this = p.expression(New(KIn, "this", this, "query", chunkDSubquery(p.parseQueryModifiers(query))))
		} else {
			this = p.expression(New(KIn, "this", this, "expressions", expressions))
		}

		if matchedLParen {
			p.matchRParen(this)
		} else if !p.matchExpr(TK_R_BRACKET, this) {
			p.raiseError("Expecting ]", nil)
		}
	} else {
		this = p.expression(New(KIn, "this", this, "field", p.parseColumn()))
	}

	return this
}

// _parse_between (parser.py L6000).
func (p *Parser) parseBetween(this *Expr) *Expr {
	var symmetric any
	if p.matchTextSeq("SYMMETRIC") {
		symmetric = true
	} else if p.matchTextSeq("ASYMMETRIC") {
		symmetric = false
	}

	low := p.parseBitwise()
	p.match(TK_AND)
	high := p.parseBitwise()

	return p.expression(New(KBetween, "this", this, "low", low, "high", high, "symmetric", symmetric))
}

// _parse_escape (parser.py L6013).
func (p *Parser) parseEscape(this *Expr) *Expr {
	if !p.match(TK_ESCAPE) {
		return this
	}
	expression := p.parseString()
	if expression == nil {
		expression = p.parseNull()
	}
	return p.expression(New(KEscape, "this", this, "expression", expression))
}

// _parse_interval_span (parser.py L6020).
func (p *Parser) parseIntervalSpan(this *Expr) *Expr {
	// handle day-time format interval span with omitted units:
	//   INTERVAL '<number days> hh[:][mm[:ss[.ff]]]' <maybe `unit TO unit`>
	intervalSpanUnitsOmitted := false
	if this != nil && this.IsString() && p.s.SUPPORTS_OMITTED_INTERVAL_SPAN_UNIT &&
		chunkDIntervalDayTimeRE.MatchString(this.Name()) {
		index := p.index

		// Var "TO" Var
		firstUnit := p.parseVar(true, nil, true)
		var secondUnit *Expr
		if firstUnit != nil && p.matchTextSeq("TO") {
			secondUnit = p.parseVar(true, nil, true)
		}

		intervalSpanUnitsOmitted = !(firstUnit != nil && secondUnit != nil)

		p.retreat(index)
	}

	var unit *Expr
	if !intervalSpanUnitsOmitted {
		unit = p.parseFunction(nil, false, true, false)
		if unit == nil && (p.curr.Type == TK_VAR || p.d.S.VALID_INTERVAL_UNITS.Has(upperText(p.curr))) {
			unit = p.parseVar(true, nil, true)
		}
	}

	// Most dialects support, e.g., the form INTERVAL '5' day, thus we try to parse
	// each INTERVAL expression into this canonical form so it's easy to transpile
	if this != nil && this.IsNumber() {
		this = LiteralString(chunkDPyNumberStr(this))
	} else if this != nil && this.IsString() {
		parts := chunkDIntervalStringRE.FindAllStringSubmatch(this.Name(), -1)
		if len(parts) > 0 && unit != nil {
			// Unconsume the eagerly-parsed unit, since the real unit was part of the string
			unit = nil
			p.retreat(p.index - 1)
		}

		if len(parts) == 1 {
			this = LiteralString(parts[0][1])
			unit = p.expression(New(KVar, "this", pyUpper(parts[0][2])))
		}
	}

	if p.s.INTERVAL_SPANS && p.matchTextSeq("TO") {
		expression := p.parseFunction(nil, false, true, false)
		if expression == nil {
			expression = p.parseVar(true, nil, true)
		}
		unit = p.expression(New(KIntervalSpan, "this", unit, "expression", expression))
	}

	return p.expression(New(KInterval, "this", this, "unit", unit))
}

// _parse_interval (parser.py L6078).
func (p *Parser) parseInterval(requireInterval bool) *Expr {
	index := p.index

	if !p.match(TK_INTERVAL) && requireInterval {
		return nil
	}

	var this *Expr
	if p.matchNoAdvance(TK_STRING) {
		this = p.parsePrimary()
	} else {
		this = p.parseTerm()
	}

	if this == nil || (this.IsA(KColumn) &&
		this.TableName() == "" &&
		!this.This().ArgB("quoted") &&
		p.curr.ok() &&
		!p.d.S.VALID_INTERVAL_UNITS.Has(upperText(p.curr))) {
		p.retreat(index)
		return nil
	}

	interval := p.parseIntervalSpan(this)

	index = p.index
	p.match(TK_PLUS)

	// Convert INTERVAL 'val_1' unit_1 [+] ... [+] 'val_n' unit_n into a sum of intervals
	if p.matchAnyNoAdvance(TK_STRING, TK_NUMBER) {
		return p.expression(New(KAdd, "this", interval, "expression", p.parseInterval(false)))
	}

	p.retreat(index)
	return interval
}

// _parse_bitwise (parser.py L6111).
func (p *Parser) parseBitwise() *Expr {
	this := p.parseTerm()

	for {
		if chunkDMatchKey(p, p.s.BITWISE) {
			kind := p.s.BITWISE[p.prev.Type]
			this = p.expression(New(kind, "this", this, "expression", p.parseTerm()))
		} else if p.d.S.DPIPE_IS_STRING_CONCAT && p.match(TK_DPIPE) {
			this = p.expression(New(
				KDPipe,
				"this", this,
				"expression", p.parseTerm(),
				"safe", !p.d.S.STRICT_STRING_CONCAT,
			))
		} else if p.match(TK_DQMARK) {
			this = p.expression(New(KCoalesce, "this", this, "expressions", ensureList(p.parseTerm())))
		} else if p.matchPair(TK_LT, TK_LT) {
			this = p.expression(New(KBitwiseLeftShift, "this", this, "expression", p.parseTerm()))
		} else if p.matchPair(TK_GT, TK_GT) {
			this = p.expression(New(KBitwiseRightShift, "this", this, "expression", p.parseTerm()))
		} else {
			break
		}
	}

	return this
}

// _parse_term (parser.py L6144).
func (p *Parser) parseTerm() *Expr {
	this := p.parseFactor()

	for chunkDMatchKey(p, p.s.TERM) {
		klass := p.s.TERM[p.prev.Type]
		comments := p.prevComments
		expression := p.parseFactor()

		this = p.expressionC(New(klass, "this", this, "expression", expression), comments)

		if this.IsA(KCollate) {
			expr := this.Expression()

			// Preserve collations such as pg_catalog."default" (Postgres) as columns, otherwise
			// fallback to Identifier / Var
			if expr.IsA(KColumn) && len(expr.Parts()) == 1 {
				ident := expr.This()
				if ident.IsA(KIdentifier) {
					if ident.ArgB("quoted") {
						this.Set("expression", ident)
					} else {
						this.Set("expression", VarExpr(ident.Name()))
					}
				}
			}
		}
	}

	return this
}

// _parse_factor (parser.py L6166).
func (p *Parser) parseFactor() *Expr {
	parseMethod := p.parseUnary
	if len(p.s.EXPONENT) > 0 {
		parseMethod = p.parseExponent
	}
	this := p.parseAtTimeZone(parseMethod())

	for chunkDMatchKey(p, p.s.FACTOR) {
		klass := p.s.FACTOR[p.prev.Type]
		comments := p.prevComments
		expression := parseMethod()

		if expression == nil && klass == KIntDiv && chunkDIsAlpha(p.prev.Text) {
			p.retreat(p.index - 1)
			return this
		}

		this = p.expressionC(New(klass, "this", this, "expression", expression), comments)

		if this.IsA(KDiv) {
			this.Set("typed", p.d.S.TYPED_DIVISION)
			this.Set("safe", p.d.S.SAFE_DIVISION)
		}
	}

	return this
}

// chunkDIsAlpha mirrors str.isalpha().
func chunkDIsAlpha(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !pyIsAlphaRune(r) {
			return false
		}
	}
	return true
}

// _parse_exponent (parser.py L6187).
func (p *Parser) parseExponent() *Expr {
	this := p.parseUnary()
	for chunkDMatchKey(p, p.s.EXPONENT) {
		comments := p.prevComments
		kind := p.s.EXPONENT[p.prev.Type]
		this = p.expressionC(New(kind, "this", this, "expression", p.parseUnary()), comments)
	}
	return this
}

// _parse_unary (parser.py L6197).
func (p *Parser) parseUnary() *Expr {
	if chunkDMatchKey(p, p.s.UNARY_PARSERS) {
		return p.s.UNARY_PARSERS[p.prev.Type](p)
	}
	return p.parseType(true, false)
}

// _parse_type (parser.py L6202).
func (p *Parser) baseParseType(parseInterval bool, fallbackToIdentifier bool) *Expr {
	if !fallbackToIdentifier {
		if atom := p.parseAtom(); atom != nil {
			return atom
		}
	}

	if parseInterval {
		if interval := p.parseInterval(true); interval != nil {
			return p.parseColumnOps(interval)
		}
	}

	index := p.index
	dataType := p.parseTypes(true, false, false, false)

	// parse_types() returns a Cast if we parsed BQ's inline constructor <type>(<values>) e.g.
	// STRUCT<a INT, b STRING>(1, 'foo'), which is canonicalized to CAST(<values> AS <type>)
	if dataType.IsA(KCast) {
		// This constructor can contain ops directly after it, for instance struct unnesting:
		// STRUCT<a INT, b STRING>(1, 'foo').* --> CAST(STRUCT(1, 'foo') AS STRUCT<a iNT, b STRING).*
		return p.parseColumnOps(dataType)
	}

	if dataType != nil {
		index2 := p.index
		this := p.parsePrimary()

		if this.IsA(KLiteral) {
			literal := this.Name()
			this = p.parseColumnOps(this)

			if parser, ok := p.s.TYPE_LITERAL_PARSERS[dataType.DTypeOf()]; ok && parser != nil {
				return parser(p, this, dataType)
			}

			if p.s.ZONE_AWARE_TIMESTAMP_CONSTRUCTOR &&
				chunkDIsType(dataType, DT_TIMESTAMP) &&
				chunkDTimeZoneRE.MatchString(literal) {
				dataType = NewDataType(DT_TIMESTAMPTZ)
			}

			return p.expression(New(KCast, "this", this, "to", dataType))
		}

		// The expressions arg gets set by the parser when we have something like DECIMAL(38, 0)
		// in the input SQL. In that case, we'll produce these tokens: DECIMAL ( 38 , 0 )
		//
		// If the index difference here is greater than 1, that means the parser itself must have
		// consumed additional tokens such as the DECIMAL scale and precision in the above example.
		//
		// If it's not greater than 1, then it must be 1, because we've consumed at least the type
		// keyword, meaning that the expressions arg of the DataType must have gotten set by a
		// callable in the TYPE_CONVERTERS mapping. For example, Snowflake converts DECIMAL to
		// DECIMAL(38, 0)) in order to facilitate the data type's transpilation.
		//
		// In these cases, we don't really want to return the converted type, but instead retreat
		// and try to parse a Column or Identifier in the section below.
		if len(dataType.Expressions()) > 0 && index2-index > 1 {
			p.retreat(index2)
			return p.parseColumnOps(dataType)
		}

		p.retreat(index)
	}

	if fallbackToIdentifier {
		return p.parseIdVar(true, nil)
	}

	return p.parseColumn()
}

// _parse_type_size (parser.py L6266).
func (p *Parser) parseTypeSize() *Expr {
	this := p.parseType(true, false)
	if this == nil {
		return nil
	}

	if this.IsA(KColumn) && this.TableName() == "" {
		this = VarExpr(pyUpper(this.Name()))
	}

	return p.expression(New(KDataTypeParam, "this", this, "expression", p.parseVar(true, nil, false)))
}

// _parse_user_defined_type (parser.py L6278).
func (p *Parser) baseParseUserDefinedType(identifier *Expr) *Expr {
	typeName := identifier.Name()

	for p.match(TK_DOT) {
		part := "None"
		if p.advanceAny(false) != nil {
			part = p.prev.Text
		}
		typeName = typeName + "." + part
	}

	return chunkDDataTypeFromStr(typeName, p.d, true)
}

// _parse_types (parser.py L6286).
func (p *Parser) baseParseTypes(checkFunc bool, schema bool, allowIdentifiers bool, withCollation bool) *Expr {
	index := p.index
	var this *Expr

	// TK_SENTINEL stands for Python's `type_token = None` (it is never a member of any type set).
	var typeToken TokenType
	if p.matchSet(p.s.TYPE_TOKENS) {
		typeToken = p.prev.Type
	} else {
		typeToken = TK_SENTINEL
		var identifier *Expr
		if allowIdentifiers {
			identifier = p.parseIdVar(false, tsPtr(newTokenSet(TK_VAR)))
		}
		if identifier.IsA(KIdentifier) {
			tokens, err := p.d.Tokenize(identifier.Name())
			if err != nil {
				if _, ok := err.(*TokenError); !ok {
					panic(err)
				}
				tokens = nil
			}

			isTypeToken := false
			if len(tokens) > 0 {
				typeToken = tokens[0].Type
				isTypeToken = p.s.TYPE_TOKENS.Has(typeToken)
			}
			if isTypeToken {
				if len(tokens) > 1 {
					return chunkDDataTypeFromStr(identifier.Name(), p.d, false)
				}
			} else if p.d.S.SUPPORTS_USER_DEFINED_TYPES {
				this = p.parseUserDefinedType(identifier)
			} else {
				p.retreat(p.index - 1)
				return nil
			}
		} else {
			return nil
		}
	}

	if typeToken == TK_PSEUDO_TYPE {
		return p.expression(New(KPseudoType, "this", upperText(p.prev)))
	}

	if typeToken == TK_OBJECT_IDENTIFIER {
		return p.expression(New(KObjectIdentifier, "this", upperText(p.prev)))
	}

	// https://materialize.com/docs/sql/types/map/
	if typeToken == TK_MAP && p.match(TK_L_BRACKET) {
		keyType := p.parseTypes(checkFunc, schema, allowIdentifiers, false)
		if !p.match(TK_FARROW) {
			p.retreat(index)
			return nil
		}

		valueType := p.parseTypes(checkFunc, schema, allowIdentifiers, false)
		if !p.match(TK_R_BRACKET) {
			p.retreat(index)
			return nil
		}

		return New(
			KDataType,
			"this", DT_MAP,
			"expressions", []*Expr{keyType, valueType},
			"nested", true,
		)
	}

	nested := p.s.NESTED_TYPE_TOKENS.Has(typeToken)
	isStruct := p.s.STRUCT_TYPE_TOKENS.Has(typeToken)
	isAggregate := p.s.AGGREGATE_TYPE_TOKENS.Has(typeToken)
	// expressions is None in Python until assigned (exprsNone tracks that).
	var expressions []*Expr
	exprsNone := true
	maybeFunc := false

	parseNestedTypes := func() *Expr {
		return p.parseTypes(checkFunc, schema, allowIdentifiers, false)
	}

	if p.match(TK_L_PAREN) {
		exprsNone = false
		if isStruct {
			expressions = p.parseCSV(func() *Expr { return p.parseStructTypes(true) }, TK_COMMA)
		} else if nested {
			expressions = p.parseCSV(parseNestedTypes, TK_COMMA)
			if typeToken == TK_NULLABLE && len(expressions) == 1 {
				this = expressions[0]
				this.Set("nullable", true)
				p.matchRParen(nil)
				return this
			}
		} else if p.s.ENUM_TYPE_TOKENS.Has(typeToken) {
			expressions = p.parseCSV(p.parseEquality, TK_COMMA)
		} else if typeToken == TK_JSON {
			// ClickHouse JSON type supports arguments: JSON(col Type, SKIP col, param=value)
			// https://clickhouse.com/docs/sql-reference/data-types/newjson
			expressions = p.parseCSV(p.parseJsonTypeArg, TK_COMMA)
		} else if isAggregate {
			funcOrIdent := p.parseFunction(nil, true, true, false)
			if funcOrIdent == nil {
				funcOrIdent = p.parseIdVar(false, tsPtr(newTokenSet(TK_VAR, TK_ANY)))
			}
			if funcOrIdent == nil {
				return nil
			}
			expressions = []*Expr{funcOrIdent}
			if p.match(TK_COMMA) {
				expressions = append(expressions, p.parseCSV(parseNestedTypes, TK_COMMA)...)
			}
		} else {
			expressions = p.parseCSV(p.parseTypeSize, TK_COMMA)

			// https://docs.snowflake.com/en/sql-reference/data-types-vector
			if typeToken == TK_VECTOR && len(expressions) == 2 {
				expressions = p.parseVectorExpressions(expressions)
			}
		}

		if !p.match(TK_R_PAREN) {
			p.retreat(index)
			return nil
		}

		maybeFunc = true
	}

	// values is None in Python until assigned (valuesNone tracks that).
	var values []*Expr
	valuesNone := true

	if nested && p.match(TK_LT) {
		exprsNone = false
		if isStruct {
			expressions = p.parseCSV(func() *Expr { return p.parseStructTypes(true) }, TK_COMMA)
		} else {
			expressions = p.parseCSV(func() *Expr {
				return p.parseTypes(checkFunc, schema, allowIdentifiers, true)
			}, TK_COMMA)
		}

		if !p.match(TK_GT) {
			p.raiseError("Expecting >", nil)
		}

		if p.matchAny(TK_L_BRACKET, TK_L_PAREN) {
			values = p.parseCSV(p.parseDisjunction, TK_COMMA)
			valuesNone = false
			if len(values) == 0 && isStruct {
				values = nil
				valuesNone = true
				p.retreat(p.index - 1)
			} else {
				p.matchAny(TK_R_BRACKET, TK_R_PAREN)
			}
		}
	}

	exprsArg := func() any {
		if exprsNone {
			return nil
		}
		return expressions
	}

	if p.s.TIMESTAMPS.Has(typeToken) {
		if p.matchTextSeq("WITH", "TIME", "ZONE") {
			maybeFunc = false
			tzType := DT_TIMESTAMPTZ
			if p.s.TIMES.Has(typeToken) {
				tzType = DT_TIMETZ
			}
			this = New(KDataType, "this", tzType, "expressions", exprsArg())
		} else if p.matchTextSeq("WITH", "LOCAL", "TIME", "ZONE") {
			maybeFunc = false
			this = New(KDataType, "this", DT_TIMESTAMPLTZ, "expressions", exprsArg())
		} else if p.matchTextSeq("WITHOUT", "TIME", "ZONE") {
			maybeFunc = false
		}
	} else if typeToken == TK_INTERVAL {
		if p.d.S.VALID_INTERVAL_UNITS.Has(upperText(p.curr)) {
			unit := p.parseVar(false, nil, true)
			if p.matchTextSeq("TO") {
				unit = New(KIntervalSpan, "this", unit, "expression", p.parseVar(false, nil, true))
			}

			this = p.expression(New(KDataType, "this", p.expression(New(KInterval, "unit", unit))))
		} else {
			this = p.expression(New(KDataType, "this", DT_INTERVAL))
		}
	} else if typeToken == TK_VOID {
		this = New(KDataType, "this", DT_NULL)
	}

	if maybeFunc && checkFunc {
		index2 := p.index
		peek := p.parseString()

		if peek == nil {
			p.retreat(index)
			return nil
		}

		p.retreat(index2)
	}

	if this == nil {
		if p.matchTextSeq("UNSIGNED") {
			unsignedTypeToken, ok := p.s.SIGNED_TO_UNSIGNED_TYPE_TOKEN[typeToken]
			if !ok {
				p.raiseError("Cannot convert "+typeToken.Name()+" to unsigned.", nil)
			} else {
				typeToken = unsignedTypeToken
			}
		}

		// NULLABLE without parentheses can be a column (Presto/Trino)
		if typeToken == TK_NULLABLE && len(expressions) == 0 {
			p.retreat(index)
			return nil
		}

		this = New(
			KDataType,
			"this", chunkDDTypeOfToken(typeToken),
			"expressions", exprsArg(),
			"nested", nested,
		)

		// Empty arrays/structs are allowed
		if !valuesNone {
			cls := KArray
			if isStruct {
				cls = KStruct
			}
			this = chunkDCast(New(cls, "expressions", values), this)
		}
	} else if len(expressions) > 0 {
		this.Set("expressions", expressions)
	}

	// https://materialize.com/docs/sql/types/list/#type-name
	for p.match(TK_LIST) {
		this = New(KDataType, "this", DT_LIST, "expressions", []*Expr{this}, "nested", true)
	}

	index = p.index

	// Postgres supports the INT ARRAY[3] syntax as a synonym for INT[3]
	matchedArray := p.match(TK_ARRAY)

	for p.curr.ok() {
		datatypeToken := p.prev.Type
		matchedLBracket := p.match(TK_L_BRACKET)

		if (!matchedLBracket && !matchedArray) || (datatypeToken == TK_ARRAY && p.match(TK_R_BRACKET)) {
			// Postgres allows casting empty arrays such as ARRAY[]::INT[],
			// not to be confused with the fixed size array parsing
			break
		}

		matchedArray = false
		values = p.parseCSV(p.parseDisjunction, TK_COMMA)
		var valuesArg any
		if len(values) > 0 {
			valuesArg = values
		}
		if len(values) > 0 &&
			!schema &&
			(!p.d.S.SUPPORTS_FIXED_SIZE_ARRAYS ||
				datatypeToken == TK_ARRAY ||
				!p.matchNoAdvance(TK_R_BRACKET)) {
			// Retreating here means that we should not parse the following values as part of the data type, e.g. in DuckDB
			// ARRAY[1] should retreat and instead be parsed into exp.Array in contrast to INT[x][y] which denotes a fixed-size array data type
			p.retreat(index)
			break
		}

		this = New(
			KDataType,
			"this", DT_ARRAY,
			"expressions", []*Expr{this},
			"values", valuesArg,
			"nested", true,
		)
		p.match(TK_R_BRACKET)
	}

	if len(p.s.TYPE_CONVERTERS) > 0 {
		if dt, ok := this.Arg("this").(DType); ok {
			if converter, ok := p.s.TYPE_CONVERTERS[dt]; ok && converter != nil {
				this = converter(this)
			}
		}
	}

	if withCollation && this.IsA(KDataType) && p.match(TK_COLLATE) {
		collate := p.parseIdentifier()
		if collate == nil {
			collate = p.parseColumn()
		}
		this.Set("collate", collate)
	}

	return this
}

// _parse_json_type_arg (parser.py L6541)
// Parse a single argument to ClickHouse's JSON type.
func (p *Parser) parseJsonTypeArg() *Expr {
	// SKIP col or SKIP REGEXP 'pattern'
	if p.matchTextSeq("SKIP") {
		isRegexp := p.match(TK_RLIKE)
		arg := p.parseColumn()
		if arg.IsA(KColumn) {
			arg = arg.ToDot(true)
		}
		return p.expression(New(KSkipJSONColumn, "regexp", isRegexp, "expression", arg))
	}

	paramOrCol := p.parseColumn()
	if !paramOrCol.IsA(KColumn) {
		return nil
	}

	// Parameter: name=value (e.g., max_dynamic_paths=2)
	if len(paramOrCol.Parts()) == 1 && p.match(TK_EQ) {
		param := paramOrCol.Name()
		value := p.parsePrimary()
		return p.expression(New(KEQ, "this", VarExpr(param), "expression", value))
	}

	// Column type hint: col_name Type
	col := paramOrCol.ToDot(true)
	kind := p.parseTypes(false, false, false, false)
	return p.expression(New(KColumnDef, "this", col, "kind", kind))
}

// _parse_vector_expressions (parser.py L6567).
func (p *Parser) parseVectorExpressions(expressions []*Expr) []*Expr {
	out := []*Expr{chunkDDataTypeFromStr(expressions[0].Name(), p.d, false)}
	return append(out, expressions[1:]...)
}

// _parse_struct_types (parser.py L6570).
func (p *Parser) baseParseStructTypes(typeRequired bool) *Expr {
	index := p.index

	var this *Expr
	if p.curr.ok() &&
		p.next.ok() &&
		p.s.TYPE_TOKENS.Has(p.curr.Type) &&
		p.s.TYPE_TOKENS.Has(p.next.Type) {
		// Takes care of special cases like `STRUCT<list ARRAY<...>>` where the identifier is also a
		// type token. Without this, the list will be parsed as a type and we'll eventually crash
		this = p.parseIdVar(true, nil)
	} else {
		this = p.parseType(false, true)
		if this == nil {
			this = p.parseIdVar(true, nil)
		}
	}

	p.match(TK_COLON)

	if typeRequired &&
		!this.IsA(KDataType) &&
		!p.matchSetNoAdvance(p.s.TYPE_TOKENS) {
		p.retreat(index)
		return p.parseTypes(false, false, true, false)
	}

	return p.parseColumnDef(this, true)
}

// _parse_at_time_zone (parser.py L6600).
func (p *Parser) parseAtTimeZone(this *Expr) *Expr {
	if !p.matchTextSeq("AT", "TIME", "ZONE") {
		return this
	}
	return p.parseAtTimeZone(p.expression(New(KAtTimeZone, "this", this, "zone", p.parseUnary())))
}

// ---------------------------------------------------------------------------
// Helpers (ports of expression builders used by this chunk).

// chunkDDTypeByName maps DType member names to values (exp.DType[name]).
var chunkDDTypeByName = func() map[string]DType {
	m := make(map[string]DType, numDTypes)
	for i := 1; i < numDTypes; i++ {
		if dtypeNames[i] != "" {
			m[dtypeNames[i]] = DType(i)
		}
	}
	return m
}()

// chunkDDTypeOfToken mirrors exp.DType[type_token.name] (raises KeyError if missing).
func chunkDDTypeOfToken(tt TokenType) DType {
	name := tt.Name()
	if tt == TK_SENTINEL {
		// Python would fail the `assert type_token is not None` here.
		panic(&ValueError{Msg: "AssertionError"})
	}
	dt, ok := chunkDDTypeByName[name]
	if !ok {
		panic(&ValueError{Msg: "'" + name + "'"})
	}
	return dt
}

// chunkDIsType mirrors DataType.is_type(dtype) for a single DType argument.
func chunkDIsType(dataType *Expr, dtype DType) bool {
	other := NewDataType(dtype)
	if dataType.DTypeOf() == DT_USERDEFINED {
		return dataType.Equal(other)
	}
	if d, ok := dataType.Arg("this").(DType); ok {
		return d == dtype
	}
	return false
}

// chunkDDataTypeFromStr mirrors exp.DataType.from_str(dtype, dialect=d, udt=udt).
func chunkDDataTypeFromStr(dtype string, d *Dialect, udt bool) *Expr {
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

// chunkDCast mirrors exp.cast(expression, to, copy=False) where `expression` is not a Cast
// and `to` is a DataType instance.
func chunkDCast(expression *Expr, to *Expr) *Expr {
	if expression.IsA(KCast) {
		// sqlglot avoids re-casting here; this path is never taken by the parser call sites.
		panic(&ValueError{Msg: "chunkDCast: re-casting a Cast is not supported"})
	}
	cast := New(KCast, "this", expression, "to", to)
	cast.SetType(to)
	return cast
}

// chunkDSubquery mirrors Query.subquery(copy=False) with no alias.
func chunkDSubquery(query *Expr) *Expr {
	return New(KSubquery, "this", query, "alias", nil)
}

// chunkDPyNum is a Python int or decimal.Decimal value produced by Literal.to_py().
type chunkDPyNum struct {
	isInt bool
	i     *big.Int
	// decimal: sign (true = negative), coefficient digits (no leading zeros, "0" for zero), exponent
	neg   bool
	coeff string
	exp   int
}

// chunkDToPyNumber mirrors Literal.to_py() / Neg.to_py() for numeric literals.
func chunkDToPyNumber(e *Expr) chunkDPyNum {
	if e.IsA(KNeg) {
		v := chunkDToPyNumber(e.This())
		if v.isInt {
			return chunkDPyNum{isInt: true, i: new(big.Int).Neg(v.i)}
		}
		// Decimal * -1: flip sign, then round to the default context precision (28 digits).
		v.neg = !v.neg
		v.coeff, v.exp = chunkDDecimalRound(v.coeff, v.exp, 28)
		return v
	}
	s := e.ThisS()
	if isPyInt(s) {
		i, ok := new(big.Int).SetString(strings.ReplaceAll(pyStrip(s), "_", ""), 10)
		if ok {
			return chunkDPyNum{isInt: true, i: i}
		}
	}
	v, ok := chunkDParseDecimal(s)
	if !ok {
		panic(&ValueError{Msg: "[<class 'decimal.ConversionSyntax'>]"})
	}
	return v
}

// chunkDParseDecimal mirrors decimal.Decimal(str) for finite values.
func chunkDParseDecimal(s string) (chunkDPyNum, bool) {
	s = strings.ReplaceAll(pyStrip(s), "_", "")
	v := chunkDPyNum{}
	if s != "" && (s[0] == '+' || s[0] == '-') {
		v.neg = s[0] == '-'
		s = s[1:]
	}
	mant, expPart := s, ""
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		mant, expPart = s[:i], s[i+1:]
	}
	intPart, fracPart := mant, ""
	if i := strings.IndexByte(mant, '.'); i >= 0 {
		intPart, fracPart = mant[:i], mant[i+1:]
	}
	if intPart == "" && fracPart == "" {
		return v, false
	}
	for _, r := range intPart + fracPart {
		if r < '0' || r > '9' {
			return v, false
		}
	}
	exp := 0
	if expPart != "" || strings.ContainsAny(s, "eE") {
		n, err := strconv.Atoi(expPart)
		if err != nil {
			return v, false
		}
		exp = n
	}
	digits := strings.TrimLeft(intPart+fracPart, "0")
	if digits == "" {
		digits = "0"
	}
	v.coeff = digits
	v.exp = exp - len(fracPart)
	return v, true
}

// chunkDDecimalRound mirrors Decimal._fix rounding (ROUND_HALF_EVEN) to `prec` digits.
func chunkDDecimalRound(coeff string, exp int, prec int) (string, int) {
	if len(coeff) <= prec {
		return coeff, exp
	}
	dropped := coeff[prec:]
	kept := coeff[:prec]
	exp += len(coeff) - prec
	roundUp := false
	if dropped[0] > '5' {
		roundUp = true
	} else if dropped[0] == '5' {
		if strings.Trim(dropped[1:], "0") != "" {
			roundUp = true
		} else {
			roundUp = (kept[len(kept)-1]-'0')%2 == 1
		}
	}
	if roundUp {
		n, _ := new(big.Int).SetString(kept, 10)
		n.Add(n, big.NewInt(1))
		kept = n.String()
		if len(kept) > prec {
			kept = kept[:len(kept)-1]
			exp++
		}
	}
	return kept, exp
}

// String mirrors str() of the Python int / Decimal.
func (v chunkDPyNum) String() string {
	if v.isInt {
		return v.i.String()
	}
	sign := ""
	if v.neg {
		sign = "-"
	}
	leftdigits := v.exp + len(v.coeff)
	var dotplace int
	if v.exp <= 0 && leftdigits > -6 {
		dotplace = leftdigits
	} else {
		dotplace = 1
	}
	var intpart, fracpart string
	if dotplace <= 0 {
		intpart = "0"
		fracpart = "." + strings.Repeat("0", -dotplace) + v.coeff
	} else if dotplace >= len(v.coeff) {
		intpart = v.coeff + strings.Repeat("0", dotplace-len(v.coeff))
		fracpart = ""
	} else {
		intpart = v.coeff[:dotplace]
		fracpart = "." + v.coeff[dotplace:]
	}
	exp := ""
	if leftdigits != dotplace {
		e := leftdigits - dotplace
		if e >= 0 {
			exp = "E+" + strconv.Itoa(e)
		} else {
			exp = "E" + strconv.Itoa(e)
		}
	}
	return sign + intpart + fracpart + exp
}

// chunkDPyNumberStr mirrors str(expression.to_py()) for a numeric literal.
func chunkDPyNumberStr(e *Expr) string { return chunkDToPyNumber(e).String() }

// chunkDLiteralToPy mirrors expression.to_py() for a numeric literal, as an arg value:
// an int when it fits, otherwise the str() of the Python value.
func chunkDLiteralToPy(e *Expr) any {
	v := chunkDToPyNumber(e)
	if v.isInt && v.i.IsInt64() {
		i64 := v.i.Int64()
		if int64(int(i64)) == i64 {
			return int(i64)
		}
	}
	return v.String()
}
