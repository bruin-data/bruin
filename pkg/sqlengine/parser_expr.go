package sqlengine

import (
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

// chunkDPySpace lists the Unicode whitespace characters for use inside a character class
// (Go's \s matches ASCII whitespace only).
const chunkDPySpace = `\t\n\x0b\x0c\r\x1c-\x1f \x{85}\x{a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}`

// chunkDIntervalStringRE matches an interval string such as '5 day': a number and a unit, with
// optional surrounding whitespace.
var chunkDIntervalStringRE = regexp.MustCompile(
	`[` + chunkDPySpace + `]*(-?[0-9]+(?:\.[0-9]+)?)[` + chunkDPySpace + `]*([a-zA-Z]+)[` + chunkDPySpace + `]*`,
)

// chunkDIntervalDayTimeRE matches, from the start of the string, a day-time interval without
// units such as '1 2:03:04.5'.
var chunkDIntervalDayTimeRE = func() *regexp.Regexp {
	s := `[` + chunkDPySpace + `]`
	d := `\p{Nd}`
	return regexp.MustCompile(
		`^` + s + `*-?` + s + `*` + d + `+(?:\.` + d + `+)?` + s + `+(?:-?(?:` + d + `+:)?` + d + `+:` + d +
			`+(?:\.` + d + `+)?|-?(?:` + d + `+:){1,2}|:)` + s + `*`,
	)
}()

// chunkDTimeZoneRE detects a time zone in a timestamp literal: a colon followed later by a letter,
// + or -, anywhere in the string.
var chunkDTimeZoneRE = regexp.MustCompile(`:.*?[a-zA-Z\+\-]`)

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

func (p *Parser) baseParseExpression() *Expr {
	return p.parseAlias(p.parseAssignment(), false)
}

// baseParseAssignment parses a disjunction followed by right-associative ASSIGNMENT operators
// (such as :=). A single-part column on the left is replaced by its identifier.
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

func (p *Parser) parseDisjunction() *Expr {
	this := p.parseConjunction()
	for chunkDMatchKey(p, p.s.DISJUNCTION) {
		comments := p.prevComments
		kind := p.s.DISJUNCTION[p.prev.Type]
		this = p.expressionC(New(kind, "this", this, "expression", p.parseConjunction()), comments)
	}
	return this
}

func (p *Parser) parseConjunction() *Expr {
	this := p.parseEquality()
	for chunkDMatchKey(p, p.s.CONJUNCTION) {
		comments := p.prevComments
		kind := p.s.CONJUNCTION[p.prev.Type]
		this = p.expressionC(New(kind, "this", this, "expression", p.parseEquality()), comments)
	}
	return this
}

// buildPredicate builds a comparison or IS predicate, parenthesizing operands whose grouping the
// operator's precedence would otherwise lose: a comparison operand of an IS predicate, or an IS
// predicate operand of a comparison.
func (p *Parser) buildPredicate(kind Kind, this, expression *Expr, comments []string) *Expr {
	if kind == KIs || kind == KNullSafeEQ || kind == KNullSafeNEQ {
		if this.IsA(KConnector, KNot, KNullSafeNEQ) ||
			(this != nil && (hasKindValue(p.s.EQUALITY, this.kind) || hasKindValue(p.s.COMPARISON, this.kind))) ||
			(kind != KIs && this.IsA(KIs)) {
			this = New(KParen, "this", this)
		}
		if expression.IsA(KPredicate, KConnector, KNot, KEscape) {
			expression = New(KParen, "this", expression)
		}
	} else {
		if isIsPredicate(unwrapNot(this)) {
			this = New(KParen, "this", this)
		}
		if isIsPredicate(unwrapNot(expression)) {
			expression = New(KParen, "this", expression)
		}
	}
	return p.expressionC(New(kind, "this", this, "expression", expression), comments)
}

func hasKindValue(m map[TokenType]Kind, k Kind) bool {
	for _, v := range m {
		if v == k {
			return true
		}
	}
	return false
}

// isIsPredicate reports whether e is an IS or IS [NOT] DISTINCT FROM predicate.
func isIsPredicate(e *Expr) bool {
	return e.IsA(KIs, KNullSafeEQ, KNullSafeNEQ)
}

func unwrapNot(e *Expr) *Expr {
	if e.IsA(KNot) {
		return e.This()
	}
	return e
}

// parseEquality parses left-associative EQUALITY operators (=, <>, ...) and, where the dialect
// binds IS predicates below the comparisons, those predicates.
func (p *Parser) parseEquality() *Expr {
	return p.parseEqualityChain(true)
}

// parseEqualityChain is parseEquality; parseIs=false leaves IS predicates that bind below the
// comparisons unparsed.
func (p *Parser) parseEqualityChain(parseIs bool) *Expr {
	this := p.parseComparison(nil)
	for {
		if chunkDMatchKey(p, p.s.EQUALITY) {
			comments := p.prevComments
			kind := p.s.EQUALITY[p.prev.Type]
			this = p.buildPredicate(kind, this, p.parseComparison(nil), comments)
		} else if parseIs && p.isPredicateAhead() {
			index := p.index
			// A postfix predicate can itself be an operand, e.g. x IS NULL = FALSE.
			this = p.parseComparison(p.parseRange(this, true))
			if p.index == index {
				break
			}
		} else {
			break
		}
	}
	return this
}

// parseComparison parses left-associative COMPARISON operators (<, >=, ...), starting from this
// when it is given.
func (p *Parser) parseComparison(this *Expr) *Expr {
	if this == nil {
		this = p.parseRange(nil, false)
	}
	for chunkDMatchKey(p, p.s.COMPARISON) {
		comments := p.prevComments
		kind := p.s.COMPARISON[p.prev.Type]
		this = p.buildPredicate(kind, this, p.parseRange(nil, false), comments)
	}
	return this
}

// IsPredicatePrecedence is where a dialect binds IS predicates relative to the comparisons.
type IsPredicatePrecedence uint8

const (
	// IsPredicateHigh binds IS above all comparisons.
	IsPredicateHigh IsPredicatePrecedence = iota
	// IsPredicateComparison binds IS alongside the ordering comparisons (<, >=, ...).
	IsPredicateComparison
	// IsPredicateEquality binds IS alongside equality (=, <>, ...).
	IsPredicateEquality
	// IsPredicateLow binds IS below all comparisons.
	IsPredicateLow
)

// isPredicateAhead reports whether an IS predicate (IS, ISNULL, NOTNULL or NOT NULL) follows and
// the dialect binds it at or below the comparisons.
func (p *Parser) isPredicateAhead() bool {
	if p.s.IS_PREDICATE_PRECEDENCE == IsPredicateHigh || !p.s.IS_PREDICATE_TOKENS.Has(p.curr.Type) {
		return false
	}
	return p.curr.Type != TK_NOT || p.next.Type == TK_NULL
}

// baseParseRange parses the predicates that follow a bitwise expression: RANGE_PARSERS operators
// (BETWEEN, IN, LIKE, IS, ...), ISNULL and NOTNULL, each optionally negated with NOT. Unless
// parseIs is set it stops at an IS predicate that binds below the comparisons.
func (p *Parser) baseParseRange(this *Expr, parseIs bool) *Expr {
	if this == nil {
		this = p.parseBitwise()
	}

	for {
		if !parseIs && p.isPredicateAhead() {
			break
		}
		negate := p.match(TK_NOT)
		if chunkDMatchKey(p, p.s.RANGE_PARSERS) {
			expression := p.s.RANGE_PARSERS[p.prev.Type](p, this)
			if expression == nil {
				return this
			}

			this = expression
		} else if p.match(TK_ISNULL) || (negate && p.match(TK_NULL)) {
			this = p.buildPredicate(KIs, this, Null(), nil)
		} else if p.match(TK_NOTNULL) {
			// Postgres supports ISNULL and NOTNULL for conditions.
			// https://blog.andreiavram.ro/postgresql-null-composite-type/
			this = p.buildPredicate(KIs, this, Null(), nil)
			if p.d.S.NORMALIZE_NOT_NULL {
				this = p.expression(New(KNot, "this", this))
			} else {
				this.Set("negate", true)
			}
		} else {
			if negate {
				p.retreat(p.index - 1)
			}
			break
		}

		if negate {
			this = p.negateRange(this)
		}
		if negate || (this.IsA(KNot) && this.This().IsA(KIs)) {
			_, isRange := p.s.RANGE_PARSERS[p.curr.Type]
			if p.curr.Type == TK_NOT || isRange {
				this = p.expression(New(KParen, "this", this))
			}
		}
	}

	return this
}

// baseNegateRange negates a range predicate: LIKE and ILIKE (also under ESCAPE) get negate set,
// anything else is wrapped in Not.
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

// parseIs parses what follows IS: [NOT] DISTINCT FROM x, [NOT] JSON [kind] [WITH|WITHOUT]
// [UNIQUE [KEYS]], or [NOT] NULL or another expression. Without any of these it rewinds to
// before IS and returns nil.
func (p *Parser) parseIs(this *Expr) *Expr {
	index := p.index - 1
	negate := p.match(TK_NOT)

	if p.matchTextSeq("DISTINCT", "FROM") {
		klass := KNullSafeNEQ
		if negate {
			klass = KNullSafeEQ
		}
		return p.buildPredicate(klass, this, p.parseIsRhs(true), nil)
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
		expression = p.parseIsRhs(false)
		if expression == nil {
			p.retreat(index)
			return nil
		}
	}

	this = p.buildPredicate(KIs, this, expression, nil)
	if negate && expression.Is(KNull) && !p.d.S.NORMALIZE_NOT_NULL {
		this.Set("negate", true)
	} else if negate {
		this = p.expression(New(KNot, "this", this))
	}
	return p.parseColumnOps(this)
}

// baseParseIsRhs parses the right operand of IS, or of IS [NOT] DISTINCT FROM when distinct is
// set, which spans the operators that bind above IS in the dialect.
func (p *Parser) baseParseIsRhs(distinct bool) *Expr {
	if distinct {
		switch p.s.IS_PREDICATE_PRECEDENCE {
		case IsPredicateLow:
			return p.parseEqualityChain(false)
		case IsPredicateEquality:
			return p.parseComparison(nil)
		case IsPredicateComparison:
			return p.parseRange(nil, false)
		}
		return p.parseBitwise()
	}
	if e := p.parseNull(); e != nil {
		return e
	}
	return p.parseBitwise()
}

// parseIn parses what follows IN: an UNNEST, a parenthesized or bracketed list or subquery, or a
// single field.
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

func (p *Parser) parseBetween(this *Expr) *Expr {
	// Preserve grouping when BETWEEN is rewritten into comparisons.
	if isIsPredicate(this) {
		this = New(KParen, "this", this)
	}

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

// parseIntervalSpan parses the unit after an interval value, including spans such as
// DAY TO SECOND, and normalizes the value to the canonical form INTERVAL '<n>' <unit>.
func (p *Parser) parseIntervalSpan(this *Expr) *Expr {
	// handle day-time format interval span with omitted units:
	//   INTERVAL '<number days> hh[:][mm[:ss[.ff]]]' <maybe `unit TO unit`>
	intervalSpanUnitsOmitted := false
	var fractionInString any
	if this != nil && this.IsString() && p.s.SUPPORTS_OMITTED_INTERVAL_SPAN_UNIT &&
		chunkDIntervalDayTimeRE.MatchString(this.Name()) {
		index := p.index

		firstUnit := p.parseVar(true, nil, true)
		var secondUnit *Expr
		if firstUnit != nil && p.matchTextSeq("TO") {
			secondUnit = p.parseVar(true, nil, true)
		}

		intervalSpanUnitsOmitted = !(firstUnit != nil && secondUnit != nil)

		p.retreat(index)
	}

	unitIndex := p.index
	var unit *Expr
	if !intervalSpanUnitsOmitted {
		// Only a token that can be a unit is parsed as one, so that a trailing operator or alias
		// isn't swallowed, e.g. INTERVAL '1 day' AND (x)
		isUnit := p.curr.ok() && (p.curr.Type == TK_VAR || p.d.S.VALID_INTERVAL_UNITS.Has(upperText(p.curr)))
		if isUnit {
			unit = p.parseFunction(nil, false, true, false)
			if unit == nil {
				unit = p.parseVar(true, nil, true)
			}
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
			p.retreat(unitIndex)
		}

		if len(parts) == 1 {
			this = LiteralString(parts[0][1])
			unit = p.expression(New(KVar, "this", pyUpper(parts[0][2])))
			// INTERVAL '1.5 DAY' differs from INTERVAL '1.5' DAY in e.g. DuckDB and Postgres
			if strings.Contains(parts[0][1], ".") {
				fractionInString = true
			}
		}
	}

	if p.s.INTERVAL_SPANS && p.matchTextSeq("TO") {
		expression := p.parseFunction(nil, false, true, false)
		if expression == nil {
			expression = p.parseVar(true, nil, true)
		}
		unit = p.expression(New(KIntervalSpan, "this", unit, "expression", expression))
	}

	return p.expression(New(KInterval, "this", this, "unit", unit, "fraction_in_string", fractionInString))
}

// parseInterval parses INTERVAL <value> [unit]; requireInterval makes the INTERVAL keyword
// mandatory. A sequence such as INTERVAL '1' DAY '2' HOUR becomes a sum of intervals.
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
	if p.s.SUPPORTS_MULTI_PART_INTERVAL_ADD {
		p.match(TK_PLUS)
	}

	// Convert INTERVAL 'val_1' unit_1 [+] ... [+] 'val_n' unit_n into a sum of intervals; the value
	// must be followed by a valid unit
	if p.matchAnyNoAdvance(TK_STRING, TK_NUMBER) &&
		!p.s.TEXT_MATCH_EXCLUDED_TOKENS.Has(p.next.Type) &&
		hasUpperText(p.d.S.VALID_INTERVAL_UNITS, p.next.Text) {
		return p.expression(New(KAdd, "this", interval, "expression", p.parseInterval(false)))
	}

	p.retreat(index)
	return interval
}

// parseBitwise parses left-associative BITWISE operators, || when it means string concatenation,
// ?? (Coalesce), << and >>, and the dialect's JSON_OPERATORS.
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
		} else if len(p.s.JSON_OPERATORS) > 0 && chunkDMatchKey(p, p.s.JSON_OPERATORS) {
			// JSON operators of the binary-operator tier (Postgres, DuckDB), below + and -
			op := p.s.JSON_OPERATORS[p.prev.Type]
			this = op(p, this, p.parseTerm())
		} else {
			break
		}
	}

	return this
}

func (p *Parser) parseTerm() *Expr {
	this := p.parseFactor()

	for chunkDMatchKey(p, p.s.TERM) {
		klass := p.s.TERM[p.prev.Type]
		comments := p.prevComments
		expression := p.parseFactor()

		this = p.expressionC(New(klass, "this", this, "expression", expression), comments)

		if this.IsA(KCollate) {
			normalizeCollate(this)
		}
	}

	return this
}

// normalizeCollate replaces a single-part column collation with its identifier when quoted, or a
// Var otherwise; collations such as pg_catalog."default" (Postgres) stay columns.
func normalizeCollate(collate *Expr) {
	expr := collate.Expression()
	if expr.IsA(KColumn) && len(expr.Parts()) == 1 {
		ident := expr.This()
		if ident.IsA(KIdentifier) {
			if ident.ArgB("quoted") {
				collate.Set("expression", ident)
			} else {
				collate.Set("expression", VarExpr(ident.Name()))
			}
		}
	}
}

// parseFactor parses left-associative FACTOR operators (*, /, DIV, ...) over parseFactorOperand
// operands, the first with an optional AT TIME ZONE. A word operator such as DIV without an
// operand after it is given back, since it may be an alias.
func (p *Parser) parseFactor() *Expr {
	parseMethod := p.parseFactorOperand
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

func (p *Parser) baseParseFactorOperand() *Expr {
	if len(p.s.EXPONENT) > 0 {
		return p.parseExponent()
	}
	return p.parseUnary()
}

// parseExponent parses left-associative EXPONENT operators over unary operands.
func (p *Parser) parseExponent() *Expr {
	this := p.parseUnary()
	for chunkDMatchKey(p, p.s.EXPONENT) {
		comments := p.prevComments
		kind := p.s.EXPONENT[p.prev.Type]
		this = p.expressionC(New(kind, "this", this, "expression", p.parseUnary()), comments)
	}
	return this
}

func (p *Parser) parseUnary() *Expr {
	if chunkDMatchKey(p, p.s.UNARY_PARSERS) {
		return p.s.UNARY_PARSERS[p.prev.Type](p)
	}
	return p.parseType(true, false)
}

// baseParseType parses an atom, an interval, a typed literal such as DATE '2020-01-01' (a cast)
// or a type constructor, and otherwise falls back to a column, or to an identifier when
// fallbackToIdentifier is set (atoms are then skipped).
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

	// parseTypes returns a Cast if we parsed BQ's inline constructor <type>(<values>) e.g.
	// STRUCT<a INT, b STRING>(1, 'foo'), which is canonicalized to CAST(<values> AS <type>)
	if dataType.IsA(KCast) {
		// This constructor can contain ops directly after it, for instance struct unnesting:
		// STRUCT<a INT, b STRING>(1, 'foo').* -->
		// CAST(STRUCT(1, 'foo') AS STRUCT<a INT, b STRING>).*
		return p.parseColumnOps(dataType)
	}

	if dataType != nil {
		index2 := p.index
		this := p.parsePrimary()

		if this.IsA(KLiteral) {
			literal := this.Name()
			this = p.parseColumnOps(this)

			if p.s.ZONE_AWARE_TIMESTAMP_CONSTRUCTOR && chunkDTimeZoneRE.MatchString(literal) {
				if chunkDIsType(dataType, DT_TIMESTAMP) {
					dataType = NewDataType(DT_TIMESTAMPTZ)
				} else if chunkDIsType(dataType, DT_TIME) {
					dataType = NewDataType(DT_TIMETZ)
				}
			}

			if parser, ok := p.s.TYPE_LITERAL_PARSERS[dataType.DTypeOf()]; ok && parser != nil {
				return parser(p, this, dataType)
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
		// converter in the TYPE_CONVERTERS mapping. For example, Snowflake converts DECIMAL to
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

// parseTypeSize parses one data type parameter, such as 10 in VARCHAR(10), with an optional
// trailing keyword (VARCHAR2(10 BYTE)).
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

// baseParseUserDefinedType parses a dotted user-defined type name that starts with identifier;
// a missing part after a dot is written as None.
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

// baseParseTypes parses a data type: parameters, nested and struct types (ARRAY<INT>,
// STRUCT<a INT>), inline constructors such as STRUCT<a INT>(1), time zone suffixes, UNSIGNED,
// array suffixes (INT[3], INT ARRAY) and COLLATE (with withCollation). With checkFunc, a type
// with parentheses counts only when a string literal follows, as in DECIMAL(10, 2) '1.5', so
// that function calls are not taken for types. It returns nil, with the position restored,
// when there is no type.
func (p *Parser) baseParseTypes(checkFunc bool, schema bool, allowIdentifiers bool, withCollation bool) *Expr {
	index := p.index
	var this *Expr

	// TK_SENTINEL means there is no type token (it is never a member of any type set).
	var typeToken TokenType
	if p.matchSet(p.s.TYPE_TOKENS) {
		typeToken = p.prev.Type
	} else {
		typeToken = TK_SENTINEL
		var identifier *Expr
		if allowIdentifiers {
			identifier = p.parseIdVar(false, tsPtr(newTokenSet(TK_VAR)))
		}
		if identifier.IsA(KIdentifier) && identifier.ArgB("quoted") && p.s.QUOTED_TYPES_TO_PRESERVE.Has(identifier.Name()) {
			// a quoted name that denotes another type than the built-in one of the same name,
			// e.g. Postgres' one-byte "char", stays a user-defined type
			this = DataTypeBuild(identifier, p.d, true, false)
		} else if identifier.IsA(KIdentifier) {
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
	// exprsNone tracks whether expressions is still unset; unset is stored as nil, not as [].
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

	// valuesNone tracks whether values is still unset (no constructor values were parsed).
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
			// a TIMESTAMP that the dialect tokenizes as TIMESTAMPTZ (e.g. Databricks)
			if typeToken == TK_TIMESTAMPTZ {
				typeToken = TK_TIMESTAMPNTZ
			}
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
			// Retreating here means that we should not parse the following values as part of the
			// data type, e.g. in DuckDB ARRAY[1] should retreat and instead be parsed into an
			// Array, in contrast to INT[x][y] which denotes a fixed-size array data type
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

// parseJsonTypeArg parses a single argument to ClickHouse's JSON type.
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

// parseVectorExpressions turns the first VECTOR parameter, the element type name, into a
// DataType.
func (p *Parser) parseVectorExpressions(expressions []*Expr) []*Expr {
	out := []*Expr{chunkDDataTypeFromStr(expressions[0].Name(), p.d, false)}
	return append(out, expressions[1:]...)
}

// baseParseStructTypes parses one struct field, name [:] type. With typeRequired, a field
// without a type is parsed again as a bare type.
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

func (p *Parser) parseAtTimeZone(this *Expr) *Expr {
	if !p.matchTextSeq("AT", "TIME", "ZONE") {
		return this
	}
	return p.parseAtTimeZone(p.expression(New(KAtTimeZone, "this", this, "zone", p.parseUnary())))
}

var chunkDDTypeByName = func() map[string]DType {
	m := make(map[string]DType, numDTypes)
	for i := 1; i < numDTypes; i++ {
		if dtypeNames[i] != "" {
			m[dtypeNames[i]] = DType(i)
		}
	}
	return m
}()

// chunkDDTypeOfToken returns the DType named like the token type tt. TK_SENTINEL and names
// without a DType panic.
func chunkDDTypeOfToken(tt TokenType) DType {
	name := tt.Name()
	if tt == TK_SENTINEL {
		panic(&ValueError{Msg: "AssertionError"})
	}
	dt, ok := chunkDDTypeByName[name]
	if !ok {
		panic(&ValueError{Msg: "'" + name + "'"})
	}
	return dt
}

// chunkDIsType reports whether dataType has the given type; user-defined types must match
// exactly.
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

// chunkDDataTypeFromStr parses a type string with d. With udt, a string that does not parse
// becomes a user-defined type instead of a parse error.
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

// chunkDCast casts expression, which must not be a Cast, to the DataType to, without copying.
func chunkDCast(expression *Expr, to *Expr) *Expr {
	if expression.IsA(KCast) {
		// re-casting a Cast is not supported; the parser call sites never get here.
		panic(&ValueError{Msg: "chunkDCast: re-casting a Cast is not supported"})
	}
	cast := New(KCast, "this", expression, "to", to)
	cast.SetType(to)
	return cast
}

// chunkDSubquery wraps query in a Subquery without an alias and without copying.
func chunkDSubquery(query *Expr) *Expr {
	return New(KSubquery, "this", query, "alias", nil)
}

// chunkDPyNum is the value of a numeric literal: an arbitrary-precision integer or a decimal.
type chunkDPyNum struct {
	isInt bool
	i     *big.Int
	// decimal: sign (true = negative), coefficient digits (no leading zeros, "0" for zero), exponent
	neg   bool
	coeff string
	exp   int
}

// chunkDToPyNumber returns the value of a numeric literal, or of a negated one. A number that does
// not parse panics; the message keeps its historical wording.
func chunkDToPyNumber(e *Expr) chunkDPyNum {
	if e.IsA(KNeg) {
		v := chunkDToPyNumber(e.This())
		if v.isInt {
			return chunkDPyNum{isInt: true, i: new(big.Int).Neg(v.i)}
		}
		// negating a decimal flips the sign, then rounds to 28 significant digits
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

// chunkDParseDecimal parses a finite decimal number: optional sign, digits with an optional point
// and an optional exponent. Surrounding whitespace and underscores are ignored.
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

// chunkDDecimalRound rounds a decimal coefficient to prec significant digits, half to even, and
// adjusts the exponent.
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

// String formats the number: integers in full, decimals in plain notation or, when the exponent
// is positive or the magnitude is below 1e-6, in scientific notation (1E+3, 1E-7).
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

func chunkDPyNumberStr(e *Expr) string { return chunkDToPyNumber(e).String() }

// chunkDLiteralToPy returns the value of a numeric literal as an arg value: an int when it is an
// integer that fits, otherwise the formatted number.
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
