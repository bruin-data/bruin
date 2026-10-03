package sqlengine

import "strings"

// Shared parser helpers used across all parser files (ports of sqlglot.parser.Parser helpers).

// parseCSV mirrors Parser._parse_csv.
func (p *Parser) parseCSV(parseMethod func() *Expr, sep TokenType) []*Expr {
	parseResult := parseMethod()
	items := []*Expr{}
	if parseResult != nil {
		items = append(items, parseResult)
	}
	for p.match(sep) {
		if parseResult != nil {
			p.addComments(parseResult)
		}
		parseResult = parseMethod()
		if parseResult != nil {
			items = append(items, parseResult)
		}
	}
	return items
}

// parseCSVAny is parseCSV for parse methods returning non-expression values.
func parseCSVAny[T any](p *Parser, parseMethod func() T, sep TokenType, isNil func(T) bool) []T {
	parseResult := parseMethod()
	items := []T{}
	if !isNil(parseResult) {
		items = append(items, parseResult)
	}
	for p.match(sep) {
		if e, ok := any(parseResult).(*Expr); ok && e != nil {
			p.addComments(e)
		}
		parseResult = parseMethod()
		if !isNil(parseResult) {
			items = append(items, parseResult)
		}
	}
	return items
}

// parseWrapped mirrors Parser._parse_wrapped for expression-returning methods.
func (p *Parser) parseWrapped(parseMethod func() *Expr, optional bool) *Expr {
	wrapped := p.match(TK_L_PAREN)
	if !wrapped && !optional {
		p.raiseError("Expecting (", nil)
	}
	result := parseMethod()
	if wrapped {
		p.matchRParen(nil)
	}
	return result
}

// parseWrappedList mirrors Parser._parse_wrapped for list-returning methods.
func (p *Parser) parseWrappedList(parseMethod func() []*Expr, optional bool) []*Expr {
	wrapped := p.match(TK_L_PAREN)
	if !wrapped && !optional {
		p.raiseError("Expecting (", nil)
	}
	result := parseMethod()
	if wrapped {
		p.matchRParen(nil)
	}
	return result
}

// parseWrappedAny mirrors Parser._parse_wrapped for arbitrary return types.
func parseWrappedAny[T any](p *Parser, parseMethod func() T, optional bool) T {
	wrapped := p.match(TK_L_PAREN)
	if !wrapped && !optional {
		p.raiseError("Expecting (", nil)
	}
	result := parseMethod()
	if wrapped {
		p.matchRParen(nil)
	}
	return result
}

// parseWrappedCSV mirrors Parser._parse_wrapped_csv.
func (p *Parser) parseWrappedCSV(parseMethod func() *Expr, sep TokenType, optional bool) []*Expr {
	return p.parseWrappedList(func() []*Expr { return p.parseCSV(parseMethod, sep) }, optional)
}

// baseParseWrappedIdVars mirrors Parser._parse_wrapped_id_vars.
func (p *Parser) baseParseWrappedIdVars(optional bool) []*Expr {
	return p.parseWrappedCSV(func() *Expr { return p.parseIdVar(true, nil) }, TK_COMMA, optional)
}

// parseExpressions mirrors Parser._parse_expressions.
func (p *Parser) parseExpressions() []*Expr {
	return p.parseCSV(func() *Expr { return p.parseExpression() }, TK_COMMA)
}

// matchLParen mirrors Parser._match_l_paren.
func (p *Parser) matchLParen(e *Expr) {
	if !p.matchExpr(TK_L_PAREN, e) {
		p.raiseError("Expecting (", nil)
	}
}

// matchRParen mirrors Parser._match_r_paren.
func (p *Parser) matchRParen(e *Expr) {
	if !p.matchExpr(TK_R_PAREN, e) {
		p.raiseError("Expecting )", nil)
	}
}

// identifierExpression mirrors Parser._identifier_expression. quoted: TriNone/TriFalse/TriTrue.
func (p *Parser) identifierExpression(tok *Token, quoted Tri) *Expr {
	if !tok.ok() {
		tok = p.prev
	}
	var q any
	switch quoted {
	case TriTrue:
		q = true
	case TriFalse:
		q = false
	}
	return p.expressionTok(New(KIdentifier, "this", tok.Text, "quoted", q), tok)
}

func triOf(b bool) Tri {
	if b {
		return TriTrue
	}
	return TriFalse
}

// findParser mirrors Parser._find_parser.
func (p *Parser) findParser(parsers map[string]parseFn, tr *trie) parseFn {
	if !p.curr.ok() {
		return nil
	}
	index := p.index
	var this []string
	for {
		curr := pyUpper(p.curr.Text)
		key := strings.Split(curr, " ")
		this = append(this, curr)
		p.advance(1)
		var result trieResult
		result, tr = inTrie(tr, key)
		if result == trieFailed {
			break
		}
		if result == trieExists {
			return parsers[strings.Join(this, " ")]
		}
	}
	p.retreat(index)
	return nil
}

// advanceAny mirrors Parser._advance_any.
func (p *Parser) advanceAny(ignoreReserved bool) *Token {
	if p.curr.ok() && (ignoreReserved || !p.s.RESERVED_TOKENS.Has(p.curr.Type)) {
		p.advance(1)
		return p.prev
	}
	return nil
}

// exprOr mirrors Python's `a or b` for expressions (b evaluated lazily).
func exprOr(a *Expr, b func() *Expr) *Expr {
	if a != nil {
		return a
	}
	return b()
}

// tsPtr returns a pointer to a token set (for optional TokenSet parameters).
func tsPtr(s TokenSet) *TokenSet { return &s }

// windowSpec mirrors the dict returned by Parser._parse_window_spec.
type windowSpec struct {
	value   any // string or *Expr (nil if absent)
	side    string
	hasSide bool
}

// noParenFunctionParser looks up NO_PAREN_FUNCTION_PARSERS including per-parser temporary
// entries (see baseParseConnectWithPrior).
func (p *Parser) noParenFunctionParser(name string) (parseFn, bool) {
	if f, ok := p.noParenOverlay[name]; ok {
		return f, true
	}
	f, ok := p.s.NO_PAREN_FUNCTION_PARSERS[name]
	return f, ok
}

func (p *Parser) setNoParenOverlay(name string, f parseFn) {
	if p.noParenOverlay == nil {
		p.noParenOverlay = map[string]parseFn{}
	}
	p.noParenOverlay[name] = f
}

func (p *Parser) popNoParenOverlay(name string) bool {
	if _, ok := p.noParenOverlay[name]; !ok {
		return false
	}
	delete(p.noParenOverlay, name)
	return true
}
