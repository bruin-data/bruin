package sqlengine

import "strings"

// parseCSV parses a sep-separated list with parseMethod, dropping nil results. Comments on a
// separator are attached to the item before it.
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

func (p *Parser) parseWrappedCSV(parseMethod func() *Expr, sep TokenType, optional bool) []*Expr {
	return p.parseWrappedList(func() []*Expr { return p.parseCSV(parseMethod, sep) }, optional)
}

func (p *Parser) baseParseWrappedIdVars(optional bool) []*Expr {
	return p.parseWrappedCSV(func() *Expr { return p.parseIdVar(true, nil) }, TK_COMMA, optional)
}

func (p *Parser) parseExpressions() []*Expr {
	return p.parseCSV(func() *Expr { return p.parseExpression() }, TK_COMMA)
}

func (p *Parser) matchLParen(e *Expr) {
	if !p.matchExpr(TK_L_PAREN, e) {
		p.raiseError("Expecting (", nil)
	}
}

func (p *Parser) matchRParen(e *Expr) {
	if !p.matchExpr(TK_R_PAREN, e) {
		p.raiseError("Expecting )", nil)
	}
}

// identifierExpression builds an Identifier from tok (p.prev when tok is unset). quoted:
// TriNone leaves the quoted arg unset, TriFalse and TriTrue set it.
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

// findParser matches the upcoming tokens against tr, the keyword trie of parsers (keys may span
// several words), and returns the parser of the first complete key, leaving its tokens consumed.
// Without a match it restores the position and returns nil.
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

// advanceAny consumes and returns the current token unless it is reserved (ignoreReserved allows
// reserved tokens too); nil when nothing was consumed.
func (p *Parser) advanceAny(ignoreReserved bool) *Token {
	if p.curr.ok() && (ignoreReserved || !p.s.RESERVED_TOKENS.Has(p.curr.Type)) {
		p.advance(1)
		return p.prev
	}
	return nil
}

func exprOr(a *Expr, b func() *Expr) *Expr {
	if a != nil {
		return a
	}
	return b()
}

func tsPtr(s TokenSet) *TokenSet { return &s }

// windowSpec is a window frame bound returned by parseWindowSpec: UNBOUNDED, CURRENT ROW or an
// expression, with an optional side such as PRECEDING.
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
