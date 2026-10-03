package sqlengine

import (
	"fmt"
	"runtime"
	"strings"
)

// Callable table types mirroring the dict[...]->callable tables of sqlglot.parser.Parser.
type (
	FuncBuilder        func(args []*Expr, d *Dialect) *Expr
	parseFn            func(p *Parser) *Expr
	parseAnyFn         func(p *Parser) any
	tokenParseFn       func(p *Parser, tok *Token) *Expr
	rangeParseFn       func(p *Parser, this *Expr) *Expr
	columnOperatorFn   func(p *Parser, this, path *Expr) *Expr
	lambdaParseFn      func(p *Parser, exprs []*Expr) *Expr
	queryModifierFn    func(p *Parser) (string, any)
	propertyParseFn    func(p *Parser, kw propKwargs) any
	typeLiteralParseFn func(p *Parser, this, dataType *Expr) *Expr
	typeConverterFn    func(dt *Expr) *Expr
	pipeTransformFn    func(p *Parser, query *Expr) *Expr
)

// propKwargs mirrors the **kwargs passed to PROPERTY_PARSERS entries.
type propKwargs struct {
	no, dual, before, default_, after, minimum, maximum bool
	local                                               string
	set                                                 bool
}

// ParserSettings is the fully-resolved parser configuration of a dialect.
type ParserSettings struct {
	*ParserData

	FUNCTIONS                     map[string]FuncBuilder
	LAMBDAS                       map[TokenType]lambdaParseFn
	COLUMN_OPERATORS              map[TokenType]columnOperatorFn
	EXPRESSION_PARSERS            map[Kind]parseFn
	STATEMENT_PARSERS             map[TokenType]parseFn
	UNARY_PARSERS                 map[TokenType]parseFn
	STRING_PARSERS                map[TokenType]tokenParseFn
	NUMERIC_PARSERS               map[TokenType]tokenParseFn
	PRIMARY_PARSERS               map[TokenType]tokenParseFn
	PLACEHOLDER_PARSERS           map[TokenType]parseFn
	RANGE_PARSERS                 map[TokenType]rangeParseFn
	PIPE_SYNTAX_TRANSFORM_PARSERS map[string]pipeTransformFn
	PROPERTY_PARSERS              map[string]propertyParseFn
	CONSTRAINT_PARSERS            map[string]parseFn
	ALTER_PARSERS                 map[string]parseAnyFn
	ALTER_ALTER_PARSERS           map[string]parseFn
	NO_PAREN_FUNCTION_PARSERS     map[string]parseFn
	FUNCTION_PARSERS              map[string]parseFn
	QUERY_MODIFIER_PARSERS        map[TokenType]queryModifierFn
	SET_PARSERS                   map[string]parseFn
	SHOW_PARSERS                  map[string]parseFn
	TYPE_LITERAL_PARSERS          map[DType]typeLiteralParseFn
	TYPE_CONVERTERS               map[DType]typeConverterFn
	ANALYZE_EXPRESSION_PARSERS    map[string]parseFn
	DESCRIBE_QUALIFIER_PARSERS    map[string]parseFn

	SHOW_TRIE *trie
	SET_TRIE  *trie

	h parserHooks
}

// Parser mirrors sqlglot.parser.Parser.
type Parser struct {
	d *Dialect
	s *ParserSettings

	errorLevel          ErrorLevel
	errorMessageContext int
	maxErrors           int
	maxNodes            int

	sql            string
	sqlRunes       []rune
	errors         []*ParseError
	tokens         []*Token
	tokensSize     int
	index          int
	curr           *Token
	next           *Token
	prev           *Token
	prevComments   []string
	pipeCteCounter int
	chunks         [][]*Token
	chunkIndex     int
	nodeCount      int

	// noParenOverlay holds temporary NO_PAREN_FUNCTION_PARSERS entries (sqlglot mutates the class table).
	noParenOverlay map[string]parseFn

	// depth counts nested entries into the recursive parse methods (see enter).
	depth int
	// maxIndex and stall implement the no-progress guard (see tick).
	maxIndex int
	stall    int
}

// maxParseStall bounds how many token-match attempts may happen without the parser ever reaching
// a new token position. A few SQLGlot parse loops spin forever on malformed input (e.g. a
// property parser that retreats inside a `while True` property loop); Python hangs there. The Go
// port raises an error instead. Legitimate parses (including backtracking) stay far below this.
const maxParseStall = 10_000_000

// tick is called on every match attempt.
func (p *Parser) tick() {
	if p.index > p.maxIndex {
		p.maxIndex = p.index
		p.stall = 0
		return
	}
	p.stall++
	if p.stall > maxParseStall {
		panic(&ValueError{Msg: "Parser made no progress (infinite loop on malformed input)"})
	}
}

// maxParseDepth bounds parser recursion. Python raises RecursionError (caught by callers like any
// other error) where a Go stack overflow would kill the process, so runaway recursion (e.g. a
// statement parser that re-enters itself without consuming tokens) becomes an error instead.
// Python's own limit is reached at ~100 nested parentheses; this allows far deeper legitimate SQL.
const maxParseDepth = 3000

// enter/leave bracket the recursive parse entry points.
func (p *Parser) enter() {
	p.depth++
	if p.depth > maxParseDepth {
		panic(&ValueError{Msg: "maximum recursion depth exceeded"})
	}
}

func (p *Parser) leave() { p.depth-- }

var sentinelNone = &Token{Type: TK_SENTINEL, Text: "SENTINEL", Line: 1, Col: 1, Comments: []string{}}

func (t *Token) ok() bool { return t != nil && t.Type != TK_SENTINEL }

// ParseOptions mirrors the parser keyword options.
type ParseOptions struct {
	ErrorLevel          *ErrorLevel
	ErrorMessageContext int
	MaxErrors           int
	MaxNodes            int
}

// NewParser creates a parser for the dialect.
func (d *Dialect) NewParser(opts *ParseOptions) *Parser {
	p := &Parser{
		d:                   d,
		s:                   d.P,
		errorLevel:          ErrorLevelImmediate,
		errorMessageContext: 100,
		maxErrors:           3,
		maxNodes:            -1,
	}
	if opts != nil {
		if opts.ErrorLevel != nil {
			p.errorLevel = *opts.ErrorLevel
		}
		if opts.ErrorMessageContext > 0 {
			p.errorMessageContext = opts.ErrorMessageContext
		}
		if opts.MaxErrors > 0 {
			p.maxErrors = opts.MaxErrors
		}
		if opts.MaxNodes != 0 {
			p.maxNodes = opts.MaxNodes
		}
	}
	p.reset()
	return p
}

func (p *Parser) reset() {
	p.sql = ""
	p.sqlRunes = nil
	p.errors = nil
	p.tokens = nil
	p.tokensSize = 0
	p.index = 0
	p.maxIndex = 0
	p.stall = 0
	p.curr = sentinelNone
	p.next = sentinelNone
	p.prev = sentinelNone
	p.prevComments = nil
	p.pipeCteCounter = 0
	p.chunks = nil
	p.chunkIndex = 0
	p.nodeCount = 0
}

func (p *Parser) advance(times int) {
	index := p.index + times
	p.index = index
	if index >= 0 && index < p.tokensSize {
		p.curr = p.tokens[index]
	} else if index < 0 && -index <= p.tokensSize {
		// Python negative indexing
		p.curr = p.tokens[p.tokensSize+index]
	} else {
		p.curr = sentinelNone
	}
	if index+1 >= 0 && index+1 < p.tokensSize {
		p.next = p.tokens[index+1]
	} else if index+1 < 0 && -(index+1) <= p.tokensSize {
		p.next = p.tokens[p.tokensSize+index+1]
	} else {
		p.next = sentinelNone
	}
	if index > 0 {
		prev := p.tokens[index-1]
		p.prev = prev
		p.prevComments = prev.Comments
	} else {
		p.prev = sentinelNone
		p.prevComments = nil
	}
}

func (p *Parser) advanceChunk() {
	p.index = -1
	p.tokens = p.chunks[p.chunkIndex]
	p.tokensSize = len(p.tokens)
	p.chunkIndex++
	p.advance(1)
}

func (p *Parser) retreat(index int) {
	if index != p.index {
		p.advance(index - p.index)
	}
}

func (p *Parser) addComments(e *Expr) {
	if e != nil && len(p.prevComments) > 0 {
		e.AddComments(p.prevComments, false)
		p.prevComments = nil
	}
}

// match mirrors Parser._match(token_type).
func (p *Parser) match(tt TokenType) bool {
	p.tick()
	if p.curr.Type == tt {
		p.advance(1)
		p.addComments(nil)
		return true
	}
	return false
}

// matchNoAdvance mirrors Parser._match(token_type, advance=False).
func (p *Parser) matchNoAdvance(tt TokenType) bool { return p.curr.Type == tt }

// matchExpr mirrors Parser._match(token_type, expression=e).
func (p *Parser) matchExpr(tt TokenType, e *Expr) bool {
	if p.curr.Type == tt {
		p.advance(1)
		p.addComments(e)
		return true
	}
	return false
}

// matchSet mirrors Parser._match_set(types).
func (p *Parser) matchSet(types TokenSet) bool {
	p.tick()
	if types.Has(p.curr.Type) {
		p.advance(1)
		return true
	}
	return false
}

func (p *Parser) matchSetNoAdvance(types TokenSet) bool { return types.Has(p.curr.Type) }

// matchAny mirrors Parser._match_set on an ad-hoc tuple of token types.
func (p *Parser) matchAny(types ...TokenType) bool {
	for _, t := range types {
		if p.curr.Type == t {
			p.advance(1)
			return true
		}
	}
	return false
}

func (p *Parser) matchAnyNoAdvance(types ...TokenType) bool {
	for _, t := range types {
		if p.curr.Type == t {
			return true
		}
	}
	return false
}

func (p *Parser) matchPair(a, b TokenType) bool {
	p.tick()
	if p.curr.Type == a && p.next.Type == b {
		p.advance(2)
		return true
	}
	return false
}

func (p *Parser) matchPairNoAdvance(a, b TokenType) bool {
	return p.curr.Type == a && p.next.Type == b
}

// matchTexts mirrors Parser._match_texts(texts).
func (p *Parser) matchTexts(texts ...string) bool {
	p.tick()
	if !p.s.TEXT_MATCH_EXCLUDED_TOKENS.Has(p.curr.Type) {
		up := pyUpper(p.curr.Text)
		for _, t := range texts {
			if up == t {
				p.advance(1)
				return true
			}
		}
	}
	return false
}

// matchTextSet mirrors Parser._match_texts with a set/mapping argument.
func (p *Parser) matchTextSet(texts StrSet) bool {
	if !p.s.TEXT_MATCH_EXCLUDED_TOKENS.Has(p.curr.Type) && texts.Has(pyUpper(p.curr.Text)) {
		p.advance(1)
		return true
	}
	return false
}

func (p *Parser) matchTextSetNoAdvance(texts StrSet) bool {
	return !p.s.TEXT_MATCH_EXCLUDED_TOKENS.Has(p.curr.Type) && texts.Has(pyUpper(p.curr.Text))
}

func matchTextKeys[V any](p *Parser, m map[string]V) bool {
	if p.s.TEXT_MATCH_EXCLUDED_TOKENS.Has(p.curr.Type) {
		return false
	}
	if _, ok := m[pyUpper(p.curr.Text)]; ok {
		p.advance(1)
		return true
	}
	return false
}

// matchTextSeq mirrors Parser._match_text_seq(*texts).
func (p *Parser) matchTextSeq(texts ...string) bool {
	p.tick()
	index := p.index
	for _, text := range texts {
		if !p.s.TEXT_MATCH_EXCLUDED_TOKENS.Has(p.curr.Type) && pyUpper(p.curr.Text) == text {
			p.advance(1)
		} else {
			p.retreat(index)
			return false
		}
	}
	return true
}

// matchTextSeqNoAdvance mirrors Parser._match_text_seq(*texts, advance=False).
func (p *Parser) matchTextSeqNoAdvance(texts ...string) bool {
	index := p.index
	ok := p.matchTextSeq(texts...)
	if ok {
		p.retreat(index)
	}
	return ok
}

func (p *Parser) isConnected() bool {
	return p.prev.ok() && p.curr.ok() && p.prev.End+1 == p.curr.Start
}

func (p *Parser) findSQL(start, end *Token) string {
	return pySlice(p.sqlRunes, start.Start, end.End+1)
}

type parsePanic struct{ err *ParseError }

// raiseError mirrors Parser.raise_error. tok may be nil.
func (p *Parser) raiseError(message string, tok *Token) {
	if !tok.ok() {
		tok = p.curr
		if !tok.ok() {
			tok = p.prev
			if !tok.ok() {
				tok = &Token{Type: TK_STRING, Text: "", Line: 1, Col: 1}
			}
		}
	}
	formatted, startCtx, highlight, endCtx := highlightSQL(p.sqlRunes, [][2]int{{tok.Start, tok.End}}, p.errorMessageContext)
	msg := fmt.Sprintf("%s. Line %d, Col: %d.\n  %s", message, tok.Line, tok.Col, formatted)
	err := &ParseError{Msg: msg, Errors: []ParseErrorDetail{{
		Description:  message,
		Line:         tok.Line,
		Col:          tok.Col,
		StartContext: startCtx,
		Highlight:    highlight,
		EndContext:   endCtx,
	}}}
	if p.errorLevel == ErrorLevelImmediate {
		panic(parsePanic{err})
	}
	p.errors = append(p.errors, err)
}

func (p *Parser) validateExpression(e *Expr, args []*Expr) *Expr {
	if p.maxNodes > -1 {
		p.nodeCount++
		if p.nodeCount > p.maxNodes {
			p.raiseError(fmt.Sprintf("Maximum number of AST nodes (%d) exceeded", p.maxNodes), nil)
		}
	}
	if p.errorLevel != ErrorLevelIgnore {
		if e == nil {
			panic(&ValueError{Msg: "'NoneType' object has no attribute 'error_messages'"})
		}
		for _, m := range e.ErrorMessages(args) {
			p.raiseError(m, nil)
		}
	}
	return e
}

// tryParse mirrors Parser._try_parse.
func tryParse[T any](p *Parser, fn func() T, retreat bool, isNil func(T) bool) (result T) {
	index := p.index
	level := p.errorLevel
	p.errorLevel = ErrorLevelImmediate
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(parsePanic); !ok {
				panic(r)
			}
			var zero T
			result = zero
			p.retreat(index)
			p.errorLevel = level
			return
		}
		if isNil(result) || retreat {
			p.retreat(index)
		}
		p.errorLevel = level
	}()
	result = fn()
	return result
}

// tryParseExpr is tryParse specialized for expression-returning methods.
func (p *Parser) tryParseExpr(fn func() *Expr, retreat bool) *Expr {
	return tryParse(p, fn, retreat, func(e *Expr) bool { return e == nil })
}

// expression mirrors Parser.expression(instance).
func (p *Parser) expression(e *Expr) *Expr {
	p.addComments(e)
	if !e.kind.isPrimitive() {
		e = p.validateExpression(e, nil)
	}
	return e
}

// expressionTok mirrors Parser.expression(instance, token).
func (p *Parser) expressionTok(e *Expr, tok *Token) *Expr {
	if tok.ok() {
		e.updatePositionsTok(tok)
	}
	return p.expression(e)
}

// expressionC mirrors Parser.expression(instance, comments=comments).
func (p *Parser) expressionC(e *Expr, comments []string) *Expr {
	if len(comments) > 0 {
		e.AddComments(comments, false)
	} else {
		p.addComments(e)
	}
	if !e.kind.isPrimitive() {
		e = p.validateExpression(e, nil)
	}
	return e
}

func (e *Expr) updatePositionsTok(tok *Token) {
	e.setPositions(tok.Line, tok.Col, tok.Start, tok.End)
}

// updatePositionsFrom mirrors update_positions(other_expression).
func (e *Expr) updatePositionsFrom(other *Expr) *Expr {
	if other == nil {
		return e
	}
	if other.posSet {
		e.setPositions(int(other.posLine), int(other.posCol), int(other.posStart), int(other.posEnd))
	}
	if other.meta != nil {
		for _, k := range [...]string{"line", "col", "start", "end"} {
			if v, ok := other.meta[k]; ok {
				e.Meta()[k] = v
			}
		}
	}
	return e
}

func (p *Parser) parseBatchStatements(parseMethod func(p *Parser) *Expr, sepFirstStatement bool) []*Expr {
	var expressions []*Expr
	if sepFirstStatement {
		p.match(TK_BEGIN)
		expressions = append(expressions, parseMethod(p))
	}
	chunksLength := len(p.chunks)
	for p.chunkIndex < chunksLength {
		p.advanceChunk()
		if p.matchNoAdvance(TK_ELSE) {
			return expressions
		}
		if len(expressions) > 0 && !p.next.ok() && p.match(TK_END) {
			expressions = append(expressions, New(KEndStatement))
			continue
		}
		expressions = append(expressions, parseMethod(p))
		if p.index < p.tokensSize {
			p.raiseError("Invalid expression / Unexpected token", nil)
		}
		p.checkErrors()
	}
	return expressions
}

func (p *Parser) parseImpl(parseMethod func(p *Parser) *Expr, rawTokens []*Token, sql string) []*Expr {
	p.reset()
	p.sql = sql
	p.sqlRunes = []rune(sql)
	total := len(rawTokens)
	chunks := [][]*Token{{}}
	for i, tok := range rawTokens {
		if tok.Type == TK_SEMICOLON {
			if len(tok.Comments) > 0 {
				chunks = append(chunks, []*Token{tok})
			}
			if i < total-1 {
				chunks = append(chunks, []*Token{})
			}
		} else {
			chunks[len(chunks)-1] = append(chunks[len(chunks)-1], tok)
		}
	}
	p.chunks = chunks
	return p.parseBatchStatements(parseMethod, false)
}

// Logger receives the messages SQLGlot logs (parse errors under ErrorLevel.WARN). It discards them
// by default; embedders can replace it.
var Logger = func(msg string) {}

func (p *Parser) checkErrors() {
	if p.errorLevel == ErrorLevelWarn {
		for _, e := range p.errors {
			Logger(e.Error())
		}
	} else if p.errorLevel == ErrorLevelRaise && len(p.errors) > 0 {
		errs := make([]error, len(p.errors))
		var details []ParseErrorDetail
		for i, e := range p.errors {
			errs[i] = e
			details = append(details, e.Errors...)
		}
		panic(parsePanic{&ParseError{Msg: concatMessages(errs, p.maxErrors), Errors: details}})
	}
}

// recoverParse converts parser panics into errors.
func recoverParse(err *error) {
	if r := recover(); r != nil {
		switch x := r.(type) {
		case parsePanic:
			*err = x.err
		case *ValueError:
			*err = x
		case *TokenError:
			*err = x
		case *UnsupportedError:
			*err = x
		default:
			*err = internalError(x)
		}
	}
}

// internalError converts an unexpected panic (a Go runtime error in ported code) into an error.
// Out-of-range indexing is reported like the IndexError Python raises at the same spot.
func internalError(r any) error {
	if re, ok := r.(runtime.Error); ok && strings.Contains(re.Error(), "index out of range") {
		return &ValueError{Msg: "list index out of range"}
	}
	if e, ok := r.(error); ok {
		return fmt.Errorf("internal error: %w", e)
	}
	return fmt.Errorf("internal error: %v", r)
}

// ParseTokens mirrors Parser.parse(raw_tokens, sql).
func (p *Parser) ParseTokens(tokens []*Token, sql string) (out []*Expr, err error) {
	defer recoverParse(&err)
	out = p.parseImpl(func(p *Parser) *Expr { return p.parseStatement() }, tokens, sql)
	return out, nil
}

// ParseInto mirrors Parser.parse_into for a single expression type.
func (p *Parser) ParseInto(kind Kind, tokens []*Token, sql string) (out []*Expr, err error) {
	defer recoverParse(&err)
	fn, ok := p.s.EXPRESSION_PARSERS[kind]
	if !ok {
		return nil, fmt.Errorf("No parser registered for %s", kind.Name())
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				if pp, ok := r.(parsePanic); ok {
					if len(pp.err.Errors) > 0 {
						pp.err.Errors[0].IntoExpression = kind.Name()
					}
					tokText := sql
					panic(parsePanic{&ParseError{
						Msg:    fmt.Sprintf("Failed to parse '%s' into %s", tokText, kindClassRepr(kind)),
						Errors: pp.err.Errors,
					}})
				}
				panic(r)
			}
		}()
		out = p.parseImpl(fn, tokens, sql)
	}()
	return out, nil
}

func kindClassRepr(k Kind) string {
	return "<class 'sqlglot.expressions." + kindModule(k) + "." + k.Name() + "'>"
}

// Parse mirrors Dialect.parse(sql).
func (d *Dialect) Parse(sql string, opts *ParseOptions) ([]*Expr, error) {
	tokens, err := d.Tokenize(sql)
	if err != nil {
		return nil, err
	}
	return d.NewParser(opts).ParseTokens(tokens, sql)
}

// ParseOne mirrors sqlglot.parse_one(sql, read=dialect).
func (d *Dialect) ParseOne(sql string, opts *ParseOptions) (*Expr, error) {
	result, err := d.Parse(sql, opts)
	if err != nil {
		return nil, err
	}
	if len(result) == 0 || result[0] == nil {
		return nil, &ParseError{Msg: fmt.Sprintf("No expression was parsed from '%s'", sql)}
	}
	if len(result) > 1 {
		return New(KBlock, "expressions", result), nil
	}
	return result[0], nil
}

// ParseOneInto mirrors sqlglot.parse_one(sql, read=dialect, into=kind).
func (d *Dialect) ParseOneInto(kind Kind, sql string, opts *ParseOptions) (*Expr, error) {
	tokens, err := d.Tokenize(sql)
	if err != nil {
		return nil, err
	}
	result, err := d.NewParser(opts).ParseInto(kind, tokens, sql)
	if err != nil {
		return nil, err
	}
	if len(result) == 0 || result[0] == nil {
		return nil, &ParseError{Msg: fmt.Sprintf("No expression was parsed from '%s'", sql)}
	}
	if len(result) > 1 {
		return New(KBlock, "expressions", result), nil
	}
	return result[0], nil
}

func (p *Parser) warnUnsupported() {
	if p.tokensSize <= 1 {
		return
	}
	sql := []rune(p.findSQL(p.tokens[0], p.tokens[len(p.tokens)-1]))
	if len(sql) > p.errorMessageContext {
		sql = sql[:p.errorMessageContext]
	}
	_ = sql // sqlglot logs a warning here; we stay silent.
}

func upperText(t *Token) string { return pyUpper(t.Text) }

func seqGet(list []*Expr, i int) *Expr {
	if i < 0 {
		i += len(list)
	}
	if i < 0 || i >= len(list) {
		return nil
	}
	return list[i]
}

func ensureList(e *Expr) []*Expr {
	if e == nil {
		return []*Expr{}
	}
	return []*Expr{e}
}
