package sqlengine

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
)

// Function types of the parser's dispatch tables (FUNCTIONS, STATEMENT_PARSERS, ...).
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

// propKwargs holds the optional modifiers (NO, DUAL, BEFORE, ...) passed to PROPERTY_PARSERS
// entries.
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
	JSON_OPERATORS                map[TokenType]columnOperatorFn
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

	// noParenOverlay holds temporary NO_PAREN_FUNCTION_PARSERS entries for this parser only, so the
	// dialect's shared table is never mutated.
	noParenOverlay map[string]parseFn

	// depth counts nested entries into the recursive parse methods (see enter).
	depth int
	// maxIndex and stall implement the no-progress guard (see tick).
	maxIndex int
	stall    int
}

// maxParseStall bounds how many token-match attempts may happen without the parser ever reaching
// a new token position. A few parse loops would otherwise spin forever on malformed input (e.g. a
// property parser that retreats inside an endless property loop); the parser raises an error
// instead. Legitimate parses (including backtracking) stay far below this.
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

// maxParseDepth bounds parser recursion, so runaway recursion (e.g. a statement parser that
// re-enters itself without consuming tokens) becomes an error instead of a Go stack overflow that
// would kill the process. The limit is far above the nesting of legitimate SQL.
const maxParseDepth = 3000

// enter and leave bracket the recursive parse entry points.
func (p *Parser) enter() {
	p.depth++
	if p.depth > maxParseDepth {
		panic(&ValueError{Msg: "maximum recursion depth exceeded"})
	}
}

func (p *Parser) leave() { p.depth-- }

var sentinelNone = &Token{Type: TK_SENTINEL, Text: "SENTINEL", Line: 1, Col: 1, Comments: []string{}}

func (t *Token) ok() bool { return t != nil && t.Type != TK_SENTINEL }

// ParseOptions configures a parser; zero values keep the defaults.
type ParseOptions struct {
	ErrorLevel          *ErrorLevel
	ErrorMessageContext int
	MaxErrors           int
	MaxNodes            int
}

func (d *Dialect) NewParser(opts *ParseOptions) *Parser {
	p := &Parser{}
	d.initParser(p, opts)
	return p
}

// parserPool recycles parsers for the parses that create and drop their own (Dialect.Parse and
// ParseOneInto): nothing references a parser once its parse has returned.
var parserPool = sync.Pool{New: func() any { return new(Parser) }}

func (d *Dialect) pooledParser(opts *ParseOptions) *Parser {
	p := parserPool.Get().(*Parser)
	d.initParser(p, opts)
	return p
}

func releaseParser(p *Parser) {
	*p = Parser{}
	parserPool.Put(p)
}

func (d *Dialect) initParser(p *Parser, opts *ParseOptions) {
	*p = Parser{
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
		// A negative index counts from the end
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

func (p *Parser) match(tt TokenType) bool {
	p.tick()
	if p.curr.Type == tt {
		p.advance(1)
		p.addComments(nil)
		return true
	}
	return false
}

func (p *Parser) matchNoAdvance(tt TokenType) bool { return p.curr.Type == tt }

// matchExpr consumes the current token if it has type tt, attaching its comments to e.
func (p *Parser) matchExpr(tt TokenType, e *Expr) bool {
	if p.curr.Type == tt {
		p.advance(1)
		p.addComments(e)
		return true
	}
	return false
}

func (p *Parser) matchSet(types TokenSet) bool {
	p.tick()
	if types.Has(p.curr.Type) {
		p.advance(1)
		return true
	}
	return false
}

func (p *Parser) matchSetNoAdvance(types TokenSet) bool { return types.Has(p.curr.Type) }

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

// matchTexts consumes the current token if its text equals one of texts, ignoring case.
func (p *Parser) matchTexts(texts ...string) bool {
	p.tick()
	if !p.s.TEXT_MATCH_EXCLUDED_TOKENS.Has(p.curr.Type) {
		for _, t := range texts {
			if pyUpperEq(p.curr.Text, t) {
				p.advance(1)
				return true
			}
		}
	}
	return false
}

func hasUpperText[V any](texts map[string]V, text string) bool {
	var buf [64]byte
	if b, ok := upperASCII(buf[:], text); ok {
		_, found := texts[string(b)]
		return found
	}
	_, found := texts[pyUpper(text)]
	return found
}

func (p *Parser) matchTextSet(texts StrSet) bool {
	if !p.s.TEXT_MATCH_EXCLUDED_TOKENS.Has(p.curr.Type) && hasUpperText(texts, p.curr.Text) {
		p.advance(1)
		return true
	}
	return false
}

func (p *Parser) matchTextSetNoAdvance(texts StrSet) bool {
	return !p.s.TEXT_MATCH_EXCLUDED_TOKENS.Has(p.curr.Type) && hasUpperText(texts, p.curr.Text)
}

func matchTextKeysNoAdvance[V any](p *Parser, m map[string]V) bool {
	return !p.s.TEXT_MATCH_EXCLUDED_TOKENS.Has(p.curr.Type) && hasUpperText(m, p.curr.Text)
}

func matchTextKeys[V any](p *Parser, m map[string]V) bool {
	if p.s.TEXT_MATCH_EXCLUDED_TOKENS.Has(p.curr.Type) {
		return false
	}
	if hasUpperText(m, p.curr.Text) {
		p.advance(1)
		return true
	}
	return false
}

// matchTextSeq consumes the next tokens if their texts equal texts, ignoring case; otherwise it
// consumes nothing.
func (p *Parser) matchTextSeq(texts ...string) bool {
	p.tick()
	index := p.index
	for _, text := range texts {
		if !p.s.TEXT_MATCH_EXCLUDED_TOKENS.Has(p.curr.Type) && pyUpperEq(p.curr.Text, text) {
			p.advance(1)
		} else {
			p.retreat(index)
			return false
		}
	}
	return true
}

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
	return pySlice(p.runes(), int(start.Start), int(end.End)+1)
}

// runes returns the SQL as code points (token positions are code-point offsets),
// converting lazily: only error reporting and command fallbacks need it.
func (p *Parser) runes() []rune {
	if p.sqlRunes == nil {
		p.sqlRunes = []rune(p.sql)
	}
	return p.sqlRunes
}

type parsePanic struct{ err *ParseError }

// raiseError records a parse error at tok, highlighting it in the SQL; a nil tok means the current
// (or previous) token. With ErrorLevelImmediate it panics right away.
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
	formatted, startCtx, highlight, endCtx := highlightSQL(p.runes(), [][2]int{{int(tok.Start), int(tok.End)}}, p.errorMessageContext)
	msg := fmt.Sprintf("%s. Line %d, Col: %d.\n  %s", message, tok.Line, tok.Col, formatted)
	err := &ParseError{Msg: msg, Errors: []ParseErrorDetail{{
		Description:  message,
		Line:         int(tok.Line),
		Col:          int(tok.Col),
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

// tryParse runs fn with errors raised immediately. It backs up to where it started on a parse
// error (returning the zero value), on a nil result, or when retreat is set.
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

func (p *Parser) tryParseExpr(fn func() *Expr, retreat bool) *Expr {
	return tryParse(p, fn, retreat, func(e *Expr) bool { return e == nil })
}

// expression attaches pending comments to e and validates it unless it is a primitive kind.
func (p *Parser) expression(e *Expr) *Expr {
	p.addComments(e)
	if !e.kind.isPrimitive() {
		e = p.validateExpression(e, nil)
	}
	return e
}

func (p *Parser) expressionTok(e *Expr, tok *Token) *Expr {
	if tok.ok() {
		e.updatePositionsTok(tok)
	}
	return p.expression(e)
}

// expressionC is expression with explicit comments, used instead of the pending ones when
// non-empty.
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
	e.posLine, e.posCol, e.posStart, e.posEnd = tok.Line, tok.Col, tok.Start, tok.End
	e.flags |= flagPosSet
	e.clearPositionMeta()
}

func (e *Expr) updatePositionsFrom(other *Expr) *Expr {
	if other == nil {
		return e
	}
	if other.posSet() {
		e.setPositions(int(other.posLine), int(other.posCol), int(other.posStart), int(other.posEnd))
	}
	if m := other.metaMap(); m != nil {
		for _, k := range [...]string{"line", "col", "start", "end"} {
			if v, ok := m[k]; ok {
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
	total := len(rawTokens)
	// Split into statement chunks (semicolons separate statements; a semicolon carrying
	// comments forms its own chunk). Chunks are views into one array.
	all := make([]*Token, 0, total)
	type span struct{ a, b int }
	var spanBuf [4]span
	spans := append(spanBuf[:0], span{})
	for i, tok := range rawTokens {
		if tok.Type == TK_SEMICOLON {
			if len(tok.Comments) > 0 {
				all = append(all, tok)
				spans = append(spans, span{len(all) - 1, len(all)})
			}
			if i < total-1 {
				spans = append(spans, span{len(all), len(all)})
			}
		} else {
			all = append(all, tok)
			spans[len(spans)-1].b = len(all)
		}
	}
	chunks := make([][]*Token, len(spans))
	for i, sp := range spans {
		chunks[i] = all[sp.a:sp.b:sp.b]
	}
	p.chunks = chunks
	return p.parseBatchStatements(parseMethod, false)
}

// Logger receives log messages (parse errors under ErrorLevelWarn). It discards them by default;
// embedders can replace it.
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

// internalError converts an unexpected panic (such as a Go runtime error) into an error.
// Out-of-range indexing keeps its historical "list index out of range" message.
func internalError(r any) error {
	if re, ok := r.(runtime.Error); ok && strings.Contains(re.Error(), "index out of range") {
		return &ValueError{Msg: "list index out of range"}
	}
	if e, ok := r.(error); ok {
		return fmt.Errorf("internal error: %w", e)
	}
	return fmt.Errorf("internal error: %v", r)
}

func (p *Parser) ParseTokens(tokens []*Token, sql string) (out []*Expr, err error) {
	defer recoverParse(&err)
	out = p.parseImpl(func(p *Parser) *Expr { return p.parseStatement() }, tokens, sql)
	return out, nil
}

// ParseInto parses the tokens of sql as expressions of the given kind, using its
// EXPRESSION_PARSERS entry.
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
						Msg:    fmt.Sprintf("Failed to parse '%s' into %s", tokText, kind.Name()),
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

// Parse tokenizes and parses sql into one expression per statement.
func (d *Dialect) Parse(sql string, opts *ParseOptions) ([]*Expr, error) {
	tokens, arena, err := d.tokenizeForParse(sql)
	if arena != nil {
		defer arena.release()
	}
	if err != nil {
		return nil, err
	}
	p := d.pooledParser(opts)
	defer releaseParser(p)
	return p.ParseTokens(tokens, sql)
}

// tokenizeForParse tokenizes sql for a parse that keeps no tokens, into a pooled arena (nil when
// a dialect tokenizer hook is in use).
func (d *Dialect) tokenizeForParse(sql string) ([]*Token, *tokenArena, error) {
	if d.hooks.tokenize != nil {
		tokens, err := d.Tokenize(sql)
		return tokens, nil, err
	}
	return d.tok.tokenizeInArena(sql)
}

// ParseOne parses sql into a single expression; several statements are wrapped in a Block.
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

// ParseOneInto is ParseOne with sql parsed as the given kind.
func (d *Dialect) ParseOneInto(kind Kind, sql string, opts *ParseOptions) (*Expr, error) {
	tokens, arena, err := d.tokenizeForParse(sql)
	if arena != nil {
		defer arena.release()
	}
	if err != nil {
		return nil, err
	}
	p := d.pooledParser(opts)
	defer releaseParser(p)
	result, err := p.ParseInto(kind, tokens, sql)
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
	_ = sql // No warning is logged.
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
