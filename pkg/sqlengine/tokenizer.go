package sqlengine

import (
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"
)

// Token is a lexical token of SQL text. Positions are offsets in Unicode code points.
type Token struct {
	Type     TokenType
	Line     int32
	Col      int32
	Start    int32
	End      int32
	Text     string
	Comments []string
}

// String formats the token for debugging, e.g. <Token token_type: TokenType.VAR, text: x, ...>,
// with the comments quoted.
func (t *Token) String() string {
	cs := make([]string, len(t.Comments))
	for i, c := range t.Comments {
		cs[i] = pyRepr(c)
	}
	return fmt.Sprintf("<Token token_type: TokenType.%s, text: %s, line: %d, col: %d, start: %d, end: %d, comments: [%s]>",
		tokenTypeNames[t.Type], t.Text, t.Line, t.Col, t.Start, t.End, strings.Join(cs, ", "))
}

// Name returns the token type name, e.g. "L_PAREN".
func (t TokenType) Name() string {
	if int(t) < len(tokenTypeNames) {
		return tokenTypeNames[t]
	}
	return "?"
}

func (t TokenType) String() string { return "TokenType." + t.Name() }

// TokenError reports SQL that could not be tokenized.
type TokenError struct{ Msg string }

func (e *TokenError) Error() string { return e.Msg }

// tokenizerConfig is the fully-resolved configuration that a tokenizerCore reads.
type tokenizerConfig struct {
	singleTokens                     map[rune]TokenType
	keywords                         map[string]TokenType
	quotes                           map[string]string
	formatStrings                    map[string]formatString
	identifiers                      map[rune]string
	comments                         map[string]string
	stringEscapes                    StrSet
	byteStringEscapes                StrSet
	identifierEscapes                StrSet
	escapeFollowChars                StrSet
	commands                         TokenSet
	commandPrefixTokens              TokenSet
	nestedComments                   bool
	hintStart                        string
	tokensPrecedingHint              TokenSet
	hasBitStrings                    bool
	hasHexStrings                    bool
	numericLiterals                  map[string]string
	varSingleTokens                  StrSet
	stringEscapesAllowedInRawStrings bool
	heredocTagIsIdentifier           bool
	heredocStringAlternative         TokenType
	keywordTrie                      *trie
	numbersCanBeUnderscoreSeparated  bool
	numbersCanHaveDecimals           bool
	identifiersCanStartWithDigit     bool
	unescapedSequences               map[string]string
	// numericEscapes maps the char after a backslash ('0' for octal) to the escape's base,
	// minimum and maximum digit count and maximum value.
	numericEscapes map[rune][4]int
	// numericEscapesAreBytes makes byte-valued numeric escapes (octal, and hex up to 0xFF) raw
	// bytes, so that consecutive ones decode together as UTF-8.
	numericEscapesAreBytes   bool
	dropUnknownEscapes       bool
	loneSurrogateReplacement string
	// byteStringsAreBytes stores byte string literals as bytes: each char of the text is a byte,
	// so non-ASCII chars are kept as their UTF-8 bytes.
	byteStringsAreBytes bool
	// identEscapes holds, per identifier delimiter, IDENTIFIER_ESCAPES plus the delimiter.
	identEscapes map[string]StrSet
	// kwText interns the KEYWORDS keys (token texts of keyword tokens).
	kwText map[string]string
	// singleASCII duplicates singleTokens for ASCII runes (singleASCIIOk marks presence).
	singleASCII   [utf8.RuneSelf]TokenType
	singleASCIIOk [utf8.RuneSelf]bool
}

// single looks up the SINGLE_TOKENS type of the character r.
func (c *tokenizerConfig) single(r rune) (TokenType, bool) {
	if r >= 0 && r < utf8.RuneSelf {
		return c.singleASCII[r], c.singleASCIIOk[r]
	}
	tt, ok := c.singleTokens[r]
	return tt, ok
}

func newTokenizerConfig(s *TokenizerSettings, d *DialectSettings) *tokenizerConfig {
	c := &tokenizerConfig{
		singleTokens:                     map[rune]TokenType{},
		keywords:                         s.KEYWORDS,
		quotes:                           s._QUOTES,
		formatStrings:                    s._FORMAT_STRINGS,
		identifiers:                      map[rune]string{},
		comments:                         s._COMMENTS,
		stringEscapes:                    s._STRING_ESCAPES,
		byteStringEscapes:                s._BYTE_STRING_ESCAPES,
		identifierEscapes:                s._IDENTIFIER_ESCAPES,
		escapeFollowChars:                s._ESCAPE_FOLLOW_CHARS,
		commands:                         s.COMMANDS,
		commandPrefixTokens:              s.COMMAND_PREFIX_TOKENS,
		nestedComments:                   s.NESTED_COMMENTS,
		hintStart:                        s.HINT_START,
		tokensPrecedingHint:              s.TOKENS_PRECEDING_HINT,
		hasBitStrings:                    len(s.BIT_STRINGS) > 0,
		hasHexStrings:                    len(s.HEX_STRINGS) > 0,
		numericLiterals:                  s.NUMERIC_LITERALS,
		varSingleTokens:                  s.VAR_SINGLE_TOKENS,
		stringEscapesAllowedInRawStrings: s.STRING_ESCAPES_ALLOWED_IN_RAW_STRINGS,
		heredocTagIsIdentifier:           s.HEREDOC_TAG_IS_IDENTIFIER,
		heredocStringAlternative:         s.HEREDOC_STRING_ALTERNATIVE,
		numbersCanHaveDecimals:           s.NUMBERS_CAN_HAVE_DECIMALS,
		numericEscapesAreBytes:           s.NUMERIC_ESCAPES_ARE_BYTES,
		dropUnknownEscapes:               s.DROP_UNKNOWN_ESCAPES,
		loneSurrogateReplacement:         s.LONE_SURROGATE_REPLACEMENT,
	}
	if len(s.NUMERIC_ESCAPES) > 0 {
		c.numericEscapes = map[rune][4]int{}
		for k, v := range s.NUMERIC_ESCAPES {
			c.numericEscapes[[]rune(k)[0]] = v
		}
	}
	for k, v := range s.SINGLE_TOKENS {
		r := []rune(k)
		if len(r) == 1 {
			c.singleTokens[r[0]] = v
			if r[0] >= 0 && r[0] < utf8.RuneSelf {
				c.singleASCII[r[0]] = v
				c.singleASCIIOk[r[0]] = true
			}
		}
	}
	for k, v := range s._IDENTIFIERS {
		r := []rune(k)
		if len(r) == 1 {
			c.identifiers[r[0]] = v
		}
	}
	if d != nil {
		c.numbersCanBeUnderscoreSeparated = d.NUMBERS_CAN_BE_UNDERSCORE_SEPARATED
		c.identifiersCanStartWithDigit = d.IDENTIFIERS_CAN_START_WITH_DIGIT
		c.unescapedSequences = d.UNESCAPED_SEQUENCES
		c.byteStringsAreBytes = d.BYTE_STRING_IS_BYTES_TYPE
	}
	c.keywordTrie = buildKeywordTrie(s)
	c.identEscapes = map[string]StrSet{}
	for _, end := range c.identifiers {
		escapes := c.identifierEscapes.Clone()
		escapes[end] = struct{}{}
		c.identEscapes[end] = escapes
	}
	c.kwText = make(map[string]string, len(c.keywords))
	for k := range c.keywords {
		c.kwText[k] = k
	}
	return c
}

func buildKeywordTrie(s *TokenizerSettings) *trie {
	var keys []string
	add := func(key string) {
		if strings.Contains(key, " ") {
			keys = append(keys, pyUpper(key))
			return
		}
		for single := range s.SINGLE_TOKENS {
			if strings.Contains(key, single) {
				keys = append(keys, pyUpper(key))
				return
			}
		}
	}
	for k := range s.KEYWORDS {
		add(k)
	}
	for k := range s._COMMENTS {
		add(k)
	}
	for k := range s._QUOTES {
		add(k)
	}
	for k := range s._FORMAT_STRINGS {
		add(k)
	}
	return newTrieFromStrings(keys...)
}

const noChar rune = -1

// tokenizerCore holds the state of one tokenization run; cores are pooled.
type tokenizerCore struct {
	*tokenizerConfig
	sql           []rune
	src           string // the input; token texts are substrings of it when ascii
	ascii         bool
	kwBuf         []rune
	slab          []Token
	arena         *tokenArena
	size          int
	tokens        []*Token
	start         int
	current       int
	line          int
	col           int
	comments      []string
	char          rune
	end           bool
	peek          rune
	prevTokenLine int
}

type tokenPanic struct{ msg string }

var tokenizerPool = sync.Pool{New: func() any { return &tokenizerCore{} }}

func newTokenizerCore(c *tokenizerConfig) *tokenizerCore {
	t := tokenizerPool.Get().(*tokenizerCore)
	t.tokenizerConfig = c
	return t
}

// release returns the core to the pool; the returned tokens do not reference its buffers.
func (t *tokenizerCore) release() {
	if t.arena != nil {
		t.arena.lastLen = len(t.slab)
		t.arena = nil
	}
	t.tokenizerConfig = nil
	t.tokens = nil
	t.comments = nil
	t.slab = nil
	t.src = ""
	if cap(t.sql) > 1<<16 {
		t.sql = nil
	}
	tokenizerPool.Put(t)
}

func (t *tokenizerCore) reset() {
	t.sql = t.sql[:0]
	t.src = ""
	t.ascii = false
	t.slab = nil
	t.size = 0
	t.tokens = nil
	t.start = 0
	t.current = 0
	t.line = 1
	t.col = 0
	t.comments = nil
	t.char = noChar
	t.end = false
	t.peek = noChar
	t.prevTokenLine = -1
}

func (t *tokenizerCore) tokenize(sql string) (tokens []*Token, err error) {
	defer t.release()
	t.reset()
	t.src = sql
	t.ascii = true
	for i := range len(sql) {
		if sql[i] >= utf8.RuneSelf {
			t.ascii = false
			break
		}
	}
	if t.ascii {
		if cap(t.sql) < len(sql) {
			t.sql = make([]rune, len(sql))
		}
		t.sql = t.sql[:len(sql)]
		for i := range len(sql) {
			t.sql[i] = rune(sql[i])
		}
	} else {
		t.sql = append(t.sql[:0], []rune(sql)...)
	}
	t.size = len(t.sql)
	if t.arena != nil && cap(t.arena.tokens) >= t.size/4+4 {
		t.tokens = t.arena.tokens[:0]
	} else {
		t.tokens = make([]*Token, 0, t.size/4+4)
	}

	defer func() {
		if r := recover(); r != nil {
			start := t.current - 50
			if start < 0 {
				start = 0
			}
			end := t.current + 50
			if end > t.size-1 {
				end = t.size - 1
			}
			ctx := ""
			if end > start {
				ctx = string(t.sql[start:end])
			}
			tokens = nil
			err = &TokenError{Msg: fmt.Sprintf("Error tokenizing '%s'", ctx)}
		}
	}()

	t.scan(false)
	return t.tokens, nil
}

func (t *tokenizerCore) fail(msg string) { panic(tokenPanic{msg}) }

func isDigitRune(r rune) bool { return r >= '0' && r <= '9' }

func (t *tokenizerCore) at(i int) rune {
	if i < 0 {
		i += t.size
	}
	return t.sql[i]
}

func (t *tokenizerCore) scan(checkSemicolon bool) {
	for t.size > 0 && !t.end {
		current := t.current
		for current < t.size {
			ch := t.sql[current]
			if ch == ' ' || ch == '\t' {
				current++
			} else {
				break
			}
		}
		offset := 1
		if current > t.current {
			offset = current - t.current
		}
		t.start = current
		t.advance(offset, false)

		if !pyIsSpaceRune(t.char) {
			if isDigitRune(t.char) {
				t.scanNumber()
			} else if end, ok := t.identifiers[t.char]; ok {
				t.scanIdentifier(end)
			} else {
				t.scanKeywords()
			}
		}
		if checkSemicolon && t.peek == ';' {
			break
		}
	}
	if len(t.tokens) > 0 && len(t.comments) > 0 {
		last := t.tokens[len(t.tokens)-1]
		last.Comments = append(last.Comments, t.comments...)
	}
}

// span returns the source text of runes [a, b) without copying when the input is ASCII.
func (t *tokenizerCore) span(a, b int) string {
	if t.ascii {
		return t.src[a:b]
	}
	return string(t.sql[a:b])
}

// asciiStrings holds the one-character strings of the ASCII range (no allocation per char).
var asciiStrings = func() (out [utf8.RuneSelf]string) {
	for i := range out {
		out[i] = string(rune(i))
	}
	return
}()

// runeString is string(r) without allocating for ASCII.
func runeString(r rune) string {
	if r >= 0 && r < utf8.RuneSelf {
		return asciiStrings[r]
	}
	return string(r)
}

func (t *tokenizerCore) chars(size int) string {
	if size == 1 {
		if t.char == noChar {
			return ""
		}
		return runeString(t.char)
	}
	start := t.current - 1
	end := start + size
	if end <= t.size {
		if start < 0 {
			return t.span(0, end)
		}
		return t.span(start, end)
	}
	return ""
}

func (t *tokenizerCore) advance(i int, alnum bool) {
	ch := t.char
	if ch == '\n' || ch == '\r' {
		if !(ch == '\r' && t.peek == '\n') {
			t.col = i
			t.line++
		}
	} else {
		t.col += i
	}
	t.current += i
	t.end = t.current >= t.size
	t.char = t.at(t.current - 1)
	if t.end {
		t.peek = noChar
	} else {
		t.peek = t.sql[t.current]
	}

	if alnum && pyIsAlnumRune(t.char) {
		col, cur, end, peek := t.col, t.current, t.end, t.peek
		for peek != noChar && pyIsAlnumRune(peek) {
			col++
			cur++
			end = cur >= t.size
			if end {
				peek = noChar
			} else {
				peek = t.sql[cur]
			}
		}
		t.col, t.current, t.end, t.peek = col, cur, end, peek
		t.char = t.sql[cur-1]
	}
}

func (t *tokenizerCore) text() string { return t.span(t.start, t.current) }

func (t *tokenizerCore) add0(tt TokenType) { t.push(tt, t.span(t.start, t.current)) }

func (t *tokenizerCore) addS(tt TokenType, text string) { t.push(tt, text) }

// newToken allocates tokens in slabs.
func (t *tokenizerCore) newToken() *Token {
	if len(t.slab) == cap(t.slab) {
		// First slab sized from the input (SQL averages about one token per 4 characters), later
		// ones grow with the token count.
		n := len(t.tokens)/2 + 8
		if len(t.tokens) == 0 {
			n = t.size/4 + 4
		}
		if n > 1024 {
			n = 1024
		}
		if t.arena != nil {
			t.slab = t.arena.nextSlab(n)
		} else {
			t.slab = make([]Token, 0, n)
		}
	}
	t.slab = t.slab[:len(t.slab)+1]
	return &t.slab[len(t.slab)-1]
}

// tokenArena recycles token storage across parses that tokenize their own input. Nothing keeps a
// *Token once parsing is done: expressions copy positions and texts and take over comment lists
// (allocated separately from the tokens), and errors hold only strings.
type tokenArena struct {
	slabs   [][]Token // storage handed out in order; used counts the ones in use
	used    int
	lastLen int // tokens taken from the last slab in use (earlier ones are full)
	tokens  []*Token
	nTokens int
}

const tokenArenaMaxTokens = 1 << 16

var tokenArenaPool = sync.Pool{New: func() any { return &tokenArena{} }}

func (a *tokenArena) nextSlab(n int) []Token {
	if a.used < len(a.slabs) {
		s := a.slabs[a.used]
		if cap(s) < n {
			s = make([]Token, 0, n)
			a.slabs[a.used] = s
		}
		a.used++
		return s[:0]
	}
	s := make([]Token, 0, n)
	a.slabs = append(a.slabs, s)
	a.used++
	return s
}

// tokenizeInArena tokenizes into a pooled arena; release the arena once the tokens are unused.
func (c *tokenizerConfig) tokenizeInArena(sql string) ([]*Token, *tokenArena, error) {
	a := tokenArenaPool.Get().(*tokenArena)
	t := newTokenizerCore(c)
	t.arena = a
	tokens, err := t.tokenize(sql)
	if cap(tokens) >= cap(a.tokens) {
		a.tokens = tokens[:0]
		a.nTokens = len(tokens)
	}
	return tokens, a, err
}

// release clears what the parse used (so pooled storage keeps no SQL alive) and pools the arena.
func (a *tokenArena) release() {
	total := 0
	for i, s := range a.slabs {
		switch {
		case i < a.used-1:
			clear(s[:cap(s)])
		case i == a.used-1:
			clear(s[:a.lastLen])
		}
		total += cap(s)
	}
	clear(a.tokens[:a.nTokens])
	a.used, a.lastLen, a.nTokens = 0, 0, 0
	if total+cap(a.tokens) > tokenArenaMaxTokens {
		return
	}
	tokenArenaPool.Put(a)
}

func (t *tokenizerCore) push(tt TokenType, txt string) {
	t.prevTokenLine = t.line
	if len(t.comments) > 0 && tt == TK_SEMICOLON && len(t.tokens) > 0 {
		last := t.tokens[len(t.tokens)-1]
		last.Comments = append(last.Comments, t.comments...)
		t.comments = nil
	}
	comments := t.comments
	if comments == nil {
		comments = []string{}
	}
	tok := t.newToken()
	*tok = Token{
		Type:     tt,
		Text:     txt,
		Line:     int32(t.line),
		Col:      int32(t.col),
		Start:    int32(t.start),
		End:      int32(t.current - 1),
		Comments: comments,
	}
	t.tokens = append(t.tokens, tok)
	t.comments = nil

	if t.commands.Has(tt) && t.peek != ';' &&
		(len(t.tokens) == 1 || t.commandPrefixTokens.Has(t.tokens[len(t.tokens)-2].Type)) {
		start := t.current
		n := len(t.tokens)
		t.scan(true)
		t.tokens = t.tokens[:n]
		s := pyStrip(t.span(start, t.current))
		if s != "" {
			t.addS(TK_STRING, s)
		}
	}
}

func strp(s string) *string { return &s }

func asciiUpperRune(r rune) rune {
	if r >= 'a' && r <= 'z' {
		return r - 32
	}
	return r
}

func (t *tokenizerCore) scanKeywords() {
	size := 0
	wordLen := 0
	hasWord := false
	// chars is accumulated in a reusable rune buffer; word is its prefix at the last trie end.
	chars := append(t.kwBuf[:0], t.char)
	char := t.char
	prevSpace := false
	skip := false
	tr := t.keywordTrie
	_, singleToken := t.single(char)

	for len(chars) > 0 {
		if !skip {
			sub := tr.getRune(asciiUpperRune(char))
			if sub == nil {
				break
			}
			tr = sub
			if tr.end {
				wordLen = len(chars)
				hasWord = true
			}
		}
		end := t.current + size
		size++
		if end < t.size {
			char = t.sql[end]
			if !singleToken {
				_, singleToken = t.single(char)
			}
			isSpace := pyIsSpaceRune(char)
			if !isSpace || !prevSpace {
				if isSpace {
					char = ' '
				}
				chars = append(chars, char)
				prevSpace = isSpace
				skip = false
			} else {
				skip = true
			}
		} else {
			char = noChar
			break
		}
	}
	t.kwBuf = chars

	if hasWord {
		word := chars[:wordLen]
		if t.scanString(runesString(word)) {
			return
		}
		if t.scanComment(runesString(word)) {
			return
		}
		if prevSpace || singleToken || char == noChar {
			t.advance(size-1, false)
			w, ok := t.keywordText(word)
			if !ok {
				t.fail("KeyError")
			}
			t.addS(t.keywords[w], w)
			return
		}
	}

	if tt, ok := t.single(t.char); ok {
		t.addS(tt, runeString(t.char))
		return
	}
	t.scanVar()
}

// runesString converts a short rune slice to a string (no allocation for one ASCII rune).
func runesString(r []rune) string {
	if len(r) == 1 {
		return runeString(r[0])
	}
	return string(r)
}

// keywordText returns the interned KEYWORDS key equal to pyUpper(word), if any.
func (t *tokenizerCore) keywordText(word []rune) (string, bool) {
	var buf [64]byte
	n := 0
	for _, r := range word {
		if r >= utf8.RuneSelf || n == len(buf) {
			w, ok := t.kwText[pyUpper(string(word))]
			return w, ok
		}
		if r >= 'a' && r <= 'z' {
			r -= 32
		}
		buf[n] = byte(r)
		n++
	}
	w, ok := t.kwText[string(buf[:n])]
	return w, ok
}

// keywordType looks up the KEYWORDS type of the uppercased source span [a, b).
func (t *tokenizerCore) keywordType(a, b int) (TokenType, bool) {
	var buf [64]byte
	if b-a > len(buf) {
		tt, ok := t.keywords[pyUpper(t.span(a, b))]
		return tt, ok
	}
	n := 0
	for _, r := range t.sql[a:b] {
		if r >= utf8.RuneSelf {
			tt, ok := t.keywords[pyUpper(t.span(a, b))]
			return tt, ok
		}
		if r >= 'a' && r <= 'z' {
			r -= 32
		}
		buf[n] = byte(r)
		n++
	}
	tt, ok := t.keywords[string(buf[:n])]
	return tt, ok
}

func (t *tokenizerCore) scanComment(commentStart string) bool {
	commentEnd, ok := t.comments2(commentStart)
	if !ok {
		return false
	}
	commentStartLine := t.line
	commentStartSize := len([]rune(commentStart))

	if commentEnd != "" {
		t.advance(commentStartSize, false)
		count := 1
		commentEndSize := len([]rune(commentEnd))
		for !t.end {
			if t.chars(commentEndSize) == commentEnd {
				count--
				if count == 0 {
					break
				}
			}
			t.advance(1, true)
			if t.nestedComments && !t.end && t.chars(commentEndSize) == commentStart {
				t.advance(commentStartSize, false)
				count++
			}
		}
		txt := []rune(t.text())
		// Strip the start delimiter and the part of the end delimiter scanned so far.
		t.comments = append(t.comments, pySlice(txt, commentStartSize, -commentEndSize+1))
		t.advance(commentEndSize-1, false)
	} else {
		for !t.end && t.peek != '\n' && t.peek != '\r' {
			t.advance(1, true)
		}
		txt := []rune(t.text())
		t.comments = append(t.comments, pySlice(txt, commentStartSize, len(txt)))
	}

	if commentStart == t.hintStart && len(t.tokens) > 0 && t.tokensPrecedingHint.Has(t.tokens[len(t.tokens)-1].Type) {
		t.add0(TK_HINT)
	}

	if commentStartLine == t.prevTokenLine {
		last := t.tokens[len(t.tokens)-1]
		last.Comments = append(last.Comments, t.comments...)
		t.comments = nil
		t.prevTokenLine = t.line
	}
	return true
}

func (t *tokenizerCore) comments2(start string) (string, bool) {
	v, ok := t.tokenizerConfig.comments[start]
	return v, ok
}

// pySlice returns string(s[a:b]) with bounds clamped to s and negative bounds counted from the
// end.
func pySlice(s []rune, a, b int) string {
	n := len(s)
	if a < 0 {
		a += n
		if a < 0 {
			a = 0
		}
	}
	if b < 0 {
		b += n
		if b < 0 {
			b = 0
		}
	}
	if a > n {
		a = n
	}
	if b > n {
		b = n
	}
	if a >= b {
		return ""
	}
	return string(s[a:b])
}

func (t *tokenizerCore) scanNumber() {
	if t.char == '0' {
		peek := asciiUpperRune(t.peek)
		if peek == 'B' && t.hasBitStrings {
			t.scanBits()
			return
		} else if peek == 'X' {
			if t.hasHexStrings {
				t.scanHex()
			} else {
				t.add0(TK_NUMBER)
			}
			return
		}
	}

	decimal := false
	scientific := 0
	isUnderscoreSeparated := false
	numberText := ""
	numericLiteral := ""
	var numericType TokenType

	for {
		if isDigitRune(t.peek) {
			end := t.current + 1
			for end < t.size && isDigitRune(t.sql[end]) {
				end++
			}
			t.advance(end-t.current, false)
		} else if t.peek == '.' && !decimal {
			if (len(t.tokens) > 0 && t.tokens[len(t.tokens)-1].Type == TK_PARAMETER) || !t.numbersCanHaveDecimals {
				break
			}
			decimal = true
			t.advance(1, false)
		} else if (t.peek == '-' || t.peek == '+') && scientific == 1 {
			if t.current+1 < t.size && isDigitRune(t.sql[t.current+1]) {
				scientific++
				t.advance(1, false)
			} else {
				break
			}
		} else if asciiUpperRune(t.peek) == 'E' && scientific == 0 &&
			// an exponent starts with a digit, so in 1e_5 the number ends before e_5
			!(t.numbersCanBeUnderscoreSeparated && t.current+1 < t.size && t.sql[t.current+1] == '_') {
			scientific++
			t.advance(1, false)
		} else if t.peek == '_' && t.numbersCanBeUnderscoreSeparated &&
			// an underscore only separates two digits; anywhere else the number ends there
			isDigitRune(t.char) && t.current+1 < t.size && isDigitRune(t.sql[t.current+1]) {
			isUnderscoreSeparated = true
			t.advance(1, false)
		} else if t.peek != noChar && pyIsIdentifierRune(t.peek) {
			numberText = t.text()
			var literalText strings.Builder
			for t.peek != noChar && !pyIsSpaceRune(t.peek) {
				if _, ok := t.single(t.peek); ok {
					break
				}
				literalText.WriteString(string(t.peek))
				t.advance(1, false)
			}
			numericLiteral += literalText.String()
			if lit, ok := t.numericLiterals[pyUpper(numericLiteral)]; ok {
				numericType = t.keywords[lit]
			} else {
				numericType = t.keywords[""]
			}
			if numericType != TK_NONE {
				break
			} else if t.identifiersCanStartWithDigit {
				t.add0(TK_VAR)
				return
			}
			t.advance(-len([]rune(numericLiteral)), false)
			break
		} else {
			break
		}
	}

	if numberText == "" {
		numberText = t.span(t.start, t.current)
	}
	if isUnderscoreSeparated {
		numberText = strings.ReplaceAll(numberText, "_", "")
	}
	t.addS(TK_NUMBER, numberText)
	if numericType != TK_NONE {
		t.addS(TK_DCOLON, "::")
		t.addS(numericType, numericLiteral)
	}
}

func (t *tokenizerCore) scanBits() {
	t.advance(1, false)
	value := t.extractValue()
	if pyIntParse(value, 2) {
		t.addS(TK_BIT_STRING, pySlice([]rune(value), 2, len([]rune(value))))
	} else {
		t.add0(TK_IDENTIFIER)
	}
}

func (t *tokenizerCore) scanHex() {
	t.advance(1, false)
	value := t.extractValue()
	if pyIntParse(value, 16) {
		t.addS(TK_HEX_STRING, pySlice([]rune(value), 2, len([]rune(value))))
	} else {
		t.add0(TK_IDENTIFIER)
	}
}

func (t *tokenizerCore) extractValue() string {
	for {
		ch := t.peek
		if ch == noChar || pyIsSpaceRune(ch) {
			break
		}
		if _, ok := t.single(ch); ok {
			break
		}
		t.advance(1, true)
	}
	return t.text()
}

// pyIntParse reports whether s is an integer in base: optional surrounding whitespace and sign,
// a 0b/0o/0x prefix matching the base (optional), then digits with single underscores allowed
// between them.
func pyIntParse(s string, base int) bool {
	s = pyStrip(s)
	if s == "" {
		return false
	}
	if s[0] == '+' || s[0] == '-' {
		s = s[1:]
	}
	low := strings.ToLower(s)
	if base == 2 && strings.HasPrefix(low, "0b") || base == 16 && strings.HasPrefix(low, "0x") || base == 8 && strings.HasPrefix(low, "0o") {
		s = s[2:]
		if strings.HasPrefix(s, "_") {
			s = s[1:]
		}
	}
	if s == "" {
		return false
	}
	prevUnderscore := true
	for _, r := range s {
		if r == '_' {
			if prevUnderscore {
				return false
			}
			prevUnderscore = true
			continue
		}
		prevUnderscore = false
		var d int
		switch {
		case r >= '0' && r <= '9':
			d = int(r - '0')
		case r >= 'a' && r <= 'z':
			d = int(r-'a') + 10
		case r >= 'A' && r <= 'Z':
			d = int(r-'A') + 10
		default:
			return false
		}
		if d >= base {
			return false
		}
	}
	return !prevUnderscore
}

func (t *tokenizerCore) scanString(start string) bool {
	tokenType := TK_STRING
	base := 0
	var end string

	if e, ok := t.quotes[start]; ok {
		end = e
	} else if f, ok := t.formatStrings[start]; ok {
		end, tokenType = f.end, f.tokenType
		switch tokenType {
		case TK_HEX_STRING:
			base = 16
		case TK_BIT_STRING:
			base = 2
		case TK_HEREDOC_STRING:
			t.advance(1, false)
			var tag string
			if runeString(t.char) == end {
				tag = ""
			} else {
				tag = t.extractString(end, nil, true, !t.heredocTagIsIdentifier, false)
			}
			if tag != "" && t.heredocTagIsIdentifier && (t.end || pyIsDigit(tag) || strings.IndexFunc(tag, pyIsSpaceRune) >= 0) {
				if !t.end {
					t.advance(-1, false)
				}
				t.advance(-len([]rune(tag)), false)
				t.add0(t.heredocStringAlternative)
				return true
			}
			end = start + tag + end
		}
	} else {
		return false
	}

	t.advance(len([]rune(start)), false)
	escapes := t.stringEscapes
	if tokenType == TK_BYTE_STRING {
		escapes = t.byteStringEscapes
	}
	rawString := tokenType == TK_RAW_STRING || tokenType == TK_NATIONAL_RAW_STRING || tokenType == TK_HEREDOC_STRING
	bytesLiteral := tokenType == TK_BYTE_STRING && t.byteStringsAreBytes
	text := t.extractString(end, escapes, rawString, true, bytesLiteral)

	if base != 0 && text != "" {
		if !pyIntParse(text, base) {
			t.fail(fmt.Sprintf("Numeric string contains invalid characters from %d:%d", t.line, t.start))
		}
	}
	t.addS(tokenType, text)
	return true
}

func (t *tokenizerCore) scanIdentifier(identifierEnd string) {
	t.advance(1, false)
	escapes, ok := t.identEscapes[identifierEnd]
	if !ok {
		escapes = t.identifierEscapes.Clone()
		escapes[identifierEnd] = struct{}{}
	}
	text := t.extractString(identifierEnd, escapes, false, true, false)
	t.addS(TK_IDENTIFIER, text)
}

func (t *tokenizerCore) scanVar() {
	for {
		peek := t.peek
		if peek == noChar || pyIsSpaceRune(peek) {
			break
		}
		if !t.varSingleTokens.Has(runeString(peek)) {
			if _, ok := t.single(peek); ok {
				break
			}
		}
		t.advance(1, true)
	}
	tt := TK_VAR
	if !(len(t.tokens) > 0 && t.tokens[len(t.tokens)-1].Type == TK_PARAMETER) {
		if kw, ok := t.keywordType(t.start, t.current); ok {
			tt = kw
		}
	}
	t.add0(tt)
}

// extractString reads a string up to the delimiter, decoding its escape sequences. For a bytes
// literal each char of the result is a byte, so non-ASCII chars are stored as their UTF-8 bytes.
func (t *tokenizerCore) extractString(delimiter string, escapes StrSet, rawString bool, raiseUnmatched bool, bytesLiteral bool) string {
	var text strings.Builder
	delim := []rune(delimiter)
	delimSize := len(delim)
	if escapes == nil {
		escapes = t.stringEscapes
	}
	sql := t.sql

	if delimSize == 1 {
		pos := t.current - 1
		endPos := -1
		for i := pos; i < t.size; i++ {
			if pos < 0 {
				break
			}
			if sql[i] == delim[0] {
				endPos = i
				break
			}
		}
		if endPos != -1 &&
			(endPos+1 >= t.size || sql[endPos+1] != delim[0] || !escapes.Has(delimiter)) &&
			(!(len(t.unescapedSequences) > 0 || escapes.Has("\\")) || !containsRune(sql[pos:endPos], '\\')) {
			newlines := 0
			lastNL := -1
			for i := pos; i < endPos; i++ {
				if sql[i] == '\n' {
					newlines++
					lastNL = i
				}
			}
			if newlines > 0 {
				t.line += newlines
				t.col = endPos - lastNL
			} else {
				t.col += endPos - pos
			}
			t.current = endPos + 1
			t.end = t.current >= t.size
			t.char = sql[endPos]
			if t.end {
				t.peek = noChar
			} else {
				t.peek = sql[t.current]
			}
			if bytesLiteral {
				return utf8BytesAsChars(t.span(pos, endPos))
			}
			return t.span(pos, endPos)
		}
	}

	for {
		backslashEscape := !rawString && t.char == '\\' && escapes.Has("\\")

		// Numeric escapes come first, since they may overlap fixed sequences (e.g. \000 vs \0)
		if backslashEscape && len(t.numericEscapes) > 0 {
			if decoded := t.scanNumericEscape(); decoded != "" {
				text.WriteString(decoded)
				continue
			}
		}

		if !rawString && len(t.unescapedSequences) > 0 && t.peek != noChar && escapes.Has(runeString(t.char)) {
			if seq, ok := t.unescapedSequences[runeString(t.char)+runeString(t.peek)]; ok {
				t.advance(2, false)
				text.WriteString(seq)
				continue
			}
		}

		// An escape that is not decoded drops its backslash where configured, unless the string
		// is unterminated
		if backslashEscape && t.dropUnknownEscapes && t.current+1 < t.size {
			peek := t.peek
			t.advance(1, false)
			t.advance(1, false)
			if bytesLiteral {
				text.WriteString(utf8BytesAsChars(string(peek)))
			} else {
				text.WriteRune(peek)
			}
			continue
		}

		isValidCustomEscape := len(t.escapeFollowChars) > 0 && t.char == '\\' && !t.escapeFollowChars.Has(runeStr(t.peek))

		escapedDelimiter := runeStr(t.peek) == delimiter ||
			(delimSize > 1 && t.peek == delim[0] && t.quotesHas(runeStr(t.peek)))

		if (t.stringEscapesAllowedInRawStrings || !rawString) &&
			escapes.Has(runeStr(t.char)) &&
			(escapedDelimiter || escapes.Has(runeStr(t.peek)) || isValidCustomEscape) &&
			(!t.quotesHas(runeStr(t.char)) || t.char == t.peek) {
			if escapedDelimiter {
				if !rawString {
					text.WriteString(runeStr(t.peek))
				} else {
					text.WriteString(runeStr(t.char) + runeStr(t.peek))
				}
			} else if isValidCustomEscape && t.char != t.peek {
				text.WriteString(runeStr(t.peek))
			} else {
				text.WriteString(runeStr(t.char) + runeStr(t.peek))
			}
			if t.current+1 < t.size {
				t.advance(2, false)
			} else {
				t.fail(fmt.Sprintf("Missing %s from %d:%d", delimiter, t.line, t.current))
			}
		} else {
			if t.chars(delimSize) == delimiter {
				if delimSize > 1 {
					t.advance(delimSize-1, false)
				}
				break
			}
			if t.end {
				if !raiseUnmatched {
					return text.String() + runeStr(t.char)
				}
				t.fail(fmt.Sprintf("Missing %s from %d:%d", delimiter, t.line, t.start))
			}
			current := t.current - 1
			t.advance(1, true)
			if bytesLiteral {
				text.WriteString(utf8BytesAsChars(t.span(current, t.current-1)))
			} else {
				text.WriteString(t.span(current, t.current-1))
			}
		}
	}
	return text.String()
}

// utf8BytesAsChars returns s with each byte of its UTF-8 encoding as one char (U+0000 to U+00FF).
func utf8BytesAsChars(s string) string {
	ascii := true
	for i := range len(s) {
		if s[i] >= utf8.RuneSelf {
			ascii = false
			break
		}
	}
	if ascii {
		return s
	}
	var b strings.Builder
	for i := range len(s) {
		b.WriteRune(rune(s[i]))
	}
	return b.String()
}

// readNumericEscape reads up to maxDigits digits of the given base from start, stopping before the
// value exceeds maxValue. It returns the value and the end index, with -1 as the value when fewer
// than minDigits were read.
func (t *tokenizerCore) readNumericEscape(start, base, minDigits, maxDigits, maxValue int) (int, int) {
	value, end := 0, start
	limit := min(start+maxDigits, t.size)
	for end < limit {
		digit := hexDigitValue(t.sql[end])
		if digit >= base || value*base+digit > maxValue {
			break
		}
		value = value*base + digit
		end++
	}
	if end-start < minDigits {
		return -1, end
	}
	return value, end
}

// hexDigitValue returns the value of a hexadecimal digit, or 16 for any other char.
func hexDigitValue(r rune) int {
	switch {
	case r >= '0' && r <= '9':
		return int(r - '0')
	case r >= 'a' && r <= 'f':
		return int(r-'a') + 10
	case r >= 'A' && r <= 'F':
		return int(r-'A') + 10
	}
	return 16
}

// readByteEscape reads the numeric escape at pos (a backslash) when it encodes a byte (an octal
// escape or a hex one of at most 0xFF), returning the byte and the end index; the byte is -1
// otherwise.
func (t *tokenizerCore) readByteEscape(pos int) (int, int) {
	if pos+1 >= t.size {
		return -1, pos
	}
	peek := t.sql[pos+1]
	isOctal := peek >= '0' && peek <= '7'
	key := peek
	if isOctal {
		key = '0'
	}
	spec, ok := t.numericEscapes[key]
	if !ok || (spec[0] != 8 && spec[3] > 0xFF) {
		return -1, pos
	}
	digitsStart := pos + 2
	if isOctal {
		digitsStart = pos + 1
	}
	value, end := t.readNumericEscape(digitsStart, spec[0], spec[1], spec[2], spec[3])
	if value >= 0 {
		value &= 0xFF
	}
	return value, end
}

// scanNumericEscape decodes the numeric escape (per numericEscapes) that starts at the current
// char, a backslash, and moves past it. It returns "" when nothing is decoded: an escape with too
// few digits, a lone UTF-16 surrogate (unless loneSurrogateReplacement is set) or an escape at
// the end of the input, which is left to the unterminated string check. With
// numericEscapesAreBytes, a byte escape that starts a valid UTF-8 sequence of byte escapes (e.g.
// \xC3\xA9) decodes to that one character, any other byte above 0x7F stays that raw byte, and
// octal values wrap around to a byte (\541 is \x61).
func (t *tokenizerCore) scanNumericEscape() string {
	start := t.current
	peek := t.peek

	// Octal escapes start with a digit (e.g. \101), the others with a letter (e.g. \u0041)
	isOctal := peek >= '0' && peek <= '7'
	key := peek
	if isOctal {
		key = '0'
	}
	spec, ok := t.numericEscapes[key]
	if !ok {
		return ""
	}
	base, minDigits, maxDigits, maxValue := spec[0], spec[1], spec[2], spec[3]
	digitsStart := start + 1
	if isOctal {
		digitsStart = start
	}
	value, end := t.readNumericEscape(digitsStart, base, minDigits, maxDigits, maxValue)

	isByte := t.numericEscapesAreBytes && (base == 8 || maxValue <= 0xFF)
	if isByte && value >= 0 {
		value &= 0xFF
	}

	// A UTF-16 surrogate is only valid as a high half (D800-DBFF) followed by an escaped low half
	// (DC00-DFFF), e.g. \uD83D\uDE00 is U+1F600
	if value >= 0xD800 && value <= 0xDFFF {
		low, lowEnd := -1, end
		if value <= 0xDBFF && end+1 < t.size && t.sql[end] == '\\' && t.sql[end+1] == peek {
			low, lowEnd = t.readNumericEscape(end+2, base, minDigits, maxDigits, maxValue)
		}

		if low >= 0xDC00 && low <= 0xDFFF {
			value = 0x10000 + ((value - 0xD800) << 10) + (low - 0xDC00)
			end = lowEnd
		} else if t.loneSurrogateReplacement != "" && end < t.size {
			t.advance(end-start+1, false)
			return t.loneSurrogateReplacement
		} else {
			value = -1
		}
	}

	// Decode one UTF-8 character at a time. Only 0xC2..0xF4 can start a multi-byte character.
	if isByte && value >= 0xC2 && value <= 0xF4 {
		run := []byte{byte(value)}
		runEnd := end
		byteCount := 4
		if value < 0xE0 {
			byteCount = 2
		} else if value < 0xF0 {
			byteCount = 3
		}
		for len(run) < byteCount && runEnd < t.size && t.sql[runEnd] == '\\' {
			b, bEnd := t.readByteEscape(runEnd)
			if b < 0x80 || b > 0xBF {
				break
			}
			run = append(run, byte(b))
			runEnd = bEnd
		}

		if len(run) == byteCount && utf8.Valid(run) && runEnd < t.size {
			t.advance(runEnd-start+1, false)
			return string(run)
		}
	}

	if value >= 0 && end < t.size {
		t.advance(end-start+1, false)
		if isByte && value > 0x7F {
			// Not part of a UTF-8 character: keep the byte itself, which the generator writes
			// back as a byte escape. Decoding it as the code point (\xFF as ÿ) changes the bytes.
			return string([]byte{byte(value)})
		}
		return string(rune(value))
	}
	return ""
}

func (t *tokenizerCore) quotesHas(s string) bool {
	_, ok := t.quotes[s]
	return ok
}

func runeStr(r rune) string {
	if r == noChar {
		return ""
	}
	return runeString(r)
}

func containsRune(s []rune, r rune) bool {
	for _, x := range s {
		if x == r {
			return true
		}
	}
	return false
}
