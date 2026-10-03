package sqlengine

import (
	"fmt"
	"strings"
)

// Token mirrors sqlglot.tokens.Token. Positions are in Unicode code points, like Python.
type Token struct {
	Type     TokenType
	Text     string
	Line     int
	Col      int
	Start    int
	End      int
	Comments []string
}

// String mirrors Token.__repr__ (comments rendered as a Python list repr).
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

// TokenError mirrors sqlglot.errors.TokenError.
type TokenError struct{ Msg string }

func (e *TokenError) Error() string { return e.Msg }

// tokenizerConfig is the fully-resolved configuration of a tokenizer (TokenizerCore args).
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
	}
	for k, v := range s.SINGLE_TOKENS {
		r := []rune(k)
		if len(r) == 1 {
			c.singleTokens[r[0]] = v
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
	}
	c.keywordTrie = buildKeywordTrie(s)
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

// tokenizerCore mirrors sqlglot.tokenizer_core.TokenizerCore.
type tokenizerCore struct {
	*tokenizerConfig
	sql           []rune
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

func newTokenizerCore(c *tokenizerConfig) *tokenizerCore {
	return &tokenizerCore{tokenizerConfig: c}
}

func (t *tokenizerCore) reset() {
	t.sql = nil
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

// tokenize mirrors TokenizerCore.tokenize.
func (t *tokenizerCore) tokenize(sql string) (tokens []*Token, err error) {
	t.reset()
	t.sql = []rune(sql)
	t.size = len(t.sql)

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

func (t *tokenizerCore) chars(size int) string {
	if size == 1 {
		if t.char == noChar {
			return ""
		}
		return string(t.char)
	}
	start := t.current - 1
	end := start + size
	if end <= t.size {
		if start < 0 {
			return string(t.sql[:end])
		}
		return string(t.sql[start:end])
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

func (t *tokenizerCore) text() string { return string(t.sql[t.start:t.current]) }

func (t *tokenizerCore) add(tt TokenType, text *string) {
	t.prevTokenLine = t.line
	if len(t.comments) > 0 && tt == TK_SEMICOLON && len(t.tokens) > 0 {
		last := t.tokens[len(t.tokens)-1]
		last.Comments = append(last.Comments, t.comments...)
		t.comments = nil
	}
	var txt string
	if text == nil {
		txt = string(t.sql[t.start:t.current])
	} else {
		txt = *text
	}
	comments := t.comments
	if comments == nil {
		comments = []string{}
	}
	t.tokens = append(t.tokens, &Token{
		Type:     tt,
		Text:     txt,
		Line:     t.line,
		Col:      t.col,
		Start:    t.start,
		End:      t.current - 1,
		Comments: comments,
	})
	t.comments = nil

	if t.commands.Has(tt) && t.peek != ';' &&
		(len(t.tokens) == 1 || t.commandPrefixTokens.Has(t.tokens[len(t.tokens)-2].Type)) {
		start := t.current
		n := len(t.tokens)
		t.scan(true)
		t.tokens = t.tokens[:n]
		s := pyStrip(string(t.sql[start:t.current]))
		if s != "" {
			t.add(TK_STRING, &s)
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
	var word string
	hasWord := false
	chars := string(t.char)
	char := t.char
	prevSpace := false
	skip := false
	tr := t.keywordTrie
	_, singleToken := t.singleTokens[char]

	for chars != "" {
		if !skip {
			sub := tr.get(string(asciiUpperRune(char)))
			if sub == nil {
				break
			}
			tr = sub
			if tr.end {
				word = chars
				hasWord = true
			}
		}
		end := t.current + size
		size++
		if end < t.size {
			char = t.sql[end]
			if !singleToken {
				_, singleToken = t.singleTokens[char]
			}
			isSpace := pyIsSpaceRune(char)
			if !isSpace || !prevSpace {
				if isSpace {
					char = ' '
				}
				chars += string(char)
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

	if hasWord {
		if t.scanString(word) {
			return
		}
		if t.scanComment(word) {
			return
		}
		if prevSpace || singleToken || char == noChar {
			t.advance(size-1, false)
			w := pyUpper(word)
			tt, ok := t.keywords[w]
			if !ok {
				t.fail("KeyError")
			}
			t.add(tt, &w)
			return
		}
	}

	if tt, ok := t.singleTokens[t.char]; ok {
		t.add(tt, strp(string(t.char)))
		return
	}
	t.scanVar()
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
		// self._text[comment_start_size : -comment_end_size + 1]
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
		t.add(TK_HINT, nil)
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

// pySlice mirrors Python slicing s[a:b] with clamping and negative indices.
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
		if peek == 'B' {
			if t.hasBitStrings {
				t.scanBits()
			} else {
				t.add(TK_NUMBER, nil)
			}
			return
		} else if peek == 'X' {
			if t.hasHexStrings {
				t.scanHex()
			} else {
				t.add(TK_NUMBER, nil)
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
		} else if asciiUpperRune(t.peek) == 'E' && scientific == 0 {
			scientific++
			t.advance(1, false)
		} else if t.peek == '_' && t.numbersCanBeUnderscoreSeparated {
			isUnderscoreSeparated = true
			t.advance(1, false)
		} else if t.peek != noChar && pyIsIdentifierRune(t.peek) {
			numberText = t.text()
			for t.peek != noChar && !pyIsSpaceRune(t.peek) {
				if _, ok := t.singleTokens[t.peek]; ok {
					break
				}
				numericLiteral += string(t.peek)
				t.advance(1, false)
			}
			if lit, ok := t.numericLiterals[pyUpper(numericLiteral)]; ok {
				numericType = t.keywords[lit]
			} else {
				numericType = t.keywords[""]
			}
			if numericType != TK_NONE {
				break
			} else if t.identifiersCanStartWithDigit {
				t.add(TK_VAR, nil)
				return
			}
			t.advance(-len([]rune(numericLiteral)), false)
			break
		} else {
			break
		}
	}

	if numberText == "" {
		numberText = string(t.sql[t.start:t.current])
	}
	if isUnderscoreSeparated {
		numberText = strings.ReplaceAll(numberText, "_", "")
	}
	t.add(TK_NUMBER, &numberText)
	if numericType != TK_NONE {
		t.add(TK_DCOLON, strp("::"))
		t.add(numericType, &numericLiteral)
	}
}

func (t *tokenizerCore) scanBits() {
	t.advance(1, false)
	value := t.extractValue()
	if pyIntParse(value, 2) {
		t.add(TK_BIT_STRING, strp(pySlice([]rune(value), 2, len([]rune(value)))))
	} else {
		t.add(TK_IDENTIFIER, nil)
	}
}

func (t *tokenizerCore) scanHex() {
	t.advance(1, false)
	value := t.extractValue()
	if pyIntParse(value, 16) {
		t.add(TK_HEX_STRING, strp(pySlice([]rune(value), 2, len([]rune(value)))))
	} else {
		t.add(TK_IDENTIFIER, nil)
	}
}

func (t *tokenizerCore) extractValue() string {
	for {
		ch := t.peek
		if ch == noChar || pyIsSpaceRune(ch) {
			break
		}
		if _, ok := t.singleTokens[ch]; ok {
			break
		}
		t.advance(1, true)
	}
	return t.text()
}

// pyIntParse reports whether Python's int(s, base) would succeed.
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
			if string(t.char) == end {
				tag = ""
			} else {
				tag = t.extractString(end, nil, true, !t.heredocTagIsIdentifier)
			}
			if tag != "" && t.heredocTagIsIdentifier && (t.end || pyIsDigit(tag) || strings.IndexFunc(tag, pyIsSpaceRune) >= 0) {
				if !t.end {
					t.advance(-1, false)
				}
				t.advance(-len([]rune(tag)), false)
				t.add(t.heredocStringAlternative, nil)
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
	text := t.extractString(end, escapes, tokenType == TK_RAW_STRING, true)

	if base != 0 && text != "" {
		if !pyIntParse(text, base) {
			t.fail(fmt.Sprintf("Numeric string contains invalid characters from %d:%d", t.line, t.start))
		}
	}
	t.add(tokenType, &text)
	return true
}

func (t *tokenizerCore) scanIdentifier(identifierEnd string) {
	t.advance(1, false)
	escapes := t.identifierEscapes.Clone()
	escapes[identifierEnd] = struct{}{}
	text := t.extractString(identifierEnd, escapes, false, true)
	t.add(TK_IDENTIFIER, &text)
}

func (t *tokenizerCore) scanVar() {
	for {
		peek := t.peek
		if peek == noChar || pyIsSpaceRune(peek) {
			break
		}
		if !t.varSingleTokens.Has(string(peek)) {
			if _, ok := t.singleTokens[peek]; ok {
				break
			}
		}
		t.advance(1, true)
	}
	tt := TK_VAR
	if !(len(t.tokens) > 0 && t.tokens[len(t.tokens)-1].Type == TK_PARAMETER) {
		if kw, ok := t.keywords[pyUpper(string(t.sql[t.start:t.current]))]; ok {
			tt = kw
		}
	}
	t.add(tt, nil)
}

func (t *tokenizerCore) extractString(delimiter string, escapes StrSet, rawString bool, raiseUnmatched bool) string {
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
			return string(sql[pos:endPos])
		}
	}

	for {
		if !rawString && len(t.unescapedSequences) > 0 && t.peek != noChar && escapes.Has(string(t.char)) {
			if seq, ok := t.unescapedSequences[string(t.char)+string(t.peek)]; ok {
				t.advance(2, false)
				text.WriteString(seq)
				continue
			}
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
			text.WriteString(string(sql[current : t.current-1]))
		}
	}
	return text.String()
}

func (t *tokenizerCore) quotesHas(s string) bool {
	_, ok := t.quotes[s]
	return ok
}

func runeStr(r rune) string {
	if r == noChar {
		return ""
	}
	return string(r)
}

func containsRune(s []rune, r rune) bool {
	for _, x := range s {
		if x == r {
			return true
		}
	}
	return false
}
