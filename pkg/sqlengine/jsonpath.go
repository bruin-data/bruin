package sqlengine

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// Port of sqlglot/jsonpath.py.

// jsonPathTokenizer mirrors an instance of a JSONPathTokenizer class: the resolved tokenizer
// settings plus the class-level VAR_TOKENS used by the JSON path parser.
type jsonPathTokenizer struct {
	*TokenizerSettings
	cfg        *tokenizerConfig
	VAR_TOKENS TokenSet
}

// baseJSONPathTokenizerSettings mirrors the class attributes of sqlglot.jsonpath.JSONPathTokenizer
// (including the ones inherited from tokens.Tokenizer and those derived by __init_subclass__).
func baseJSONPathTokenizerSettings() *TokenizerSettings {
	return &TokenizerSettings{
		BIT_STRINGS:  nil,
		BYTE_STRINGS: nil,
		// "BYTE_STRING_ESCAPES" is not in the class __dict__, so it is copied from STRING_ESCAPES.
		BYTE_STRING_ESCAPES:        []string{"\\"},
		COMMANDS:                   newTokenSet(TK_COMMAND, TK_EXECUTE, TK_FETCH, TK_RENAME, TK_SHOW),
		COMMAND_PREFIX_TOKENS:      newTokenSet(TK_SEMICOLON, TK_BEGIN),
		COMMENTS:                   [][2]string{{"--", ""}, {"/*", "*/"}},
		ESCAPE_FOLLOW_CHARS:        nil,
		HEREDOC_STRINGS:            nil,
		HEREDOC_STRING_ALTERNATIVE: TK_VAR,
		HEREDOC_TAG_IS_IDENTIFIER:  false,
		HEX_STRINGS:                nil,
		HINT_START:                 "/*+",
		IDENTIFIERS:                [][2]string{{"\"", "\""}},
		IDENTIFIER_ESCAPES:         []string{"\\"},
		KEYWORDS: map[string]TokenType{
			"..": TK_DOT,
		},
		NESTED_COMMENTS:           true,
		NUMBERS_CAN_HAVE_DECIMALS: false,
		NUMERIC_LITERALS:          map[string]string{},
		QUOTES:                    []string{"'"},
		RAW_STRINGS:               nil,
		SINGLE_TOKENS: map[string]TokenType{
			"(":  TK_L_PAREN,
			")":  TK_R_PAREN,
			"[":  TK_L_BRACKET,
			"]":  TK_R_BRACKET,
			":":  TK_COLON,
			",":  TK_COMMA,
			"-":  TK_DASH,
			".":  TK_DOT,
			"?":  TK_PLACEHOLDER,
			"@":  TK_PARAMETER,
			"'":  TK_QUOTE,
			"\"": TK_QUOTE,
			"$":  TK_DOLLAR,
			"*":  TK_STAR,
		},
		STRING_ESCAPES:                        []string{"\\"},
		STRING_ESCAPES_ALLOWED_IN_RAW_STRINGS: true,
		TOKENS_PRECEDING_HINT:                 newTokenSet(TK_SELECT, TK_INSERT, TK_UPDATE, TK_DELETE),
		UNICODE_STRINGS:                       nil,
		VAR_SINGLE_TOKENS:                     newStrSet(),
		_BYTE_STRING_ESCAPES:                  newStrSet("\\"),
		// HINT_START is not in KEYWORDS, so no "/*+" entry.
		_COMMENTS:            map[string]string{"--": "", "/*": "*/", "{#": "#}"},
		_ESCAPE_FOLLOW_CHARS: newStrSet(),
		_FORMAT_STRINGS: map[string]formatString{
			"n'": {"'", TK_NATIONAL_STRING},
			"N'": {"'", TK_NATIONAL_STRING},
		},
		_IDENTIFIERS:        map[string]string{"\"": "\""},
		_IDENTIFIER_ESCAPES: newStrSet("\\"),
		_QUOTES:             map[string]string{"'": "'"},
		_STRING_ESCAPES:     newStrSet("\\"),
	}
}

// newJSONPathTokenizerSettings resolves the JSONPathTokenizer class of a dialect (the Dialect
// metaclass inherits it from the first base class unless the dialect defines its own):
//
//   - BigQuery:   VAR_TOKENS = {VAR, DASH, NUMBER}
//   - Hive:       VAR_TOKENS = {VAR, DASH} (inherited by Spark2, Spark, Databricks)
//   - Databricks: Hive's, plus IDENTIFIERS = ["`", '"']
//   - Snowflake:  SINGLE_TOKENS without "$"
func newJSONPathTokenizerSettings(d *Dialect) (*TokenizerSettings, TokenSet) {
	s := baseJSONPathTokenizerSettings()
	varTokens := newTokenSet(TK_VAR)
	switch {
	case d.Is("databricks"):
		varTokens = varTokens.With(TK_DASH)
		s.IDENTIFIERS = [][2]string{{"`", "`"}, {"\"", "\""}}
		s._IDENTIFIERS = map[string]string{"`": "`", "\"": "\""}
	case d.Is("hive"):
		varTokens = varTokens.With(TK_DASH)
	case d.Is("bigquery"):
		varTokens = varTokens.With(TK_DASH, TK_NUMBER)
	case d.Is("snowflake"):
		delete(s.SINGLE_TOKENS, "$")
	}
	return s, varTokens
}

// jsonPathTokenizers caches the resolved JSONPathTokenizer per dialect name.
var jsonPathTokenizers sync.Map

// jsonpathTokenizer mirrors Dialect.jsonpath_tokenizer().
func (d *Dialect) jsonpathTokenizer() *jsonPathTokenizer {
	if v, ok := jsonPathTokenizers.Load(d.Name); ok {
		return v.(*jsonPathTokenizer)
	}
	s, varTokens := newJSONPathTokenizerSettings(d)
	jt := &jsonPathTokenizer{TokenizerSettings: s, cfg: newTokenizerConfig(s, d.S), VAR_TOKENS: varTokens}
	v, _ := jsonPathTokenizers.LoadOrStore(d.Name, jt)
	return v.(*jsonPathTokenizer)
}

// tokenize mirrors JSONPathTokenizer.tokenize.
func (jt *jsonPathTokenizer) tokenize(path string) ([]*Token, error) {
	return newTokenizerCore(jt.cfg).tokenize(path)
}

// jsonPathIndexError mirrors the Python IndexError that jsonpath.parse can raise for
// truncated filters/scripts such as "$[?".
type jsonPathIndexError struct{}

func (jsonPathIndexError) Error() string { return "list index out of range" }

// parseJSONPath mirrors sqlglot.jsonpath.parse: it takes in a JSON path string and parses it
// into a JSONPath expression. Errors are *ParseError, *TokenError, *ValueError (int() failures)
// or jsonPathIndexError.
func parseJSONPath(path string, d *Dialect) (result *Expr, err error) {
	if d == nil {
		d = prototype("")
	}
	jsonpathTokenizer := d.jsonpathTokenizer()
	tokens, err := jsonpathTokenizer.tokenize(path)
	if err != nil {
		return nil, err
	}

	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case *ParseError:
				result, err = nil, x
			case *ValueError:
				result, err = nil, x
			case jsonPathIndexError:
				result, err = nil, x
			default:
				panic(r)
			}
		}
	}()

	pathRunes := []rune(path)
	size := len(tokens)
	i := 0

	// curr mirrors _curr(); ok=false means None.
	curr := func() (TokenType, bool) {
		if i < size {
			return tokens[i].Type, true
		}
		return TK_NONE, false
	}
	prev := func() *Token { return tokens[i-1] }
	advance := func() *Token {
		i++
		return prev()
	}
	errorMsg := func(msg string) string { return fmt.Sprintf("%s at index %d: %s", msg, i, path) }
	match := func(tokenType TokenType) *Token {
		if tt, ok := curr(); ok && tt == tokenType {
			return advance()
		}
		return nil
	}
	matchRaise := func(tokenType TokenType) *Token {
		if tt, ok := curr(); ok && tt == tokenType {
			return advance()
		}
		panic(&ParseError{Msg: errorMsg("Expected " + tokenType.String())})
	}
	matchSet := func(types TokenSet) *Token {
		if tt, ok := curr(); ok && types.Has(tt) {
			return advance()
		}
		return nil
	}

	var parseBracket func() *Expr

	// parseLiteral returns a string, an int, a JSONPathPart *Expr or false.
	parseLiteral := func() any {
		token := match(TK_STRING)
		if token == nil {
			token = match(TK_IDENTIFIER)
		}
		if token != nil {
			return token.Text
		}
		if match(TK_STAR) != nil {
			return New(KJSONPathWildcard)
		}
		if match(TK_PLACEHOLDER) != nil || match(TK_L_PAREN) != nil {
			script := prev().Text == "("
			start := i

			for {
				if match(TK_L_BRACKET) != nil {
					parseBracket() // nested call which we can throw away
				}
				if tt, ok := curr(); !ok || tt == TK_R_BRACKET {
					break
				}
				advance()
			}

			var end int
			if i < size {
				end = int(tokens[i].End)
			} else {
				end = int(tokens[size-1].End)
			}
			if start >= size {
				panic(jsonPathIndexError{})
			}
			text := pySlice(pathRunes, int(tokens[start].Start), end)
			if script {
				return New(KJSONPathScript, "this", text)
			}
			return New(KJSONPathFilter, "this", text)
		}

		number := ""
		if match(TK_DASH) != nil {
			number = "-"
		}

		if token := match(TK_NUMBER); token != nil {
			number += token.Text
		}

		if number != "" {
			return jsonPathPyInt(number)
		}

		return false
	}

	parseSlice := func() any {
		start := parseLiteral()
		var end, step any
		if match(TK_COLON) != nil {
			end = parseLiteral()
		}
		if match(TK_COLON) != nil {
			step = parseLiteral()
		}

		if end == nil && step == nil {
			return start
		}

		return New(KJSONPathSlice, "start", start, "end", end, "step", step)
	}

	parseBracket = func() *Expr {
		literal := parseSlice()

		var node *Expr
		_, isStr := literal.(string)
		if isStr || !jsonPathIsFalse(literal) {
			indexes := []any{literal}
			for match(TK_COMMA) != nil {
				literal = parseSlice()

				if truthy(literal) {
					indexes = append(indexes, literal)
				}
			}

			if len(indexes) == 1 {
				le, isExpr := literal.(*Expr)
				if _, ok := literal.(string); ok {
					node = New(KJSONPathKey, "this", indexes[0])
				} else if isExpr && le.IsA(KJSONPathPart) && le.IsA(KJSONPathScript, KJSONPathFilter) {
					node = New(KJSONPathSelector, "this", indexes[0])
				} else {
					node = New(KJSONPathSubscript, "this", indexes[0])
				}
			} else {
				node = newJSONPathUnion(indexes)
			}
		} else {
			panic(&ParseError{Msg: errorMsg("Cannot have empty segment")})
		}

		matchRaise(TK_R_BRACKET)

		return node
	}

	// parseVarText consumes & returns the text for a var. In BigQuery it's valid to have a key
	// with spaces in it, e.g JSON_QUERY(..., '$. a b c ') should produce a single
	// JSONPathKey(' a b c '). This is done by merging "consecutive" vars until a key separator
	// is found (dot, colon etc) or the path string is exhausted.
	parseVarText := func() string {
		prevIndex := i - 2

		for matchSet(jsonpathTokenizer.VAR_TOKENS) != nil {
		}

		start := 0
		if prevIndex >= 0 {
			start = int(tokens[prevIndex].End) + 1
		}

		if i >= len(tokens) {
			// This key is the last token for the path, so it's text is the remaining path
			return pySlice(pathRunes, start, len(pathRunes))
		}
		return pySlice(pathRunes, start, int(tokens[i].Start))
	}

	// We canonicalize the JSON path AST so that it always starts with a
	// "root" element, so paths like "field" will be generated as "$.field"
	match(TK_DOLLAR)
	expressions := []*Expr{New(KJSONPathRoot)}

	for i < size {
		if match(TK_DOT) != nil || match(TK_COLON) != nil {
			recursive := prev().Text == ".."

			var value any
			if matchSet(jsonpathTokenizer.VAR_TOKENS) != nil {
				value = parseVarText()
			} else if match(TK_IDENTIFIER) != nil {
				value = prev().Text
			} else if match(TK_STAR) != nil {
				value = New(KJSONPathWildcard)
			} else {
				value = nil
			}

			if recursive {
				expressions = append(expressions, New(KJSONPathRecursive, "this", value))
			} else if truthy(value) {
				expressions = append(expressions, New(KJSONPathKey, "this", value))
			} else if !d.S.JSON_PATH_SINGLE_DOT_IS_WILDCARD {
				panic(&ParseError{Msg: errorMsg("Expected key name or * after DOT")})
			}
		} else if match(TK_L_BRACKET) != nil {
			expressions = append(expressions, parseBracket())
		} else if matchSet(jsonpathTokenizer.VAR_TOKENS) != nil {
			expressions = append(expressions, New(KJSONPathKey, "this", parseVarText()))
		} else if match(TK_IDENTIFIER) != nil {
			expressions = append(expressions, New(KJSONPathKey, "this", prev().Text))
		} else if match(TK_STAR) != nil {
			expressions = append(expressions, New(KJSONPathWildcard))
		} else {
			panic(&ParseError{Msg: errorMsg("Unexpected " + tokens[i].Type.String())})
		}
	}

	return New(KJSONPath, "expressions", expressions), nil
}

// jsonPathIsFalse mirrors `value is False`.
func jsonPathIsFalse(v any) bool {
	b, ok := v.(bool)
	return ok && !b
}

// jsonPathPyInt mirrors Python's int(text) for the numbers produced by the JSON path parser.
func jsonPathPyInt(text string) int {
	if !isPyInt(text) {
		panic(&ValueError{Msg: "invalid literal for int() with base 10: " + pyRepr(text)})
	}
	n, err := strconv.Atoi(strings.ReplaceAll(pyStrip(text), "_", ""))
	if err != nil {
		// Python ints are unbounded; Go ints are not.
		panic(&ValueError{Msg: "int too large to convert: " + pyRepr(text)})
	}
	return n
}

// newJSONPathUnion builds JSONPathUnion(expressions=indexes). The Python list may mix str, int and
// JSONPathPart values: it is stored as []*Expr or []string when homogeneous, and as []any
// otherwise (in which case the parents of the expression items are set like Expr._set_parent).
func newJSONPathUnion(indexes []any) *Expr {
	allExprs, allStrings := true, true
	for _, x := range indexes {
		if _, ok := x.(*Expr); !ok {
			allExprs = false
		}
		if _, ok := x.(string); !ok {
			allStrings = false
		}
	}
	switch {
	case allExprs:
		list := make([]*Expr, len(indexes))
		for i, x := range indexes {
			list[i] = x.(*Expr)
		}
		return New(KJSONPathUnion, "expressions", list)
	case allStrings:
		list := make([]string, len(indexes))
		for i, x := range indexes {
			list[i] = x.(string)
		}
		return New(KJSONPathUnion, "expressions", list)
	}
	list := append([]any{}, indexes...)
	u := New(KJSONPathUnion, "expressions", list)
	for i, x := range list {
		if xe, ok := x.(*Expr); ok && xe != nil {
			xe.parent = u
			xe.argKey = "expressions"
			xe.index = int32(i)
		}
	}
	return u
}

// jsonPathUnionItems returns the items of a JSONPathUnion's expressions list (str, int or
// JSONPathPart values), whatever their Go representation (see newJSONPathUnion).
func jsonPathUnionItems(e *Expr) []any {
	switch v := e.Arg("expressions").(type) {
	case []*Expr:
		out := make([]any, len(v))
		for i, x := range v {
			out[i] = x
		}
		return out
	case []string:
		out := make([]any, len(v))
		for i, x := range v {
			out[i] = x
		}
		return out
	case []any:
		return v
	}
	return nil
}

// jsonPathFormat mirrors f"{value}" for the values stored in JSONPathPart arguments.
func jsonPathFormat(v any) string {
	switch x := v.(type) {
	case nil:
		return "None"
	case string:
		return x
	case int:
		return strconv.Itoa(x)
	case bool:
		if x {
			return "True"
		}
		return "False"
	case *Expr:
		if x == nil {
			return "None"
		}
		return exprSQL(x)
	}
	return fmt.Sprint(v)
}

// JSON_PATH_PART_TRANSFORMS mirrors sqlglot.jsonpath.JSON_PATH_PART_TRANSFORMS.
var JSON_PATH_PART_TRANSFORMS = map[Kind]GenFunc{
	KJSONPathFilter: func(g *Generator, e *Expr) string { return "?" + jsonPathFormat(e.Arg("this")) },
	KJSONPathKey:    func(g *Generator, e *Expr) string { return g.jsonpathkeySQL(e) },
	KJSONPathRecursive: func(g *Generator, e *Expr) string {
		// f"..{e.this or ''}"
		this := e.Arg("this")
		if !truthy(this) {
			return ".."
		}
		return ".." + jsonPathFormat(this)
	},
	KJSONPathRoot:     func(g *Generator, e *Expr) string { return "$" },
	KJSONPathScript:   func(g *Generator, e *Expr) string { return "(" + jsonPathFormat(e.Arg("this")) },
	KJSONPathSelector: func(g *Generator, e *Expr) string { return "[" + g.jsonPathPart(e.Arg("this")) + "]" },
	KJSONPathSlice: func(g *Generator, e *Expr) string {
		var parts []string
		for _, p := range []any{e.Arg("start"), e.Arg("end"), e.Arg("step")} {
			if p == nil {
				continue
			}
			if jsonPathIsFalse(p) {
				parts = append(parts, "")
			} else {
				parts = append(parts, g.jsonPathPart(p))
			}
		}
		return strings.Join(parts, ":")
	},
	KJSONPathSubscript: func(g *Generator, e *Expr) string { return g.jsonpathsubscriptSQL(e) },
	KJSONPathUnion: func(g *Generator, e *Expr) string {
		items := jsonPathUnionItems(e)
		parts := make([]string, len(items))
		for i, p := range items {
			parts[i] = g.jsonPathPart(p)
		}
		return "[" + strings.Join(parts, ",") + "]"
	},
	KJSONPathWildcard: func(g *Generator, e *Expr) string { return "*" },
}

// allJSONPathPartKinds lists the keys of JSON_PATH_PART_TRANSFORMS.
var allJSONPathPartKinds = []Kind{
	KJSONPathFilter, KJSONPathKey, KJSONPathRecursive, KJSONPathRoot, KJSONPathScript,
	KJSONPathSelector, KJSONPathSlice, KJSONPathSubscript, KJSONPathUnion, KJSONPathWildcard,
}

// ALL_JSON_PATH_PARTS mirrors sqlglot.jsonpath.ALL_JSON_PATH_PARTS.
var ALL_JSON_PATH_PARTS = newKindSet(allJSONPathPartKinds...)

// toJSONPath mirrors Dialect.to_json_path (dispatching to dialect overrides).
func (d *Dialect) toJSONPath(path *Expr) *Expr {
	if d.hooks != nil && d.hooks.toJSONPath != nil {
		return d.hooks.toJSONPath(d, path)
	}
	return d.baseToJSONPath(path)
}

// baseToJSONPath mirrors Dialect.to_json_path.
func (d *Dialect) baseToJSONPath(path *Expr) *Expr {
	if path.IsA(KLiteral) {
		pathText := path.Name()
		if path.IsNumber() {
			pathText = "[" + pathText + "]"
		}
		parsed, err := parseJSONPath(pathText, d)
		if err == nil {
			return parsed
		}
		if _, ok := err.(*ParseError); !ok {
			// Only ParseError is caught; anything else propagates.
			panic(err)
		}
		// sqlglot logs "Invalid JSON path syntax" here when STRICT_JSON_PATH_SYNTAX is set
		// and the path does not start with "lax"/"strict"; logging is dropped.
	}

	return path
}

// setupDuckDBToJSONPath installs DuckDB.to_json_path.
func setupDuckDBToJSONPath(d *Dialect) {
	d.hooks.toJSONPath = func(d *Dialect, path *Expr) *Expr {
		if path.IsA(KLiteral) {
			// DuckDB also supports the JSON pointer syntax, where every path starts with a `/`.
			// Additionally, it allows accessing the back of lists using the `[#-i]` syntax.
			// This check ensures we'll avoid trying to parse these as JSON paths, which can
			// either result in a noisy warning or in an invalid representation of the path.
			pathText := path.Name()
			if strings.HasPrefix(pathText, "/") || strings.Contains(pathText, "[#") {
				return path
			}
		}

		return d.baseToJSONPath(path)
	}
}
