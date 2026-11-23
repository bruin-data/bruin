package sqlengine

import "sync"

var (
	athenaTrinoTokOnce sync.Once
	athenaTrinoTok     *tokenizerConfig
)

// athenaTrinoTokenizer returns the Trino tokenizer extended with UNLOAD as a command keyword.
func athenaTrinoTokenizer() *tokenizerConfig {
	athenaTrinoTokOnce.Do(func() {
		trino := prototypeLocked2("trino")
		s := *trino.T
		kw := make(map[string]TokenType, len(s.KEYWORDS)+1)
		for k, v := range s.KEYWORDS {
			kw[k] = v
		}
		kw["UNLOAD"] = TK_COMMAND
		s.KEYWORDS = kw
		athenaTrinoTok = newTokenizerConfig(&s, trino.S)
	})
	return athenaTrinoTok
}

// athenaHiveTokenStream prepends the TK_HIVE_TOKEN_STREAM marker that routes tokens to the Hive parser.
func athenaHiveTokenStream(tokens []*Token) []*Token {
	return append([]*Token{{Type: TK_HIVE_TOKEN_STREAM, Text: "", Line: 1, Col: 1, Comments: []string{}}}, tokens...)
}

// setupAthenaTokenizer installs Athena's tokenize hook, which routes each statement to the
// Hive or Trino tokenizer.
//
// Statements are tokenized with the Trino tokenizer first; only those that may be Hive DDL are
// tokenized again with the combined Hive and Trino rules to decide. Since the combined rules
// reject literals that are valid in only one engine ('\' in Trino, "a\"b" in Hive), a failure
// there falls back to whichever engine's own tokens route to that engine.
func setupAthenaTokenizer(d *Dialect) {
	d.hooks.tokenize = func(d *Dialect, sql string) ([]*Token, error) {
		trinoTokens, trinoErr := newTokenizerCore(athenaTrinoTokenizer()).tokenize(sql)
		if trinoErr == nil && !athenaMayBeHive(trinoTokens) {
			return trinoTokens, nil
		}

		hive := prototypeLocked2("hive")
		tokens, err := newTokenizerCore(d.tok).tokenize(sql)
		if err != nil {
			if trinoErr == nil && !tokenizeAsHive(trinoTokens) {
				return trinoTokens, nil
			}

			if hiveTokens, hiveErr := newTokenizerCore(hive.tok).tokenize(sql); hiveErr == nil && tokenizeAsHive(hiveTokens) {
				return athenaHiveTokenStream(hiveTokens), nil
			}
			return nil, err
		}

		if tokenizeAsHive(tokens) {
			hiveTokens, err := newTokenizerCore(hive.tok).tokenize(sql)
			if err != nil {
				return nil, err
			}
			return athenaHiveTokenStream(hiveTokens), nil
		}

		if trinoErr != nil {
			return nil, trinoErr
		}
		return trinoTokens, nil
	}
}

// prototypeLocked2 fetches a prototype at tokenization time (outside dialect setup).
func prototypeLocked2(name string) *Dialect { return prototype(name) }

func athenaMayBeHive(tokens []*Token) bool {
	if len(tokens) == 0 {
		return false
	}
	switch tokens[0].Type {
	case TK_ALTER, TK_CREATE, TK_DESCRIBE, TK_DROP, TK_SHOW:
		return true
	}
	return pyUpper(tokens[0].Text) == "MSCK"
}

func tokenizeAsHive(tokens []*Token) bool {
	if len(tokens) < 2 {
		return false
	}
	first, second, rest := tokens[0], tokens[1], tokens[2:]
	firstText := pyUpper(first.Text)
	secondText := pyUpper(second.Text)
	if first.Type == TK_DESCRIBE || first.Type == TK_SHOW || firstText == "MSCK REPAIR" {
		return true
	}
	if first.Type == TK_ALTER || first.Type == TK_CREATE || first.Type == TK_DROP {
		if secondText == "DATABASE" || secondText == "EXTERNAL" || secondText == "SCHEMA" {
			return true
		}
		if second.Type == TK_VIEW {
			return false
		}
		for _, t := range rest {
			if t.Type == TK_SELECT {
				return false
			}
		}
		return true
	}
	return false
}
