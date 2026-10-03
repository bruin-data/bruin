package sqlengine

import "sync"

var (
	athenaTrinoTokOnce sync.Once
	athenaTrinoTok     *tokenizerConfig
)

// setupAthenaTokenizer mirrors Athena.Tokenizer.tokenize, which routes to the Hive or
// Trino tokenizer depending on the statement.
func setupAthenaTokenizer(d *Dialect) {
	d.hooks.tokenize = func(d *Dialect, sql string) ([]*Token, error) {
		tokens, err := newTokenizerCore(d.tok).tokenize(sql)
		if err != nil {
			return nil, err
		}
		if tokenizeAsHive(tokens) {
			hive := prototypeLocked2("hive")
			ht, err := newTokenizerCore(hive.tok).tokenize(sql)
			if err != nil {
				return nil, err
			}
			return append([]*Token{{Type: TK_HIVE_TOKEN_STREAM, Text: "", Line: 1, Col: 1, Comments: []string{}}}, ht...), nil
		}
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
		return newTokenizerCore(athenaTrinoTok).tokenize(sql)
	}
}

// prototypeLocked2 fetches a prototype at tokenization time (outside dialect setup).
func prototypeLocked2(name string) *Dialect { return prototype(name) }

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
