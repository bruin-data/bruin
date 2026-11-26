//go:build !bruin_no_duckdb

package duck

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/sqlengine"
)

// splitStatements returns the non-empty top-level statements of a script, without their semicolons.
func splitStatements(sqlStr string) ([]string, error) {
	tokens, err := sqlengine.MustDialect("duckdb").Tokenize(sqlStr)
	if err != nil {
		return nil, fmt.Errorf("failed to tokenize DuckDB query: %w", err)
	}
	// Token positions count code points, not bytes.
	runes := []rune(sqlStr)
	statements := []string{}
	start, hasTokens := 0, false
	for _, token := range tokens {
		if token.Type != sqlengine.TK_SEMICOLON {
			hasTokens = true
			continue
		}
		if hasTokens {
			statements = append(statements, strings.TrimSpace(string(runes[start:token.Start])))
		}
		start, hasTokens = int(token.End)+1, false
	}
	if hasTokens {
		statements = append(statements, strings.TrimSpace(string(runes[start:])))
	}
	return statements, nil
}
