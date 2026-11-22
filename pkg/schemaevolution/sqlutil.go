package schemaevolution

import (
	"strings"

	"github.com/bruin-data/bruin/pkg/sqlengine"
)

func dialectOrDefault(dialectName string) *sqlengine.Dialect {
	d, err := sqlengine.GetDialect(dialectName)
	if err != nil || d == nil {
		return sqlengine.MustDialect("")
	}
	return d
}

// IsConstantLiteralExpr reports whether expr is empty or a constant SQL literal
// (NULL, TRUE, FALSE, signed/unsigned integer/decimal/scientific literal, or single-quoted
// string literal) using pkg/sqlengine's dialect tokenizer without regular expressions.
func IsConstantLiteralExpr(dialectName, expr string) bool {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return true
	}
	tokens, err := dialectOrDefault(dialectName).Tokenize(trimmed)
	if err != nil || len(tokens) == 0 {
		return false
	}
	if len(tokens) == 1 {
		switch tokens[0].Type {
		case sqlengine.TK_NULL, sqlengine.TK_TRUE, sqlengine.TK_FALSE, sqlengine.TK_NUMBER, sqlengine.TK_STRING:
			return true
		default:
			return false
		}
	}

	rest := tokens
	if rest[0].Type == sqlengine.TK_PLUS || rest[0].Type == sqlengine.TK_DASH {
		rest = rest[1:]
	}
	if len(rest) == 1 && rest[0].Type == sqlengine.TK_NUMBER {
		return true
	}
	// Leading-dot decimal literal such as `.5`, `-.5`, or `+.5e-2` tokenizes as [TK_DOT, TK_NUMBER].
	if len(rest) == 2 &&
		rest[0].Type == sqlengine.TK_DOT &&
		rest[1].Type == sqlengine.TK_NUMBER &&
		int(rest[0].End)+1 == int(rest[1].Start) &&
		!strings.Contains(rest[1].Text, ".") {
		return true
	}
	return false
}

// IsBareValuesQuery reports whether queryStr starts with a top-level VALUES clause
// (ignoring leading whitespace and SQL comments).
func IsBareValuesQuery(dialectName, queryStr string) bool {
	tokens, err := dialectOrDefault(dialectName).Tokenize(queryStr)
	if err != nil || len(tokens) == 0 {
		return false
	}
	return tokens[0].Type == sqlengine.TK_VALUES
}

// StripTrailingSemicolonsAndComments removes trailing SQL comments and any trailing
// statement-terminating semicolons using pkg/sqlengine token offsets so queryStr can
// safely be embedded as a subquery or terminated with `;`.
func StripTrailingSemicolonsAndComments(dialectName, queryStr string) string {
	trimmed := strings.TrimSpace(queryStr)
	if trimmed == "" {
		return ""
	}
	tokens, err := dialectOrDefault(dialectName).Tokenize(trimmed)
	if err != nil || len(tokens) == 0 {
		return strings.TrimSpace(strings.TrimRight(trimmed, ";"))
	}
	i := len(tokens) - 1
	for i >= 0 && tokens[i].Type == sqlengine.TK_SEMICOLON {
		i--
	}
	if i < 0 {
		return ""
	}
	runes := []rune(trimmed)
	end := int(tokens[i].End) + 1
	if end >= 0 && end <= len(runes) {
		return strings.TrimSpace(string(runes[:end]))
	}
	return trimmed
}

func findLeadingBeginTransactionEnd(dialectName, sqlStr string) (int, bool) {
	tokens, err := dialectOrDefault(dialectName).Tokenize(sqlStr)
	if err != nil || len(tokens) < 2 {
		return 0, false
	}

	first := tokens[0]
	if first.Type != sqlengine.TK_BEGIN && !strings.EqualFold(first.Text, "START") {
		return 0, false
	}

	idx := 1
	if idx < len(tokens) && (strings.EqualFold(tokens[idx].Text, "TRANSACTION") || strings.EqualFold(tokens[idx].Text, "WORK")) {
		idx++
	}
	if idx >= len(tokens) || tokens[idx].Type != sqlengine.TK_SEMICOLON {
		return 0, false
	}

	runes := []rune(sqlStr)
	endRune := int(tokens[idx].End) + 1
	if endRune <= 0 || endRune > len(runes) {
		return 0, false
	}
	return len(string(runes[:endRune])), true
}

// SpliceStatementsInTransaction injects stmts into mainSQL so schema evolution DDL
// and the write query execute atomically inside one SQL transaction.
func SpliceStatementsInTransaction(dialectName, mainSQL string, stmts []string) string {
	if len(stmts) == 0 {
		return mainSQL
	}

	alterBlock := strings.Join(stmts, ";\n") + ";"
	trimmed := strings.TrimSpace(mainSQL)

	if endByte, ok := findLeadingBeginTransactionEnd(dialectName, trimmed); ok {
		prefix := trimmed[:endByte]
		rest := strings.TrimLeft(trimmed[endByte:], " \t\r\n")
		return prefix + "\n" + alterBlock + "\n" + rest
	}

	body := StripTrailingSemicolonsAndComments(dialectName, trimmed)
	return "BEGIN TRANSACTION;\n" + alterBlock + "\n" + body + ";\nCOMMIT;"
}

// SplitTableIdentifier parses a 1-, 2-, or 3-part SQL table reference using pkg/sqlengine's
// dialect tokenizer, unquoting quoted identifiers while preserving dots, spaces, and escaped
// quotes inside quoted parts across dialects (DuckDB, BigQuery, Snowflake, Postgres, etc.).
func SplitTableIdentifier(dialectName, tableName, defaultSchema string) (catalog, schema, table string, partCount int) {
	trimmed := strings.TrimSpace(tableName)
	if trimmed == "" {
		return "", defaultSchema, "", 0
	}

	if parts, ok := tokenizeTableParts(dialectName, trimmed); ok && len(parts) > 0 {
		switch len(parts) {
		case 1:
			return "", defaultSchema, parts[0], 1
		case 2:
			return "", parts[0], parts[1], 2
		default:
			return parts[0], parts[1], parts[2], len(parts)
		}
	}

	rawParts := strings.Split(trimmed, ".")
	switch len(rawParts) {
	case 1:
		return "", defaultSchema, strings.TrimSpace(rawParts[0]), 1
	case 2:
		return "", strings.TrimSpace(rawParts[0]), strings.TrimSpace(rawParts[1]), 2
	default:
		return strings.TrimSpace(rawParts[0]), strings.TrimSpace(rawParts[1]), strings.TrimSpace(rawParts[2]), len(rawParts)
	}
}

func tokenizeTableParts(dialectName, tableName string) ([]string, bool) {
	tokens, err := dialectOrDefault(dialectName).Tokenize(tableName)
	if err != nil || len(tokens) == 0 {
		return nil, false
	}

	parts, nextIdx, ok := collectDotSeparatedParts(tokens, 0)
	if !ok || nextIdx != len(tokens) {
		return nil, false
	}
	return parts, true
}

func collectDotSeparatedParts(tokens []*sqlengine.Token, start int) ([]string, int, bool) {
	if start >= len(tokens) || isNonIdentifierPunctuation(tokens[start].Type) {
		return nil, start, false
	}
	parts := []string{tokens[start].Text}
	idx := start + 1
	for idx < len(tokens) && tokens[idx].Type == sqlengine.TK_DOT {
		idx++
		if idx >= len(tokens) || isNonIdentifierPunctuation(tokens[idx].Type) {
			return nil, idx, false
		}
		parts = append(parts, tokens[idx].Text)
		idx++
	}
	return parts, idx, true
}

func isNonIdentifierPunctuation(tt sqlengine.TokenType) bool {
	switch tt {
	case sqlengine.TK_DOT,
		sqlengine.TK_COMMA,
		sqlengine.TK_SEMICOLON,
		sqlengine.TK_L_PAREN,
		sqlengine.TK_R_PAREN,
		sqlengine.TK_L_BRACKET,
		sqlengine.TK_R_BRACKET,
		sqlengine.TK_L_BRACE,
		sqlengine.TK_R_BRACE,
		sqlengine.TK_STRING:
		return true
	default:
		return false
	}
}

// QuoteDoubleQuotedIdentifier quotes a single SQL identifier (such as a column name)
// in ANSI double quotes without splitting on dots, escaping embedded double quotes.
func QuoteDoubleQuotedIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
