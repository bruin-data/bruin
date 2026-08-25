package query

import (
	"fmt"
	"strings"
)

// TSQLLimit wraps a T-SQL query (SQL Server, Azure Synapse, Microsoft Fabric)
// so it returns at most `limit` rows.
//
// For a plain query it uses a derived-table wrapper:
//
//	SELECT TOP N * FROM (<query>) as t
//
// T-SQL forbids a CTE (WITH ...) as the first token of a derived table, so that
// wrapper produces "Incorrect syntax near ')'" for any query that starts with a
// WITH clause. When the query begins with a CTE, the final query is appended to
// the existing CTE list as an extra CTE and limited from there instead:
//
//	WITH <existing ctes>, __bruin_limited AS (
//	<final query>
//	)
//	SELECT TOP N * FROM __bruin_limited
//
// The original text is preserved verbatim (no identifier rewriting), which
// matters on Fabric's case-sensitive collation.
//
// A query whose final statement ends in a top-level ORDER BY is not limitable by
// either form (T-SQL disallows ORDER BY in a CTE/derived table without TOP), so
// it falls through to the derived-table wrapper unchanged — the same behaviour
// as before this helper existed.
func TSQLLimit(sql string, limit int64) string {
	sql = strings.TrimRight(sql, "; \n\t")
	if prefix, body, ok := splitLeadingWith(sql); ok {
		return fmt.Sprintf("%s,\n__bruin_limited AS (\n%s\n)\nSELECT TOP %d * FROM __bruin_limited", prefix, body, limit)
	}
	return fmt.Sprintf("SELECT TOP %d * FROM (\n%s\n) as t", limit, sql)
}

// splitLeadingWith reports whether sql begins with a CTE (WITH ...) clause and,
// if so, splits it into the prefix (through the last CTE definition) and the
// trailing main query body. It is comment-, string-, and bracket-aware so it is
// not fooled by WITH/AS/parentheses appearing inside literals or CTE bodies.
// It returns ok=false for any query without a leading WITH clause, and also for
// malformed input, so callers fall back to the plain derived-table wrapper.
func splitLeadingWith(sql string) (prefix, body string, ok bool) {
	i := skipSpaceAndComments(sql, 0)
	if !matchKeyword(sql, i, "WITH") {
		return "", "", false
	}
	i += len("WITH")

	// Walk the comma-separated CTE list. Each CTE is:
	//   name [ (col, ...) ] AS ( <body> )
	// After a CTE body's closing paren, a comma means another CTE follows;
	// anything else marks the start of the main query.
	for {
		asIdx := findKeywordAtDepthZero(sql, i, "AS")
		if asIdx < 0 {
			return "", "", false
		}
		open := skipSpaceAndComments(sql, asIdx+len("AS"))
		if open >= len(sql) || sql[open] != '(' {
			return "", "", false
		}
		end := matchParen(sql, open)
		if end < 0 {
			return "", "", false
		}
		i = end + 1
		next := skipSpaceAndComments(sql, i)
		if next < len(sql) && sql[next] == ',' {
			i = next + 1
			continue
		}
		if next >= len(sql) {
			return "", "", false
		}
		// Body keeps everything after the last CTE (comments included), with only
		// surrounding whitespace trimmed.
		return strings.TrimRight(sql[:i], " \t\r\n"), strings.TrimLeft(sql[i:], " \t\r\n"), true
	}
}

// skipLiteral reports whether s[i] begins a single-quoted string, a bracketed or
// double-quoted identifier, or a line/block comment, and if so returns the index
// just past it. T-SQL escapes closing delimiters by doubling (” ]] "").
func skipLiteral(s string, i int) (int, bool) {
	if i >= len(s) {
		return i, false
	}
	switch {
	case s[i] == '\'':
		return skipDelimited(s, i, '\''), true
	case s[i] == '"':
		return skipDelimited(s, i, '"'), true
	case s[i] == '[':
		return skipDelimited(s, i, ']'), true
	case strings.HasPrefix(s[i:], "--"):
		if j := strings.IndexByte(s[i:], '\n'); j >= 0 {
			return i + j + 1, true
		}
		return len(s), true
	case strings.HasPrefix(s[i:], "/*"):
		if j := strings.Index(s[i+2:], "*/"); j >= 0 {
			return i + 2 + j + 2, true
		}
		return len(s), true
	}
	return i, false
}

// skipDelimited scans from an opening delimiter at s[open] to the matching close
// byte, treating a doubled close byte as an escape, and returns the index past
// the closing delimiter (or len(s) if unterminated).
func skipDelimited(s string, open int, close byte) int {
	i := open + 1
	for i < len(s) {
		if s[i] == close {
			if i+1 < len(s) && s[i+1] == close {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return i
}

func skipSpaceAndComments(s string, i int) int {
	for i < len(s) {
		switch s[i] {
		case ' ', '\t', '\n', '\r':
			i++
			continue
		}
		if strings.HasPrefix(s[i:], "--") || strings.HasPrefix(s[i:], "/*") {
			if ni, ok := skipLiteral(s, i); ok {
				i = ni
				continue
			}
		}
		break
	}
	return i
}

// matchKeyword reports whether the keyword kw appears at s[i] on identifier word
// boundaries, case-insensitively.
func matchKeyword(s string, i int, kw string) bool {
	if i+len(kw) > len(s) {
		return false
	}
	if !strings.EqualFold(s[i:i+len(kw)], kw) {
		return false
	}
	if i > 0 && isIdentChar(s[i-1]) {
		return false
	}
	if i+len(kw) < len(s) && isIdentChar(s[i+len(kw)]) {
		return false
	}
	return true
}

// findKeywordAtDepthZero returns the index of keyword kw at parenthesis depth 0,
// skipping literals and comments, or -1 if not found.
func findKeywordAtDepthZero(s string, start int, kw string) int {
	depth := 0
	for i := start; i < len(s); {
		if ni, ok := skipLiteral(s, i); ok {
			i = ni
			continue
		}
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		default:
			if depth == 0 && matchKeyword(s, i, kw) {
				return i
			}
		}
		i++
	}
	return -1
}

// matchParen returns the index of the ')' matching the '(' at s[open], skipping
// literals and comments, or -1 if unbalanced.
func matchParen(s string, open int) int {
	depth := 0
	for i := open; i < len(s); {
		if ni, ok := skipLiteral(s, i); ok {
			i = ni
			continue
		}
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
		i++
	}
	return -1
}

func isIdentChar(b byte) bool {
	return b == '_' ||
		(b >= '0' && b <= '9') ||
		(b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z')
}
