package query

import (
	"fmt"
	"strings"
)

// TSQLLimit wraps a T-SQL query so it returns at most `limit` rows, preserving
// the query text verbatim. CTE-leading queries are limited via an appended CTE,
// since a derived-table wrapper is invalid T-SQL after a WITH clause.
func TSQLLimit(sql string, limit int64) string {
	sql = strings.TrimRight(sql, "; \n\t")
	if prefix, body, ok := splitLeadingWith(sql); ok {
		name := uniqueCTEName(sql)
		return fmt.Sprintf("%s,\n%s AS (\n%s\n)\nSELECT TOP %d * FROM %s", prefix, name, body, limit, name)
	}
	return fmt.Sprintf("SELECT TOP %d * FROM (\n%s\n) as t", limit, sql)
}

// uniqueCTEName returns a limit CTE name that does not already occur in sql.
func uniqueCTEName(sql string) string {
	lower := strings.ToLower(sql)
	name := "__bruin_limited"
	for strings.Contains(lower, name) {
		name += "_x"
	}
	return name
}

// splitLeadingWith splits a leading CTE clause into the prefix (through the last
// CTE) and the trailing main query. It is comment-, string-, and bracket-aware.
// ok is false when there is no leading WITH or the input is malformed, so callers
// fall back to the plain wrapper.
func splitLeadingWith(sql string) (prefix, body string, ok bool) {
	i := skipSpaceAndComments(sql, 0)
	if !matchKeyword(sql, i, "WITH") {
		return "", "", false
	}
	i += len("WITH")

	// Each CTE is `name [ (cols) ] AS ( body )`; a comma after a body means
	// another CTE follows, anything else starts the main query.
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
		return strings.TrimRight(sql[:i], " \t\r\n"), strings.TrimLeft(sql[i:], " \t\r\n"), true
	}
}

// skipLiteral, if s[i] starts a quoted string, bracketed/quoted identifier, or
// comment, returns the index just past it. T-SQL doubles closers to escape them.
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

// skipDelimited scans from the opener at s[open] past the matching closer byte,
// treating a doubled closer as an escape.
func skipDelimited(s string, open int, closer byte) int {
	for i := open + 1; i < len(s); i++ {
		if s[i] == closer {
			if i+1 < len(s) && s[i+1] == closer {
				i++
				continue
			}
			return i + 1
		}
	}
	return len(s)
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

// matchKeyword reports whether kw appears at s[i] on identifier boundaries.
func matchKeyword(s string, i int, kw string) bool {
	if i+len(kw) > len(s) || !strings.EqualFold(s[i:i+len(kw)], kw) {
		return false
	}
	if i > 0 && isIdentChar(s[i-1]) {
		return false
	}
	return i+len(kw) >= len(s) || !isIdentChar(s[i+len(kw)])
}

// findKeywordAtDepthZero returns the index of kw at paren depth 0, or -1.
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

// matchParen returns the index of the ')' matching the '(' at s[open], or -1.
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
