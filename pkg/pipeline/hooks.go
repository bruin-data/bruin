package pipeline

import (
	"fmt"
	"strings"
)

func WrapHooks(query string, hooks Hooks) string {
	preParts := formatHookQueries(hooks.Pre)
	postParts := formatHookQueries(hooks.Post)
	if len(preParts) == 0 && len(postParts) == 0 {
		return query
	}

	// Extract DECLARE statements from the beginning of the query
	// BigQuery requires DECLARE to come before any other statements
	declares, remainingQuery := extractDeclareStatements(query)

	parts := make([]string, 0, len(declares)+len(preParts)+1+len(postParts))

	// Add DECLARE statements first
	parts = append(parts, declares...)

	// Add pre-hooks after DECLARE
	parts = append(parts, preParts...)

	// Add the main query (without DECLARE statements)
	if main := formatStatement(remainingQuery); main != "" {
		parts = append(parts, main)
	}

	parts = append(parts, postParts...)
	return strings.Join(parts, "\n")
}

func wrapHookQueriesList(queries []string, hooks Hooks) []string {
	pre := formatHookQueries(hooks.Pre)
	post := formatHookQueries(hooks.Post)
	if len(pre) == 0 && len(post) == 0 {
		return queries
	}

	combined := make([]string, 0, len(pre)+len(queries)+len(post))
	combined = append(combined, pre...)
	combined = append(combined, queries...)
	combined = append(combined, post...)
	return combined
}

func formatHookQueries(hooks []Hook) []string {
	formatted := make([]string, 0, len(hooks))
	for _, hook := range hooks {
		if formattedQuery := formatStatement(hook.Query); formattedQuery != "" {
			formatted = append(formatted, formattedQuery)
		}
	}
	return formatted
}

func formatStatement(query string) string {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return ""
	}
	if strings.HasSuffix(trimmed, ";") {
		return trimmed
	}
	return trimmed + ";"
}

// extractDeclareStatements separates DECLARE statements from the beginning of a
// query. BigQuery requires DECLARE statements to appear before any other
// statements in a script, so when hooks are present the leading DECLAREs must
// be lifted above the pre-hooks.
//
// The scan is SQL-aware: semicolons inside string literals (single, double,
// backtick, and BigQuery triple-quoted), line comments (--) and block
// comments (/* */) are not treated as statement terminators.
func extractDeclareStatements(query string) ([]string, string) {
	if strings.TrimSpace(query) == "" {
		return nil, ""
	}

	var declares []string
	n := len(query)
	i := 0
	stmtStart := 0
	for i < n {
		switch c := query[i]; {
		case c == '-' && i+1 < n && query[i+1] == '-':
			for i < n && query[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && query[i+1] == '*':
			i += 2
			for i+1 < n && (query[i] != '*' || query[i+1] != '/') {
				i++
			}
			if i+1 < n {
				i += 2
			} else {
				i = n
			}
		case c == '\'' || c == '"':
			i = skipStringLiteral(query, i)
		case c == '`':
			i++
			for i < n && query[i] != '`' {
				i++
			}
			if i < n {
				i++
			}
		case c == ';':
			stmt := query[stmtStart:i]
			if startsWithDeclare(stmt) {
				declares = append(declares, formatStatement(stmt))
				i++
				stmtStart = i
				continue
			}
			if strings.TrimSpace(stmt) == "" {
				i++
				stmtStart = i
				continue
			}
			return declares, strings.TrimSpace(query[stmtStart:])
		default:
			i++
		}
	}

	trailing := query[stmtStart:]
	if strings.TrimSpace(trailing) == "" {
		if len(declares) == 0 {
			return nil, strings.TrimSpace(query)
		}
		return declares, ""
	}
	if startsWithDeclare(trailing) {
		declares = append(declares, formatStatement(trailing))
		return declares, ""
	}
	if len(declares) == 0 {
		return nil, strings.TrimSpace(query)
	}
	return declares, strings.TrimSpace(trailing)
}

// skipStringLiteral advances past a string literal beginning at query[i].
// Handles BigQuery triple-quoted strings (”'...”' / """...""") in addition
// to standard single/double-quoted strings with backslash escapes.
func skipStringLiteral(query string, i int) int {
	n := len(query)
	quote := query[i]
	if i+2 < n && query[i+1] == quote && query[i+2] == quote {
		i += 3
		for i+2 < n && (query[i] != quote || query[i+1] != quote || query[i+2] != quote) {
			i++
		}
		if i+2 < n {
			return i + 3
		}
		return n
	}
	i++
	for i < n {
		if query[i] == '\\' && i+1 < n {
			i += 2
			continue
		}
		if query[i] == quote {
			return i + 1
		}
		i++
	}
	return n
}

// startsWithDeclare reports whether stmt's first significant token is the
// DECLARE keyword (case-insensitive). Leading whitespace and SQL comments are
// skipped before the keyword check.
func startsWithDeclare(stmt string) bool {
	n := len(stmt)
	i := 0
	for i < n {
		c := stmt[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '-' && i+1 < n && stmt[i+1] == '-':
			for i < n && stmt[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && stmt[i+1] == '*':
			i += 2
			for i+1 < n && (stmt[i] != '*' || stmt[i+1] != '/') {
				i++
			}
			if i+1 < n {
				i += 2
			} else {
				return false
			}
		default:
			rest := stmt[i:]
			const kw = "DECLARE"
			if len(rest) < len(kw) {
				return false
			}
			if !strings.EqualFold(rest[:len(kw)], kw) {
				return false
			}
			if len(rest) == len(kw) {
				return true
			}
			next := rest[len(kw)]
			return next == ' ' || next == '\t' || next == '\n' || next == '\r'
		}
	}
	return false
}

// ResolveHookTemplatesToNew renders hook query templates with the provided renderer and returns a new hooks value.
func ResolveHookTemplatesToNew(hooks Hooks, renderer RendererInterface) (Hooks, error) {
	if renderer == nil {
		return hooks, nil
	}

	rendered := Hooks{
		Pre:  make([]Hook, 0, len(hooks.Pre)),
		Post: make([]Hook, 0, len(hooks.Post)),
	}

	for i, hook := range hooks.Pre {
		renderedQuery, err := renderer.Render(hook.Query)
		if err != nil {
			return Hooks{}, fmt.Errorf("failed to render pre hook %d: %w", i+1, err)
		}
		rendered.Pre = append(rendered.Pre, Hook{Query: strings.TrimSpace(renderedQuery)})
	}

	for i, hook := range hooks.Post {
		renderedQuery, err := renderer.Render(hook.Query)
		if err != nil {
			return Hooks{}, fmt.Errorf("failed to render post hook %d: %w", i+1, err)
		}
		rendered.Post = append(rendered.Post, Hook{Query: strings.TrimSpace(renderedQuery)})
	}

	return rendered, nil
}
