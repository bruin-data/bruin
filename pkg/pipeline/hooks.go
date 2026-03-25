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

// extractDeclareStatements separates DECLARE statements from the beginning of a query.
// BigQuery requires DECLARE statements to appear before any other statements in a script.
// Returns a slice of formatted DECLARE statements and the remaining query.
func extractDeclareStatements(query string) ([]string, string) {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return nil, ""
	}

	// Split by semicolons to get individual statements
	statements := splitStatements(trimmed)
	var declares []string
	firstNonDeclare := 0

	// Collect all DECLARE statements from the beginning
	for i, stmt := range statements {
		stmtTrimmed := strings.TrimSpace(stmt)
		if stmtTrimmed == "" {
			continue
		}

		// Check if this statement starts with DECLARE (case-insensitive)
		upperStmt := strings.ToUpper(stmtTrimmed)
		if strings.HasPrefix(upperStmt, "DECLARE ") || upperStmt == "DECLARE" {
			declares = append(declares, formatStatement(stmt))
			firstNonDeclare = i + 1
		} else {
			// Stop at the first non-DECLARE statement
			break
		}
	}

	// If no DECLARE statements found, return the original query as-is
	if len(declares) == 0 {
		return nil, trimmed
	}

	// Rejoin the remaining statements
	remaining := strings.Join(statements[firstNonDeclare:], ";")
	return declares, strings.TrimSpace(remaining)
}

// splitStatements splits a query into individual statements by semicolons.
// This is a simple split that doesn't handle strings or comments containing semicolons.
func splitStatements(query string) []string {
	return strings.Split(query, ";")
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
