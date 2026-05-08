package pipeline

import (
	"fmt"
	"regexp"
	"strings"
)

// declarePrefixRegex matches a leading DECLARE keyword (case-insensitive) after any
// leading whitespace and SQL line/block comments have been stripped.
var declarePrefixRegex = regexp.MustCompile(`(?i)^declare\b`)

func WrapHooks(query string, hooks Hooks) string {
	preParts := formatHookQueries(hooks.Pre)
	postParts := formatHookQueries(hooks.Post)
	if len(preParts) == 0 && len(postParts) == 0 {
		return hoistDeclares(query)
	}

	parts := make([]string, 0, len(preParts)+1+len(postParts))
	parts = append(parts, preParts...)

	if main := formatStatement(query); main != "" {
		parts = append(parts, main)
	}

	parts = append(parts, postParts...)
	return hoistDeclares(strings.Join(parts, "\n"))
}

func wrapHookQueriesList(queries []string, hooks Hooks) []string {
	pre := formatHookQueries(hooks.Pre)
	post := formatHookQueries(hooks.Post)
	if len(pre) == 0 && len(post) == 0 {
		return hoistDeclaresList(queries)
	}

	combined := make([]string, 0, len(pre)+len(queries)+len(post))
	combined = append(combined, pre...)
	combined = append(combined, queries...)
	combined = append(combined, post...)
	return hoistDeclaresList(combined)
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

// hoistDeclares moves any DECLARE statements within a multi-statement SQL script
// to the top, preserving the relative order of DECLAREs and the relative order
// of remaining statements. Some SQL dialects (notably BigQuery) require all
// DECLARE statements to appear at the very start of a script or block; when
// asset hooks contain non-DECLARE statements like SET or IF, a materialization's
// own DECLARE can otherwise end up past the start of the script.
//
// Returns the input unchanged when no DECLARE statements are present, when only
// a single statement exists, or when reordering would have no effect.
func hoistDeclares(sql string) string {
	if !containsDeclare(sql) {
		return sql
	}

	parts := strings.Split(sql, ";")
	declares := make([]string, 0, len(parts))
	rest := make([]string, 0, len(parts))
	hadDeclare := false

	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		if isDeclareStatement(trimmed) {
			declares = append(declares, trimmed)
			hadDeclare = true
		} else {
			rest = append(rest, trimmed)
		}
	}

	if !hadDeclare {
		return sql
	}

	reordered := append(declares, rest...) //nolint:gocritic // intentional concat into a fresh slice
	return strings.Join(reordered, ";\n") + ";"
}

// hoistDeclaresList reorders a list of pre-split SQL statements so DECLAREs
// appear first, preserving relative order within each group. Mirrors hoistDeclares
// for list-based materializers.
func hoistDeclaresList(queries []string) []string {
	hasDeclare := false
	for _, q := range queries {
		if isDeclareStatement(q) {
			hasDeclare = true
			break
		}
	}
	if !hasDeclare {
		return queries
	}

	declares := make([]string, 0, len(queries))
	rest := make([]string, 0, len(queries))
	for _, q := range queries {
		if isDeclareStatement(q) {
			declares = append(declares, q)
		} else {
			rest = append(rest, q)
		}
	}
	return append(declares, rest...)
}

// containsDeclare is a fast pre-check that avoids splitting the script when
// no DECLARE keyword is present at all.
func containsDeclare(sql string) bool {
	lower := strings.ToLower(sql)
	return strings.Contains(lower, "declare")
}

// isDeclareStatement reports whether the given statement (without trailing ';')
// begins with a DECLARE keyword once leading whitespace and SQL comments are
// stripped.
func isDeclareStatement(stmt string) bool {
	trimmed := stripLeadingCommentsAndWhitespace(stmt)
	return declarePrefixRegex.MatchString(trimmed)
}

// stripLeadingCommentsAndWhitespace removes leading whitespace and SQL line
// (`-- ...`) and block (`/* ... */`) comments, returning the remainder. It
// stops at the first non-comment, non-whitespace character.
func stripLeadingCommentsAndWhitespace(s string) string {
	for {
		s = strings.TrimLeft(s, " \t\r\n")
		switch {
		case strings.HasPrefix(s, "--"):
			if idx := strings.IndexByte(s, '\n'); idx >= 0 {
				s = s[idx+1:]
			} else {
				return ""
			}
		case strings.HasPrefix(s, "/*"):
			if idx := strings.Index(s, "*/"); idx >= 0 {
				s = s[idx+2:]
			} else {
				return ""
			}
		default:
			return s
		}
	}
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
