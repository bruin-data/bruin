package pipeline

import (
	"fmt"
	"strings"
)

// DeclareHoister reorders a multi-statement SQL script so DECLARE statements
// appear before any other statements, using a real SQL parser (sqlglot) so
// nested DECLAREs inside stored procedure / BEGIN..END blocks are left in
// place. Implementations must return the input unchanged when no reordering
// is needed, and should return (input, err) on failure so callers can fall
// back gracefully.
type DeclareHoister interface {
	HoistDeclares(sql string, assetType AssetType) (string, error)
	HoistDeclaresList(queries []string, assetType AssetType) ([]string, error)
}

// WrapHooks joins pre-hooks, the rendered query, and post-hooks into a
// single multi-statement script. When a non-nil hoister is supplied, the
// joined script is passed through it so any DECLARE statements (from the
// materialization itself or from a hook) get hoisted to the top, which
// some dialects (notably BigQuery) require.
func WrapHooks(query string, hooks Hooks, hoister DeclareHoister, assetType AssetType) string {
	preParts := formatHookQueries(hooks.Pre)
	postParts := formatHookQueries(hooks.Post)
	if len(preParts) == 0 && len(postParts) == 0 {
		return maybeHoist(query, hoister, assetType)
	}

	parts := make([]string, 0, len(preParts)+1+len(postParts))
	parts = append(parts, preParts...)

	if main := formatStatement(query); main != "" {
		parts = append(parts, main)
	}

	parts = append(parts, postParts...)
	return maybeHoist(strings.Join(parts, "\n"), hoister, assetType)
}

func wrapHookQueriesList(queries []string, hooks Hooks, hoister DeclareHoister, assetType AssetType) []string {
	pre := formatHookQueries(hooks.Pre)
	post := formatHookQueries(hooks.Post)
	if len(pre) == 0 && len(post) == 0 {
		return maybeHoistList(queries, hoister, assetType)
	}

	combined := make([]string, 0, len(pre)+len(queries)+len(post))
	combined = append(combined, pre...)
	combined = append(combined, queries...)
	combined = append(combined, post...)
	return maybeHoistList(combined, hoister, assetType)
}

func maybeHoist(sql string, hoister DeclareHoister, assetType AssetType) string {
	if hoister == nil {
		return sql
	}
	hoisted, err := hoister.HoistDeclares(sql, assetType)
	if err != nil {
		return sql
	}
	return hoisted
}

func maybeHoistList(queries []string, hoister DeclareHoister, assetType AssetType) []string {
	if hoister == nil {
		return queries
	}
	hoisted, err := hoister.HoistDeclaresList(queries, assetType)
	if err != nil {
		return queries
	}
	return hoisted
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
