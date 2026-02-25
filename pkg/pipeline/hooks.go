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

	parts := make([]string, 0, len(preParts)+1+len(postParts))
	parts = append(parts, preParts...)

	if main := formatStatement(query); main != "" {
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
