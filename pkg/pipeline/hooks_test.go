package pipeline

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWrapHooks_TrimsAndSkipsEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		hooks Hooks
		want  string
	}{
		{
			name:  "trims and skips empty",
			query: "  select 9  ",
			hooks: Hooks{
				Pre:  []Hook{{Query: ""}, {Query: "select 1;"}},
				Post: []Hook{{Query: "  select 2  "}},
			},
			want: "select 1;\nselect 9;\nselect 2;",
		},
		{
			name:  "no hooks returns original query",
			query: "select 1",
			hooks: Hooks{},
			want:  "select 1",
		},
		{
			name:  "only pre hooks",
			query: "select 2",
			hooks: Hooks{
				Pre: []Hook{{Query: "select 1"}},
			},
			want: "select 1;\nselect 2;",
		},
		{
			name:  "only post hooks",
			query: "select 2",
			hooks: Hooks{
				Post: []Hook{{Query: "select 3"}},
			},
			want: "select 2;\nselect 3;",
		},
		{
			name:  "empty main query",
			query: " ",
			hooks: Hooks{
				Pre:  []Hook{{Query: "select 1"}},
				Post: []Hook{{Query: "select 3"}},
			},
			want: "select 1;\nselect 3;",
		},
		{
			name:  "preserves semicolons",
			query: "select 2;",
			hooks: Hooks{
				Pre:  []Hook{{Query: "select 1;"}},
				Post: []Hook{{Query: "select 3;"}},
			},
			want: "select 1;\nselect 2;\nselect 3;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, WrapHooks(tt.query, tt.hooks))
		})
	}
}

func TestWrapHookQueriesList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		queries []string
		hooks   Hooks
		want    []string
	}{
		{
			name:    "no hooks returns original list",
			queries: []string{"select 1"},
			hooks:   Hooks{},
			want:    []string{"select 1"},
		},
		{
			name:    "wraps pre and post hooks",
			queries: []string{"select 2"},
			hooks: Hooks{
				Pre:  []Hook{{Query: "select 1"}},
				Post: []Hook{{Query: "select 3"}},
			},
			want: []string{"select 1;", "select 2", "select 3;"},
		},
		{
			name:    "skips empty hooks",
			queries: []string{"select 2"},
			hooks: Hooks{
				Pre:  []Hook{{Query: " "}},
				Post: []Hook{{Query: ""}},
			},
			want: []string{"select 2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, wrapHookQueriesList(tt.queries, tt.hooks))
		})
	}
}

type hookRendererStub struct {
	render func(string) (string, error)
}

func (r hookRendererStub) Render(query string) (string, error) {
	return r.render(query)
}

func TestResolveHookTemplatesToNew(t *testing.T) {
	t.Parallel()

	renderer := hookRendererStub{
		render: func(query string) (string, error) {
			return query + " rendered", nil
		},
	}

	original := Hooks{
		Pre:  []Hook{{Query: "select '{{ start_date }}'"}},
		Post: []Hook{{Query: "select '{{ end_date }}'"}},
	}

	rendered, err := ResolveHookTemplatesToNew(original, renderer)
	require.NoError(t, err)
	assert.Equal(t, Hooks{
		Pre:  []Hook{{Query: "select '{{ start_date }}' rendered"}},
		Post: []Hook{{Query: "select '{{ end_date }}' rendered"}},
	}, rendered)

	// Ensure original hooks are not mutated.
	assert.Equal(t, Hooks{
		Pre:  []Hook{{Query: "select '{{ start_date }}'"}},
		Post: []Hook{{Query: "select '{{ end_date }}'"}},
	}, original)
}

func TestResolveHookTemplatesToNew_Error(t *testing.T) {
	t.Parallel()

	renderer := hookRendererStub{
		render: func(query string) (string, error) {
			if query == "bad" {
				return "", fmt.Errorf("missing variable")
			}
			return query, nil
		},
	}

	_, err := ResolveHookTemplatesToNew(Hooks{
		Pre: []Hook{{Query: "bad"}},
	}, renderer)
	require.Error(t, err)
	assert.ErrorContains(t, err, "failed to render pre hook 1")
}
