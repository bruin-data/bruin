package pipeline

import (
	"errors"
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

func TestWrapHooks_HoistsDeclares(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		hooks Hooks
		want  string
	}{
		{
			name: "materialization DECLARE bubbles past pre-hook SET",
			query: "DECLARE distinct_keys array<STRING>;\n" +
				"BEGIN TRANSACTION;\n" +
				"SELECT 1;\n" +
				"COMMIT TRANSACTION",
			hooks: Hooks{
				Pre: []Hook{
					{Query: "DECLARE my_var DATE"},
					{Query: "SET my_var = DATE('2026-01-01')"},
				},
			},
			want: "DECLARE my_var DATE;\n" +
				"DECLARE distinct_keys array<STRING>;\n" +
				"SET my_var = DATE('2026-01-01');\n" +
				"BEGIN TRANSACTION;\n" +
				"SELECT 1;\n" +
				"COMMIT TRANSACTION;",
		},
		{
			name:  "lowercase declare also hoisted",
			query: "select 1",
			hooks: Hooks{
				Pre: []Hook{
					{Query: "set x = 1"},
					{Query: "declare y int64"},
				},
			},
			want: "declare y int64;\n" +
				"set x = 1;\n" +
				"select 1;",
		},
		{
			name:  "declare preceded by line comment is still detected",
			query: "select 1",
			hooks: Hooks{
				Pre: []Hook{
					{Query: "SET x = 1"},
					{Query: "-- setup\nDECLARE y INT64"},
				},
			},
			want: "-- setup\nDECLARE y INT64;\n" +
				"SET x = 1;\n" +
				"select 1;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, WrapHooks(tt.query, tt.hooks))
		})
	}
}

func TestHoistDeclares(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "no declare is a no-op",
			in:   "SELECT 1;\nSELECT 2;",
			want: "SELECT 1;\nSELECT 2;",
		},
		{
			name: "single statement is a no-op",
			in:   "SELECT 1",
			want: "SELECT 1",
		},
		{
			name: "leading declare is a no-op",
			in:   "DECLARE x INT64;\nSELECT 1;",
			want: "DECLARE x INT64;\nSELECT 1;",
		},
		{
			name: "declare after non-declare gets hoisted",
			in:   "SET x = 1;\nDECLARE y INT64;\nSELECT 1;",
			want: "DECLARE y INT64;\nSET x = 1;\nSELECT 1;",
		},
		{
			name: "multiple declares preserve relative order",
			in:   "SET x = 1;\nDECLARE y INT64;\nSET z = 2;\nDECLARE w STRING;",
			want: "DECLARE y INT64;\nDECLARE w STRING;\nSET x = 1;\nSET z = 2;",
		},
		{
			name: "case-insensitive detection",
			in:   "set x = 1;\ndeclare y int64",
			want: "declare y int64;\nset x = 1;",
		},
		{
			name: "leading comment before declare is tolerated",
			in:   "SET x = 1;\n-- a comment\nDECLARE y INT64",
			want: "-- a comment\nDECLARE y INT64;\nSET x = 1;",
		},
		{
			name: "block comment before declare is tolerated",
			in:   "SET x = 1;\n/* block */ DECLARE y INT64",
			want: "/* block */ DECLARE y INT64;\nSET x = 1;",
		},
		{
			name: "empty parts are dropped",
			in:   "SET x = 1;;\nDECLARE y INT64;",
			want: "DECLARE y INT64;\nSET x = 1;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, hoistDeclares(tt.in))
		})
	}
}

func TestHoistDeclaresList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "no declare is a no-op",
			in:   []string{"SELECT 1", "SELECT 2"},
			want: []string{"SELECT 1", "SELECT 2"},
		},
		{
			name: "declare after non-declare gets hoisted",
			in:   []string{"SET x = 1", "DECLARE y INT64", "SELECT 1"},
			want: []string{"DECLARE y INT64", "SET x = 1", "SELECT 1"},
		},
		{
			name: "case-insensitive",
			in:   []string{"set x = 1", "declare y int64"},
			want: []string{"declare y int64", "set x = 1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, hoistDeclaresList(tt.in))
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
				return "", errors.New("missing variable")
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

func TestAssetFormatContent_HookQueries_NoSemicolonInjection(t *testing.T) {
	t.Parallel()

	asset := &Asset{
		Name: "local.hook_demo",
		Type: AssetTypeDuckDBQuery,
		Hooks: Hooks{
			Pre: []Hook{
				{Query: "SELECT 1; -- comment"},
				{Query: "SELECT 2   "},
			},
			Post: []Hook{
				{Query: "SELECT 3;   "},
			},
		},
		ExecutableFile: ExecutableFile{
			Path:    "hook_demo.sql",
			Content: "SELECT 1 AS id",
		},
	}

	_, err := asset.FormatContent()
	require.NoError(t, err)

	assert.Equal(t, "SELECT 1; -- comment", asset.Hooks.Pre[0].Query)
	assert.Equal(t, "SELECT 2", asset.Hooks.Pre[1].Query)
	assert.Equal(t, "SELECT 3;", asset.Hooks.Post[0].Query)
}
