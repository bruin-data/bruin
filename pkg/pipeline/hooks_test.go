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
		{
			name:  "DECLARE statement with hooks",
			query: "DECLARE var1 INT64;\nSELECT var1;",
			hooks: Hooks{
				Pre:  []Hook{{Query: "CREATE TEMP TABLE tmp AS SELECT 1"}},
				Post: []Hook{{Query: "DROP TABLE tmp"}},
			},
			want: "DECLARE var1 INT64;\nCREATE TEMP TABLE tmp AS SELECT 1;\nSELECT var1;\nDROP TABLE tmp;",
		},
		{
			name:  "multiple DECLARE statements with hooks",
			query: "DECLARE var1 INT64; DECLARE var2 STRING; SELECT var1, var2;",
			hooks: Hooks{
				Pre:  []Hook{{Query: "SELECT 1"}},
				Post: []Hook{{Query: "SELECT 2"}},
			},
			want: "DECLARE var1 INT64;\nDECLARE var2 STRING;\nSELECT 1;\nSELECT var1, var2;\nSELECT 2;",
		},
		{
			name:  "DECLARE with array type",
			query: "DECLARE distinct_keys array<date>;\nBEGIN TRANSACTION;\nSELECT 1;",
			hooks: Hooks{
				Pre:  []Hook{{Query: "CREATE SCHEMA IF NOT EXISTS test"}},
				Post: []Hook{{Query: "COMMIT"}},
			},
			want: "DECLARE distinct_keys array<date>;\nCREATE SCHEMA IF NOT EXISTS test;\nBEGIN TRANSACTION;\nSELECT 1;\nCOMMIT;",
		},
		{
			name:  "DECLARE case insensitive",
			query: "declare var1 INT64;\nselect var1;",
			hooks: Hooks{
				Pre: []Hook{{Query: "SELECT 1"}},
			},
			want: "declare var1 INT64;\nSELECT 1;\nselect var1;",
		},
		{
			name:  "no DECLARE statements preserves original behavior",
			query: "SELECT 1; INSERT INTO table VALUES (2);",
			hooks: Hooks{
				Pre:  []Hook{{Query: "CREATE TABLE table (id INT)"}},
				Post: []Hook{{Query: "DROP TABLE table"}},
			},
			want: "CREATE TABLE table (id INT);\nSELECT 1; INSERT INTO table VALUES (2);\nDROP TABLE table;",
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

func TestExtractDeclareStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		query              string
		wantDeclares       []string
		wantRemainingQuery string
	}{
		{
			name:               "no DECLARE statements",
			query:              "SELECT 1; INSERT INTO table VALUES (2);",
			wantDeclares:       nil,
			wantRemainingQuery: "SELECT 1; INSERT INTO table VALUES (2);",
		},
		{
			name:               "single DECLARE statement",
			query:              "DECLARE var1 INT64; SELECT var1;",
			wantDeclares:       []string{"DECLARE var1 INT64;"},
			wantRemainingQuery: "SELECT var1;",
		},
		{
			name:               "multiple DECLARE statements",
			query:              "DECLARE var1 INT64; DECLARE var2 STRING; SELECT var1, var2;",
			wantDeclares:       []string{"DECLARE var1 INT64;", "DECLARE var2 STRING;"},
			wantRemainingQuery: "SELECT var1, var2;",
		},
		{
			name:               "DECLARE with array type",
			query:              "DECLARE distinct_keys array<date>;\nBEGIN TRANSACTION;\nSELECT 1;",
			wantDeclares:       []string{"DECLARE distinct_keys array<date>;"},
			wantRemainingQuery: "BEGIN TRANSACTION;\nSELECT 1;",
		},
		{
			name:               "case insensitive DECLARE",
			query:              "declare var1 INT64; DeClaRe var2 STRING; SELECT 1;",
			wantDeclares:       []string{"declare var1 INT64;", "DeClaRe var2 STRING;"},
			wantRemainingQuery: "SELECT 1;",
		},
		{
			name:               "DECLARE in middle of query should stop at first non-DECLARE",
			query:              "DECLARE var1 INT64; SELECT 1; DECLARE var2 STRING;",
			wantDeclares:       []string{"DECLARE var1 INT64;"},
			wantRemainingQuery: "SELECT 1; DECLARE var2 STRING;",
		},
		{
			name:               "empty query",
			query:              "",
			wantDeclares:       nil,
			wantRemainingQuery: "",
		},
		{
			name:               "whitespace only",
			query:              "   \n  \t  ",
			wantDeclares:       nil,
			wantRemainingQuery: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			declares, remaining := extractDeclareStatements(tt.query)
			assert.Equal(t, tt.wantDeclares, declares)
			assert.Equal(t, tt.wantRemainingQuery, remaining)
		})
	}
}
