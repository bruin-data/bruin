package pipeline

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubHoister is a test double for DeclareHoister that captures the inputs
// it was called with and returns the configured response. It does not parse
// SQL — the real sqlglot-backed behavior is verified in pkg/sqlparser.
type stubHoister struct {
	capturedSQL     string
	capturedList    []string
	returnSQL       string
	returnList      []string
	returnErr       error
	calledHoist     bool
	calledHoistList bool
}

func (s *stubHoister) HoistDeclares(sql string, _ AssetType) (string, error) {
	s.calledHoist = true
	s.capturedSQL = sql
	if s.returnErr != nil {
		return sql, s.returnErr
	}
	if s.returnSQL != "" {
		return s.returnSQL, nil
	}
	return sql, nil
}

func (s *stubHoister) HoistDeclaresList(queries []string, _ AssetType) ([]string, error) {
	s.calledHoistList = true
	s.capturedList = queries
	if s.returnErr != nil {
		return queries, s.returnErr
	}
	if s.returnList != nil {
		return s.returnList, nil
	}
	return queries, nil
}

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
			// nil hoister: verifies the pure join behavior independent of sqlglot.
			assert.Equal(t, tt.want, WrapHooks(tt.query, tt.hooks, nil, AssetTypeBigqueryQuery))
		})
	}
}

func TestWrapHooks_DelegatesToHoister(t *testing.T) {
	t.Parallel()

	hoister := &stubHoister{returnSQL: "HOISTED OUTPUT"}
	got := WrapHooks("select 9", Hooks{
		Pre:  []Hook{{Query: "DECLARE x INT64"}},
		Post: []Hook{{Query: "select 2"}},
	}, hoister, AssetTypeBigqueryQuery)

	require.True(t, hoister.calledHoist)
	// Hoister should receive the fully joined script before reordering.
	assert.Equal(t, "DECLARE x INT64;\nselect 9;\nselect 2;", hoister.capturedSQL)
	// The hoister's return value is what callers see.
	assert.Equal(t, "HOISTED OUTPUT", got)
}

func TestWrapHooks_FallsBackOnHoisterError(t *testing.T) {
	t.Parallel()

	hoister := &stubHoister{returnErr: errors.New("python crashed")}
	joined := "DECLARE x INT64;\nselect 9;"
	got := WrapHooks("select 9", Hooks{
		Pre: []Hook{{Query: "DECLARE x INT64"}},
	}, hoister, AssetTypeBigqueryQuery)

	require.True(t, hoister.calledHoist)
	assert.Equal(t, joined, got)
}

func TestWrapHooks_SkipsHoisterWhenNoHooks(t *testing.T) {
	t.Parallel()

	// With no hooks AND no hoister, the input is returned unchanged.
	got := WrapHooks("select 1", Hooks{}, nil, AssetTypeBigqueryQuery)
	assert.Equal(t, "select 1", got)
}

func TestWrapHooks_CallsHoisterEvenWithoutHooks(t *testing.T) {
	t.Parallel()

	// A materialization can produce its own DECLAREs even with no hooks
	// configured. The hoister must still run on the bare query.
	hoister := &stubHoister{returnSQL: "REORDERED"}
	got := WrapHooks("DECLARE x INT64;\nSET x = 1;", Hooks{}, hoister, AssetTypeBigqueryQuery)
	require.True(t, hoister.calledHoist)
	assert.Equal(t, "REORDERED", got)
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
			assert.Equal(t, tt.want, wrapHookQueriesList(tt.queries, tt.hooks, nil, AssetTypeBigqueryQuery))
		})
	}
}

func TestWrapHookQueriesList_DelegatesToHoister(t *testing.T) {
	t.Parallel()

	hoister := &stubHoister{returnList: []string{"DECLARE y;", "SET x = 1;", "select 1"}}
	got := wrapHookQueriesList(
		[]string{"select 1"},
		Hooks{
			Pre: []Hook{{Query: "SET x = 1"}, {Query: "DECLARE y"}},
		},
		hoister,
		AssetTypeBigqueryQuery,
	)

	require.True(t, hoister.calledHoistList)
	assert.Equal(t, []string{"SET x = 1;", "DECLARE y;", "select 1"}, hoister.capturedList)
	assert.Equal(t, []string{"DECLARE y;", "SET x = 1;", "select 1"}, got)
}

func TestWrapHookQueriesList_FallsBackOnHoisterError(t *testing.T) {
	t.Parallel()

	hoister := &stubHoister{returnErr: errors.New("boom")}
	got := wrapHookQueriesList(
		[]string{"select 1"},
		Hooks{Pre: []Hook{{Query: "DECLARE y"}}},
		hoister,
		AssetTypeBigqueryQuery,
	)

	require.True(t, hoister.calledHoistList)
	assert.Equal(t, []string{"DECLARE y;", "select 1"}, got)
}

// Confirm AssetType reaches the hoister. Important for dialect routing on the
// Python side.
func TestWrapHooks_PassesAssetType(t *testing.T) {
	t.Parallel()

	var capturedType AssetType
	hoister := &recordingHoister{
		onHoist: func(sql string, t AssetType) (string, error) {
			capturedType = t
			return sql, nil
		},
	}

	WrapHooks("select 1", Hooks{Pre: []Hook{{Query: "select 0"}}}, hoister, AssetTypeSnowflakeQuery)
	assert.Equal(t, AssetTypeSnowflakeQuery, capturedType)
}

type recordingHoister struct {
	onHoist     func(string, AssetType) (string, error)
	onHoistList func([]string, AssetType) ([]string, error)
}

func (r *recordingHoister) HoistDeclares(sql string, t AssetType) (string, error) {
	if r.onHoist != nil {
		return r.onHoist(sql, t)
	}
	return sql, nil
}

func (r *recordingHoister) HoistDeclaresList(q []string, t AssetType) ([]string, error) {
	if r.onHoistList != nil {
		return r.onHoistList(q, t)
	}
	return q, nil
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
