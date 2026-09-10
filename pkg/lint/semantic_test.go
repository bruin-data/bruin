package lint

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetRulesIncludesSemanticLayerValid(t *testing.T) {
	t.Parallel()

	rules, err := GetRules(afero.NewMemMapFs(), nil, false, nil, false)
	require.NoError(t, err)

	var found Rule
	for _, rule := range rules {
		if rule.Name() == "semantic-layer-valid" {
			found = rule
			break
		}
	}
	require.NotNil(t, found)
	assert.True(t, found.IsFast())
	assert.Equal(t, ValidatorSeverityCritical, found.GetSeverity())
	assert.Contains(t, found.GetApplicableLevels(), LevelPipeline)
}

func TestSemanticLayerValid(t *testing.T) {
	t.Parallel()

	validTableModel := []byte(`schema: v1
name: sales
source:
  table: analytics.orders
metrics:
  - name: revenue
    expression: sum(amount)
`)
	validQueryModel := []byte(`schema: v1
name: filtered_orders
source:
  query: |
    select * from analytics.orders
    where deleted_at is null
metrics:
  - name: revenue
    expression: sum(amount)
`)
	validParenModel := []byte(`schema: v1
name: paren_orders
source:
  table: |
    (
      select * from analytics.orders
      where deleted_at is null
    ) as paren_orders
metrics:
  - name: revenue
    expression: sum(amount)
`)
	invalidBothModel := []byte(`schema: v1
name: both
source:
  table: analytics.orders
  query: select 1
`)
	invalidEmptyModel := []byte(`schema: v1
name: empty
source: {}
`)

	tests := []struct {
		name        string
		files       map[string][]byte
		pipeline    *pipeline.Pipeline
		ctx         context.Context
		wantCount   int
		wantContain []string
	}{
		{
			name: "no semantic directory",
			files: map[string][]byte{
				"/project/.bruin.yml": []byte("environments:\n  default:\n    connections: {}\n"),
			},
			pipeline: &pipeline.Pipeline{
				DefinitionFile: pipeline.DefinitionFile{Path: "/project/pipelines/daily/pipeline.yml"},
			},
			wantCount: 0,
		},
		{
			name: "valid table, query, and parenthesized sources",
			files: map[string][]byte{
				"/project/.bruin.yml":                         []byte("environments:\n  default:\n    connections: {}\n"),
				"/project/semantic/sales.yml":                 validTableModel,
				"/project/semantic/filtered.yml":              validQueryModel,
				"/project/semantic/commerce/paren_orders.yml": validParenModel,
			},
			pipeline: &pipeline.Pipeline{
				DefinitionFile: pipeline.DefinitionFile{Path: "/project/pipelines/daily/pipeline.yml"},
			},
			wantCount: 0,
		},
		{
			name: "rejects table and query together",
			files: map[string][]byte{
				"/project/.bruin.yml":        []byte("environments:\n  default:\n    connections: {}\n"),
				"/project/semantic/both.yml": invalidBothModel,
			},
			pipeline: &pipeline.Pipeline{
				DefinitionFile: pipeline.DefinitionFile{Path: "/project/pipelines/daily/pipeline.yml"},
			},
			wantCount:   1,
			wantContain: []string{"both", "invalid"},
		},
		{
			name: "rejects empty source",
			files: map[string][]byte{
				"/project/.bruin.yml":         []byte("environments:\n  default:\n    connections: {}\n"),
				"/project/semantic/empty.yml": invalidEmptyModel,
			},
			pipeline: &pipeline.Pipeline{
				DefinitionFile: pipeline.DefinitionFile{Path: "/project/pipelines/daily/pipeline.yml"},
			},
			wantCount:   1,
			wantContain: []string{"empty", "invalid"},
		},
		{
			name: "uses config file path from context",
			files: map[string][]byte{
				"/other/.bruin.yml":           []byte("environments:\n  default:\n    connections: {}\n"),
				"/other/semantic/broken.yml":  invalidEmptyModel,
				"/project/.bruin.yml":         []byte("environments:\n  default:\n    connections: {}\n"),
				"/project/semantic/sales.yml": validTableModel,
			},
			pipeline: &pipeline.Pipeline{
				DefinitionFile: pipeline.DefinitionFile{Path: "/project/pipelines/daily/pipeline.yml"},
			},
			ctx:         context.WithValue(context.Background(), config.ConfigFilePathContextKey, "/other/.bruin.yml"),
			wantCount:   1,
			wantContain: []string{"broken.yml"},
		},
		{
			name: "reports only once for the same catalog",
			files: map[string][]byte{
				"/project/.bruin.yml":         []byte("environments:\n  default:\n    connections: {}\n"),
				"/project/semantic/empty.yml": invalidEmptyModel,
			},
			pipeline: &pipeline.Pipeline{
				DefinitionFile: pipeline.DefinitionFile{Path: "/project/pipelines/daily/pipeline.yml"},
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs := afero.NewMemMapFs()
			for path, body := range tt.files {
				require.NoError(t, afero.WriteFile(fs, path, body, 0o644))
			}

			checker := &semanticLayerChecker{fs: fs}
			ctx := tt.ctx
			if ctx == nil {
				ctx = t.Context()
			}

			got, err := checker.Validate(ctx, tt.pipeline)
			require.NoError(t, err)
			assert.Len(t, got, tt.wantCount)
			for _, want := range tt.wantContain {
				require.NotEmpty(t, got)
				assert.Contains(t, got[0].Description, want)
			}

			if tt.name == "reports only once for the same catalog" {
				second, err := checker.Validate(ctx, tt.pipeline)
				require.NoError(t, err)
				assert.Empty(t, second)
			}
		})
	}
}

func TestSemanticQueryDryRun(t *testing.T) {
	t.Parallel()

	queryModel := []byte(`schema: v1
name: filtered_orders
source:
  query: select * from analytics.orders where deleted_at is null
metrics:
  - name: revenue
    expression: sum(amount)
`)
	parenModel := []byte(`schema: v1
name: paren_orders
source:
  table: |
    (
      select * from analytics.orders
    ) as paren_orders
metrics:
  - name: revenue
    expression: sum(amount)
`)
	tableModel := []byte(`schema: v1
name: sales
source:
  table: analytics.orders
metrics:
  - name: revenue
    expression: sum(amount)
`)

	sqlAsset := &pipeline.Asset{
		Name: "orders",
		Type: pipeline.AssetTypeDuckDBQuery,
	}
	p := &pipeline.Pipeline{
		DefinitionFile: pipeline.DefinitionFile{Path: "/project/pipelines/daily/pipeline.yml"},
		Assets:         []*pipeline.Asset{sqlAsset},
	}

	writeCatalog := func(t *testing.T, fs afero.Fs, models map[string][]byte) {
		t.Helper()
		require.NoError(t, afero.WriteFile(fs, "/project/.bruin.yml", []byte("environments:\n  default:\n    connections: {}\n"), 0o644))
		for name, body := range models {
			require.NoError(t, afero.WriteFile(fs, filepath.Join("/project/semantic", name), body, 0o644))
		}
	}

	t.Run("skips plain table sources", func(t *testing.T) {
		t.Parallel()

		fs := afero.NewMemMapFs()
		writeCatalog(t, fs, map[string][]byte{"sales.yml": tableModel})
		validator := &recordingQueryValidator{valid: true}
		runner := &semanticQueryDryRunner{
			fs:          fs,
			connections: &fakeConnectionManager{validator: validator},
		}

		issues, err := runner.Validate(t.Context(), p)
		require.NoError(t, err)
		assert.Empty(t, issues)
		assert.Empty(t, validator.queries)
	})

	t.Run("dry-runs query and parenthesized table sources", func(t *testing.T) {
		t.Parallel()

		fs := afero.NewMemMapFs()
		writeCatalog(t, fs, map[string][]byte{
			"filtered.yml": queryModel,
			"paren.yml":    parenModel,
			"sales.yml":    tableModel,
		})
		validator := &recordingQueryValidator{valid: true}
		runner := &semanticQueryDryRunner{
			fs:          fs,
			connections: &fakeConnectionManager{validator: validator},
		}

		issues, err := runner.Validate(t.Context(), p)
		require.NoError(t, err)
		assert.Empty(t, issues)
		require.Len(t, validator.queries, 2)
		assert.Contains(t, validator.queries, "SELECT * FROM (select * from analytics.orders where deleted_at is null) AS filtered_orders")
		assert.Contains(t, validator.queries[0]+validator.queries[1], "SELECT * FROM (")
		assert.Contains(t, validator.queries[0]+validator.queries[1], ") as paren_orders")
	})

	t.Run("reports invalid query", func(t *testing.T) {
		t.Parallel()

		fs := afero.NewMemMapFs()
		writeCatalog(t, fs, map[string][]byte{"filtered.yml": queryModel})
		runner := &semanticQueryDryRunner{
			fs:          fs,
			connections: &fakeConnectionManager{validator: &fakeQueryValidator{isValid: false}},
		}

		issues, err := runner.Validate(t.Context(), p)
		require.NoError(t, err)
		require.Len(t, issues, 1)
		assert.Contains(t, issues[0].Description, `Semantic model "filtered_orders" query is invalid`)
	})

	t.Run("reports dry-run error", func(t *testing.T) {
		t.Parallel()

		fs := afero.NewMemMapFs()
		writeCatalog(t, fs, map[string][]byte{"filtered.yml": queryModel})
		runner := &semanticQueryDryRunner{
			fs:          fs,
			connections: &fakeConnectionManager{validator: &fakeQueryValidator{err: errors.New("syntax error")}},
		}

		issues, err := runner.Validate(t.Context(), p)
		require.NoError(t, err)
		require.Len(t, issues, 1)
		assert.Contains(t, issues[0].Description, `Failed to validate semantic model "filtered_orders" query`)
		assert.Contains(t, issues[0].Description, "syntax error")
	})

	t.Run("skips when pipeline has no query validator", func(t *testing.T) {
		t.Parallel()

		fs := afero.NewMemMapFs()
		writeCatalog(t, fs, map[string][]byte{"filtered.yml": queryModel})
		runner := &semanticQueryDryRunner{
			fs:          fs,
			connections: &fakeConnectionManager{validator: struct{}{}},
		}

		issues, err := runner.Validate(t.Context(), p)
		require.NoError(t, err)
		assert.Empty(t, issues)
	})

	t.Run("GetSemanticQueryDryRunRule is a slow pipeline rule", func(t *testing.T) {
		t.Parallel()

		rule := GetSemanticQueryDryRunRule(afero.NewMemMapFs(), nil, &fakeConnectionManager{})
		assert.Equal(t, "semantic-query-dry-run", rule.Name())
		assert.False(t, rule.IsFast())
		assert.Contains(t, rule.GetApplicableLevels(), LevelPipeline)
		assert.Equal(t, ValidatorSeverityCritical, rule.GetSeverity())
	})
}

type recordingQueryValidator struct {
	valid   bool
	err     error
	queries []string
}

func (r *recordingQueryValidator) IsValid(ctx context.Context, q *query.Query) (bool, error) {
	r.queries = append(r.queries, q.Query)
	return r.valid, r.err
}
