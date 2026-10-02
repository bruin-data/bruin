package lint

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	semantic "github.com/bruin-data/bruin/semantic-engine"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type semanticFakeValidator struct {
	valid bool
	err   error
	calls int
}

func (f *semanticFakeValidator) IsValid(_ context.Context, _ *query.Query) (bool, error) {
	f.calls++
	return f.valid, f.err
}

type semanticFakeConnManager struct {
	conns map[string]any
}

func (f *semanticFakeConnManager) GetConnection(name string) any {
	return f.conns[name]
}

func testLogger() *zap.SugaredLogger {
	return zap.NewNop().Sugar()
}

const semanticTestModel = `schema: v1
name: orders
source:
  table: analytics.orders
dimensions:
  - name: country
    type: string
  - name: order_date
    type: time
    granularities:
      day: date_trunc('day', order_date)
metrics:
  - name: revenue
    expression: sum(amount)
segments:
  - name: completed
    filter: "status = 'completed'"
`

const semanticTestDir = "/repo/semantic"

func writeSemanticModel(t *testing.T, fs afero.Fs, filename, content string) {
	t.Helper()
	require.NoError(t, fs.MkdirAll(semanticTestDir, 0o755))
	require.NoError(t, afero.WriteFile(fs, filepath.Join(semanticTestDir, filename), []byte(content), 0o644))
}

func semanticTestPipeline() *pipeline.Pipeline {
	asset := &pipeline.Asset{
		Name: "orders",
		Type: pipeline.AssetTypeBigqueryQuery,
	}
	return &pipeline.Pipeline{
		Name:   "test-pipeline",
		Assets: []*pipeline.Asset{asset},
		DefaultConnections: map[string]string{
			"google_cloud_platform": "gcp-conn",
		},
	}
}

func TestSemanticQueryDryRunRule_NoSemanticDir(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	rule := &SemanticQueryDryRunRule{
		Connections: &semanticFakeConnManager{},
		Fs:          fs,
		SemanticDir: "/nonexistent/semantic",
		Logger:      testLogger(),
	}

	issues, err := rule.ValidateCrossPipeline(t.Context(), []*pipeline.Pipeline{semanticTestPipeline()})
	require.NoError(t, err)
	assert.Empty(t, issues)
}

func TestSemanticQueryDryRunRule_InvalidModelReported(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	writeSemanticModel(t, fs, "broken.yml", "name: broken\nsource: {}\n")

	rule := &SemanticQueryDryRunRule{
		Connections: &semanticFakeConnManager{},
		Fs:          fs,
		SemanticDir: "/repo/semantic",
		Logger:      testLogger(),
	}

	issues, err := rule.ValidateCrossPipeline(t.Context(), []*pipeline.Pipeline{semanticTestPipeline()})
	require.NoError(t, err)
	require.NotEmpty(t, issues)
	assert.Contains(t, issues[0].Description, "broken")
}

func TestSemanticQueryDryRunRule_ValidModelPassesDryRun(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	writeSemanticModel(t, fs, "orders.yml", semanticTestModel)

	validator := &semanticFakeValidator{valid: true}
	rule := &SemanticQueryDryRunRule{
		Connections: &semanticFakeConnManager{conns: map[string]any{"gcp-conn": validator}},
		Fs:          fs,
		SemanticDir: "/repo/semantic",
		Logger:      testLogger(),
	}

	issues, err := rule.ValidateCrossPipeline(t.Context(), []*pipeline.Pipeline{semanticTestPipeline()})
	require.NoError(t, err)
	assert.Empty(t, issues)
	assert.Positive(t, validator.calls, "expected at least one dry-run call")
}

func TestSemanticQueryDryRunRule_FailingDryRunReported(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	writeSemanticModel(t, fs, "orders.yml", semanticTestModel)

	validator := &semanticFakeValidator{valid: false, err: errors.New("column amount does not exist")}
	rule := &SemanticQueryDryRunRule{
		Connections: &semanticFakeConnManager{conns: map[string]any{"gcp-conn": validator}},
		Fs:          fs,
		SemanticDir: "/repo/semantic",
		Logger:      testLogger(),
	}

	issues, err := rule.ValidateCrossPipeline(t.Context(), []*pipeline.Pipeline{semanticTestPipeline()})
	require.NoError(t, err)
	require.NotEmpty(t, issues)
	assert.Contains(t, issues[0].Description, "orders")
	assert.Contains(t, issues[0].Description, "column amount does not exist")
}

func TestSemanticQueryDryRunRule_NoValidatorsSkipsDryRun(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	writeSemanticModel(t, fs, "orders.yml", semanticTestModel)

	// Pipeline without SQL assets means no validators can be resolved.
	rule := &SemanticQueryDryRunRule{
		Connections: &semanticFakeConnManager{},
		Fs:          fs,
		SemanticDir: "/repo/semantic",
		Logger:      testLogger(),
	}

	emptyPipeline := &pipeline.Pipeline{Name: "empty"}
	issues, err := rule.ValidateCrossPipeline(t.Context(), []*pipeline.Pipeline{emptyPipeline})
	require.NoError(t, err)
	assert.Empty(t, issues)
}

func TestSemanticQueryDryRunRule_PassesWhenAnyValidatorSucceeds(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	writeSemanticModel(t, fs, "orders.yml", semanticTestModel)

	failing := &semanticFakeValidator{valid: false, err: errors.New("not found")}
	passing := &semanticFakeValidator{valid: true}

	p := &pipeline.Pipeline{
		Name: "multi-conn",
		Assets: []*pipeline.Asset{
			{Name: "a1", Type: pipeline.AssetTypeBigqueryQuery, Connection: "conn-a"},
			{Name: "a2", Type: pipeline.AssetTypeSnowflakeQuery, Connection: "conn-b"},
		},
	}

	rule := &SemanticQueryDryRunRule{
		Connections: &semanticFakeConnManager{conns: map[string]any{
			"conn-a": failing,
			"conn-b": passing,
		}},
		Fs:          fs,
		SemanticDir: "/repo/semantic",
		Logger:      testLogger(),
	}

	issues, err := rule.ValidateCrossPipeline(t.Context(), []*pipeline.Pipeline{p})
	require.NoError(t, err)
	assert.Empty(t, issues)
}

func TestBuildSemanticValidationQueries_CoversModel(t *testing.T) {
	t.Parallel()

	model := &semantic.Model{
		Name:   "orders",
		Source: semantic.Source{Table: "analytics.orders"},
		Dimensions: []semantic.Dimension{
			{Name: "country", Type: "string"},
			{Name: "order_date", Type: "time", Granularities: map[string]string{"day": "date_trunc('day', order_date)"}},
		},
		Metrics: []semantic.Metric{
			{Name: "revenue", Expression: "sum(amount)"},
		},
		Segments: []semantic.Segment{
			{Name: "completed", Filter: "status = 'completed'"},
		},
	}
	models := map[string]*semantic.Model{"orders": model}

	queries, issues := buildSemanticValidationQueries(model, models)
	assert.Empty(t, issues)
	// comprehensive + 1 metric + 2 dimensions + 1 granularity + 1 segment = 6
	assert.Len(t, queries, 6)
	for _, q := range queries {
		assert.NotEmpty(t, q.sql)
		assert.NotEmpty(t, q.label)
	}
}

func TestBuildSemanticValidationQueries_JoinQuery(t *testing.T) {
	t.Parallel()

	orders := &semantic.Model{
		Name:   "orders",
		Source: semantic.Source{Table: "analytics.orders"},
		Joins: []semantic.Join{
			{Name: "customers", Relationship: "many_to_one", ForeignKey: "customer_id"},
		},
		Dimensions: []semantic.Dimension{{Name: "order_id", Type: "string"}},
		Metrics:    []semantic.Metric{{Name: "revenue", Expression: "sum(amount)"}},
	}
	customers := &semantic.Model{
		Name:       "customers",
		Source:     semantic.Source{Table: "analytics.customers"},
		PrimaryKey: "customer_id",
		Dimensions: []semantic.Dimension{{Name: "country", Type: "string"}},
	}
	models := map[string]*semantic.Model{"orders": orders, "customers": customers}

	queries, issues := buildSemanticValidationQueries(orders, models)
	assert.Empty(t, issues)

	foundJoin := false
	for _, q := range queries {
		if q.label == "join 'customers' query" {
			foundJoin = true
			assert.Contains(t, q.sql, "customers_country")
		}
	}
	assert.True(t, foundJoin, "expected a join validation query")
}

func TestSemanticDirFromConfigPath(t *testing.T) {
	t.Parallel()

	assert.Equal(t, filepath.Join("/repo", "semantic"), SemanticDirFromConfigPath("/repo/.bruin.yml"))
	assert.Empty(t, SemanticDirFromConfigPath(""))
}

func TestGetSemanticQueryDryRunRule(t *testing.T) {
	t.Parallel()

	rule := GetSemanticQueryDryRunRule(&semanticFakeConnManager{}, afero.NewMemMapFs(), "/repo/semantic", testLogger())
	assert.Equal(t, "semantic-query-dry-run", rule.Name())
	assert.False(t, rule.IsFast())
	assert.Equal(t, ValidatorSeverityCritical, rule.GetSeverity())
	assert.Contains(t, rule.GetApplicableLevels(), LevelCrossPipeline)
}
