package synapse

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/autocreate"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// synapseClient stands in for the *mssql.DB that Synapse connections return,
// whose RelationExists is tested in the mssql package.
type synapseClient struct {
	exists  bool
	checked []string
	queries []string
}

func (c *synapseClient) RunQueryWithoutResult(_ context.Context, q *query.Query) error {
	c.queries = append(c.queries, q.Query)
	return nil
}

func (c *synapseClient) Select(context.Context, *query.Query) ([][]interface{}, error) {
	return nil, nil
}

func (c *synapseClient) RelationExists(_ context.Context, tableName string) (bool, error) {
	c.checked = append(c.checked, tableName)
	return c.exists, nil
}

type staticConnections map[string]any

func (c staticConnections) GetConnection(name string) any {
	return c[name]
}

// TestAutoCreateFirstRun runs the wrapped operator, with the list-based
// materializer bruin run uses for synapse.sql assets, for every incremental
// strategy: while the table is missing the asset is built with create+replace,
// once it exists the strategy loads into it.
func TestAutoCreateFirstRun(t *testing.T) {
	t.Parallel()

	columns := []pipeline.Column{
		{Name: "id", PrimaryKey: true},
		{Name: "label"},
		{Name: "updated_at"},
	}

	tests := []struct {
		strategy        pipeline.MaterializationStrategy
		incrementalKey  string
		timeGranularity pipeline.MaterializationTimeGranularity
		secondRun       string
	}{
		{strategy: pipeline.MaterializationStrategyAppend, secondRun: "INSERT INTO dbo.items SELECT"},
		{strategy: pipeline.MaterializationStrategyMerge, secondRun: "MERGE INTO dbo.items target"},
		{strategy: pipeline.MaterializationStrategyDeleteInsert, incrementalKey: "id", secondRun: "DELETE FROM dbo.items WHERE id in"},
		{strategy: pipeline.MaterializationStrategyTruncateInsert, secondRun: "TRUNCATE TABLE dbo.items"},
		{strategy: pipeline.MaterializationStrategyTimeInterval, incrementalKey: "updated_at", timeGranularity: pipeline.MaterializationTimeGranularityTimestamp, secondRun: "DELETE FROM dbo.items WHERE updated_at BETWEEN '2024-01-01T00:00:00.000000Z'"},
	}

	for _, tt := range tests {
		t.Run(string(tt.strategy), func(t *testing.T) {
			t.Parallel()

			asset := &pipeline.Asset{
				Name:       "dbo.items",
				Type:       pipeline.AssetTypeSynapseQuery,
				Connection: "synapse",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        tt.strategy,
					IncrementalKey:  tt.incrementalKey,
					TimeGranularity: tt.timeGranularity,
				},
				Columns: columns,
				ExecutableFile: pipeline.ExecutableFile{
					Content: "SELECT 1 AS id, 'a' AS label, CAST('2024-01-01 10:00:00' AS DATETIME2) AS updated_at",
				},
			}

			client := &synapseClient{}
			out := runAutoCreate(t, client, asset)
			assert.Contains(t, out, "Table 'dbo.items' does not exist yet, creating it from the query.")
			firstRun := strings.Join(client.queries, "\n")
			assert.Contains(t, firstRun, "IF OBJECT_ID('dbo.items', 'U') IS NOT NULL DROP TABLE dbo.items;")
			assert.Contains(t, firstRun, "SELECT * INTO dbo.items FROM #bruin_tmp_")
			assert.NotContains(t, firstRun, tt.secondRun)

			client.exists = true
			client.queries = nil
			out = runAutoCreate(t, client, asset)
			assert.NotContains(t, out, "does not exist yet")
			secondRun := strings.Join(client.queries, "\n")
			assert.Contains(t, secondRun, tt.secondRun)
			assert.NotContains(t, secondRun, "DROP TABLE dbo.items")

			assert.Equal(t, []string{"dbo.items", "dbo.items"}, client.checked)
		})
	}
}

func runAutoCreate(t *testing.T, client *synapseClient, asset *pipeline.Asset) string {
	t.Helper()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	var out bytes.Buffer
	ctx := context.WithValue(t.Context(), executor.KeyPrinter, &out)
	ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, start)
	ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, end)
	ctx = context.WithValue(ctx, pipeline.RunConfigExecutionDate, start)
	ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "run")
	ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, false)

	connections := staticConnections{"synapse": client}
	extractor := &query.WholeFileExtractor{
		Fs:       afero.NewMemMapFs(),
		Renderer: jinja.NewRendererWithStartEndDates(&start, &end, &start, "p", "run", nil),
	}
	parser, err := sqlparser.NewSQLParser(false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = parser.Close() })

	materializer := pipeline.HookWrapperMaterializerList{Mat: NewMaterializer(false)}
	op := autocreate.Wrap(NewBasicOperator(connections, extractor, materializer, parser), connections)

	ti := &scheduler.AssetInstance{Pipeline: &pipeline.Pipeline{Name: "p"}, Asset: asset}
	require.NoError(t, op.Run(ctx, ti))

	return out.String()
}

func TestMaterializer_RenderFirstRunRejectsUnsupportedStrategy(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "dbo.orders",
		Materialization: pipeline.Materialization{
			Type:     pipeline.MaterializationTypeTable,
			Strategy: pipeline.MaterializationStrategySCD2ByColumn,
		},
		Columns: []pipeline.Column{{Name: "id", Type: "INT", PrimaryKey: true}},
	}

	_, err := NewMaterializer(false).Render(asset.FirstRun(), "SELECT 1 AS id")
	require.EqualError(t, err, "unsupported materialization type - strategy combination: (`table` - `scd2_by_column`)")
}
