package spark

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/autocreate"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ autocreate.RelationChecker = (*Client)(nil)

func TestClient_RelationExists(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	client := &Client{connection: db}
	columns := []string{"namespace", "tableName", "isTemporary"}

	mock.ExpectQuery("SHOW TABLES IN `lake`.`analytics`").
		WillReturnRows(sqlmock.NewRows(columns).AddRow("analytics", "events", false))
	exists, err := client.RelationExists(context.Background(), "lake.analytics.events")
	require.NoError(t, err)
	assert.True(t, exists)

	mock.ExpectQuery("SHOW TABLES IN `analytics`").
		WillReturnError(errors.New("[SCHEMA_NOT_FOUND] The schema `analytics` cannot be found"))
	mock.ExpectQuery("SHOW NAMESPACES").
		WillReturnRows(sqlmock.NewRows([]string{"namespace"}).AddRow("default"))
	exists, err = client.RelationExists(context.Background(), "analytics.events")
	require.NoError(t, err)
	assert.False(t, exists)

	mock.ExpectQuery("SHOW TABLES").
		WillReturnRows(sqlmock.NewRows(columns).AddRow("default", "other", false))
	exists, err = client.RelationExists(context.Background(), "events")
	require.NoError(t, err)
	assert.False(t, exists)

	require.NoError(t, mock.ExpectationsWereMet())

	_, err = client.RelationExists(context.Background(), "a.b.c.d")
	require.ErrorContains(t, err, "table name must be in format")
}

// TestMaterializer_FirstRunCreatesTable checks that the first run of every
// incremental strategy builds the table without assuming that it exists.
func TestMaterializer_FirstRunCreatesTable(t *testing.T) {
	t.Parallel()

	columns := []pipeline.Column{
		{Name: "id", Type: "INT", PrimaryKey: true},
		{Name: "updated_at", Type: "TIMESTAMP"},
	}
	strategies := []pipeline.Materialization{
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyAppend},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyMerge},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyDeleteInsert, IncrementalKey: "id"},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyTruncateInsert},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyTimeInterval, IncrementalKey: "updated_at", TimeGranularity: pipeline.MaterializationTimeGranularityTimestamp},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategySCD2ByColumn},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategySCD2ByTime, IncrementalKey: "updated_at"},
	}

	for _, mat := range strategies {
		t.Run(string(mat.Strategy), func(t *testing.T) {
			t.Parallel()

			asset := &pipeline.Asset{Name: "lake.analytics.events", Materialization: mat, Columns: columns}
			got, err := NewMaterializer(false).Render(asset.FirstRun(), "SELECT 1 AS id, current_timestamp() AS updated_at")
			require.NoError(t, err)
			assert.True(
				t,
				strings.HasPrefix(got, "DROP TABLE IF EXISTS `lake`.`analytics`.`events`;\nCREATE TABLE `lake`.`analytics`.`events`\n"),
				"first run has to replace the table, got:\n%s", got,
			)
			assert.NotContains(t, got, "MERGE INTO")
		})
	}
}

var _ autocreate.FirstRunBlocker = BasicOperator{}

func TestBasicOperator_FirstRunBlocker(t *testing.T) {
	t.Parallel()

	header := "/* @bruin\nname: x\n@bruin */\n"
	tests := []struct {
		name    string
		asset   string
		content string
		blocked bool
	}{
		{name: "plain query", asset: "sales.orders", content: header + "SELECT 1 AS id"},
		{name: "SET CATALOG", asset: "sales.orders", content: header + "SET CATALOG lake;\nSELECT 1 AS id", blocked: true},
		{name: "default catalog setting", asset: "orders", content: header + "-- switch\nSET spark.sql.defaultCatalog = lake;\nSELECT 1 AS id", blocked: true},
		{name: "USE", asset: "sales.orders", content: header + "USE lake.sales;\nSELECT 1 AS id", blocked: true},
		{name: "RESET", asset: "sales.orders", content: header + "RESET;\nSELECT 1 AS id", blocked: true},
		{name: "three-part name", asset: "lake.sales.orders", content: header + "SET CATALOG lake;\nSELECT 1 AS id"},
		{name: "SET inside the query text", asset: "sales.orders", content: header + "SELECT 'SET CATALOG x' AS note"},
		{name: "SET from a template", asset: "sales.orders", content: header + "{% if true %}SET spark.sql.defaultCatalog = lake;{% endif %}\nSELECT 1 AS id", blocked: true},
		{name: "SET only on a full refresh", asset: "sales.orders", content: header + "{% if full_refresh %}SET CATALOG lake;{% endif %}\nSELECT 1 AS id", blocked: true},
		{name: "SET only on incremental runs", asset: "sales.orders", content: header + "{% if not full_refresh %}SET CATALOG lake;{% endif %}\nSELECT 1 AS id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			asset := &pipeline.Asset{Name: tt.asset, ExecutableFile: pipeline.ExecutableFile{Content: tt.content}}
			start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			ctx := context.WithValue(context.Background(), pipeline.RunConfigStartDate, start)
			ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, start)
			ctx = context.WithValue(ctx, pipeline.RunConfigExecutionDate, start)
			ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "run")
			ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, true)
			op := BasicOperator{extractor: &query.FileQuerySplitterExtractor{
				Fs:                        afero.NewMemMapFs(),
				Renderer:                  jinja.NewRendererWithStartEndDates(&start, &start, &start, "p", "run", nil),
				PreserveSessionStatements: true,
				BackslashEscapes:          true,
			}}
			reason := op.FirstRunBlocker(ctx, &pipeline.Pipeline{Name: "p"}, asset.FirstRun())
			if tt.blocked {
				require.Contains(t, reason, "session statements")
			} else {
				require.Empty(t, reason)
			}
		})
	}
}
