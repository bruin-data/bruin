package sail

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/autocreate"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ autocreate.RelationChecker = (*Client)(nil)

var showTablesColumns = []string{"namespace", "tableName", "isTemporary"}

type expectedQuery struct {
	query string
	rows  *sqlmock.Rows
	err   error
}

func TestSparkSQLRelationExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		parts     []string
		queries   []expectedQuery
		want      bool
		wantError string
	}{
		{
			name:  "one-part name is looked up in the current namespace",
			parts: []string{"events"},
			queries: []expectedQuery{
				{query: "SHOW TABLES", rows: sqlmock.NewRows(showTablesColumns).AddRow("default", "events", false)},
			},
			want: true,
		},
		{
			name:  "schema.table",
			parts: []string{"analytics", "events"},
			queries: []expectedQuery{
				{query: "SHOW TABLES IN `analytics`", rows: sqlmock.NewRows(showTablesColumns).AddRow("analytics", "other", false).AddRow("analytics", "events", false)},
			},
			want: true,
		},
		{
			name:  "catalog.schema.table",
			parts: []string{"lake", "analytics", "events"},
			queries: []expectedQuery{
				{query: "SHOW TABLES IN `lake`.`analytics`", rows: sqlmock.NewRows(showTablesColumns).AddRow("analytics", "events", false)},
			},
			want: true,
		},
		{
			name:  "names are compared case-insensitively",
			parts: []string{"Analytics", "Events"},
			queries: []expectedQuery{
				{query: "SHOW TABLES IN `Analytics`", rows: sqlmock.NewRows(showTablesColumns).AddRow("analytics", "events", false)},
			},
			want: true,
		},
		{
			name:  "views and temporary views count",
			parts: []string{"analytics", "events"},
			queries: []expectedQuery{
				{query: "SHOW TABLES IN `analytics`", rows: sqlmock.NewRows(showTablesColumns).AddRow("", "events", true)},
			},
			want: true,
		},
		{
			name:  "Sail names the namespace column database",
			parts: []string{"analytics", "events"},
			queries: []expectedQuery{
				{query: "SHOW TABLES IN `analytics`", rows: sqlmock.NewRows([]string{"database", "tableName", "isTemporary"}).AddRow("analytics", "events", false)},
			},
			want: true,
		},
		{
			name:  "missing table",
			parts: []string{"analytics", "events"},
			queries: []expectedQuery{
				{query: "SHOW TABLES IN `analytics`", rows: sqlmock.NewRows(showTablesColumns).AddRow("analytics", "other", false)},
			},
			want: false,
		},
		{
			name:  "a namespace named like the table does not count",
			parts: []string{"events", "events"},
			queries: []expectedQuery{
				{query: "SHOW TABLES IN `events`", rows: sqlmock.NewRows(showTablesColumns).AddRow("events", "other", false)},
			},
			want: false,
		},
		{
			name:  "backticks are escaped",
			parts: []string{"ana`lytics", "events"},
			queries: []expectedQuery{
				{query: "SHOW TABLES IN `ana``lytics`", rows: sqlmock.NewRows(showTablesColumns)},
			},
			want: false,
		},
		{
			name:  "missing schema",
			parts: []string{"analytics", "events"},
			queries: []expectedQuery{
				{query: "SHOW TABLES IN `analytics`", err: errors.New("[SCHEMA_NOT_FOUND] The schema `analytics` cannot be found")},
				{query: "SHOW NAMESPACES", rows: sqlmock.NewRows([]string{"namespace"}).AddRow("default").AddRow("staging")},
			},
			want: false,
		},
		{
			name:  "missing schema in a catalog",
			parts: []string{"lake", "analytics", "events"},
			queries: []expectedQuery{
				{query: "SHOW TABLES IN `lake`.`analytics`", err: errors.New("Namespace does not exist: analytics")},
				{query: "SHOW NAMESPACES IN `lake`", rows: sqlmock.NewRows([]string{"namespace"}).AddRow("staging")},
			},
			want: false,
		},
		{
			name:  "SHOW TABLES failing for a listed schema is an error",
			parts: []string{"analytics", "events"},
			queries: []expectedQuery{
				{query: "SHOW TABLES IN `analytics`", err: errors.New("permission denied")},
				{query: "SHOW NAMESPACES", rows: sqlmock.NewRows([]string{"namespace"}).AddRow("ANALYTICS")},
				{query: "SHOW TABLES IN `analytics`", err: errors.New("permission denied again")},
			},
			wantError: "permission denied",
		},
		{
			name:  "schema created between the two checks is listed again",
			parts: []string{"analytics", "events"},
			queries: []expectedQuery{
				{query: "SHOW TABLES IN `analytics`", err: errors.New("[SCHEMA_NOT_FOUND] The schema `analytics` cannot be found")},
				{query: "SHOW NAMESPACES", rows: sqlmock.NewRows([]string{"namespace"}).AddRow("analytics")},
				{query: "SHOW TABLES IN `analytics`", rows: sqlmock.NewRows(showTablesColumns)},
			},
			want: false,
		},
		{
			name:  "a table found when listing again counts",
			parts: []string{"analytics", "events"},
			queries: []expectedQuery{
				{query: "SHOW TABLES IN `analytics`", err: errors.New("connection reset")},
				{query: "SHOW NAMESPACES", rows: sqlmock.NewRows([]string{"namespace"}).AddRow("analytics")},
				{query: "SHOW TABLES IN `analytics`", rows: sqlmock.NewRows(showTablesColumns).AddRow("analytics", "events", false)},
			},
			want: true,
		},
		{
			name:  "nested namespaces are listed with their full path",
			parts: []string{"lake", "analytics", "events"},
			queries: []expectedQuery{
				{query: "SHOW TABLES IN `lake`.`analytics`", err: errors.New("timeout")},
				{query: "SHOW NAMESPACES IN `lake`", rows: sqlmock.NewRows([]string{"namespace"}).AddRow("lake.analytics")},
				{query: "SHOW TABLES IN `lake`.`analytics`", err: errors.New("timeout")},
			},
			wantError: "timeout",
		},
		{
			name:  "failing to list namespaces keeps the original error",
			parts: []string{"analytics", "events"},
			queries: []expectedQuery{
				{query: "SHOW TABLES IN `analytics`", err: errors.New("connection reset")},
				{query: "SHOW NAMESPACES", err: errors.New("still down")},
			},
			wantError: "connection reset",
		},
		{
			name:  "one-part name errors are returned",
			parts: []string{"events"},
			queries: []expectedQuery{
				{query: "SHOW TABLES", err: errors.New("connection reset")},
			},
			wantError: "connection reset",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer db.Close()

			for _, q := range tt.queries {
				expectation := mock.ExpectQuery(q.query)
				if q.err != nil {
					expectation.WillReturnError(q.err)
				} else {
					expectation.WillReturnRows(q.rows)
				}
			}

			got, err := SparkSQLRelationExists(context.Background(), db, tt.parts)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClient_RelationExists(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SHOW TABLES IN `analytics`").
		WillReturnRows(sqlmock.NewRows([]string{"database", "tableName", "isTemporary"}).AddRow("analytics", "events", false))

	client := &Client{connection: db}
	exists, err := client.RelationExists(context.Background(), "analytics.events")
	require.NoError(t, err)
	assert.True(t, exists)
	require.NoError(t, mock.ExpectationsWereMet())

	_, err = client.RelationExists(context.Background(), "lake.analytics.events")
	require.ErrorContains(t, err, "table name must be in format")
	_, err = client.RelationExists(context.Background(), "analytics..events")
	require.ErrorContains(t, err, "empty component")
}

// TestMaterializer_FirstRunCreatesTable checks that the first run of every
// incremental strategy Sail supports builds the table without assuming that it
// exists.
func TestMaterializer_FirstRunCreatesTable(t *testing.T) {
	t.Parallel()

	strategies := []pipeline.Materialization{
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyAppend},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyDeleteInsert, IncrementalKey: "id"},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyTruncateInsert},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyTimeInterval, IncrementalKey: "dt", TimeGranularity: pipeline.MaterializationTimeGranularityDate},
	}

	for _, mat := range strategies {
		t.Run(string(mat.Strategy), func(t *testing.T) {
			t.Parallel()

			asset := &pipeline.Asset{Name: "analytics.events", Materialization: mat}
			got, err := NewMaterializer(false).Render(asset.FirstRun(), "SELECT 1 AS id")
			require.NoError(t, err)
			assert.Equal(t, "DROP TABLE IF EXISTS `analytics`.`events`;\nCREATE TABLE `analytics`.`events` AS\nSELECT 1 AS id;", strings.TrimSpace(got))
		})
	}
}
