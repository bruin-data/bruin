package mssql

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/autocreate"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ autocreate.RelationChecker = (*DB)(nil)

type staticConnections map[string]any

func (c staticConnections) GetConnection(name string) any {
	return c[name]
}

func newMockDB(t *testing.T, matcher sqlmock.QueryMatcher) (*DB, sqlmock.Sqlmock) { //nolint:ireturn // sqlmock only exposes its mock as an interface.
	t.Helper()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	require.NoError(t, err)
	t.Cleanup(func() { _ = mockDB.Close() })

	return &DB{conn: sqlx.NewDb(mockDB, "sqlmock")}, mock
}

// OBJECT_ID resolves the name on the server: default schema and then dbo for a
// bare name, the session's database, collation case rules, views and synonyms,
// and NULL for a missing schema or database. The client has to pass the name as
// written and bracket-quoted, escaped, and map the result.
func TestDB_RelationExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		table      string
		wantQuery  string
		result     int
		want       bool
		queryError error
		wantErr    string
	}{
		{
			name:      "schema.table exists",
			table:     "dbo.orders",
			wantQuery: "SELECT CASE WHEN OBJECT_ID(N'dbo.orders') IS NULL AND OBJECT_ID(N'[dbo].[orders]') IS NULL THEN 0 ELSE 1 END",
			result:    1,
			want:      true,
		},
		{
			name:      "one-part name resolved by the server",
			table:     "orders",
			wantQuery: "SELECT CASE WHEN OBJECT_ID(N'orders') IS NULL AND OBJECT_ID(N'[orders]') IS NULL THEN 0 ELSE 1 END",
			result:    1,
			want:      true,
		},
		{
			name:      "database.schema.table",
			table:     "sales.dbo.orders",
			wantQuery: "SELECT CASE WHEN OBJECT_ID(N'sales.dbo.orders') IS NULL AND OBJECT_ID(N'[sales].[dbo].[orders]') IS NULL THEN 0 ELSE 1 END",
			result:    1,
			want:      true,
		},
		{
			name:      "missing object, schema or database",
			table:     "nowhere.orders",
			wantQuery: "SELECT CASE WHEN OBJECT_ID(N'nowhere.orders') IS NULL AND OBJECT_ID(N'[nowhere].[orders]') IS NULL THEN 0 ELSE 1 END",
			result:    0,
			want:      false,
		},
		{
			name:      "a name that needs quoting is checked quoted as well",
			table:     "dbo.my-table",
			wantQuery: "SELECT CASE WHEN OBJECT_ID(N'dbo.my-table') IS NULL AND OBJECT_ID(N'[dbo].[my-table]') IS NULL THEN 0 ELSE 1 END",
			result:    1,
			want:      true,
		},
		{
			name:      "quotes and brackets are escaped",
			table:     "dbo.o'brien]",
			wantQuery: "SELECT CASE WHEN OBJECT_ID(N'dbo.o''brien]') IS NULL AND OBJECT_ID(N'[dbo].[o''brien]]]') IS NULL THEN 0 ELSE 1 END",
			result:    0,
			want:      false,
		},
		{
			name:       "query errors are returned",
			table:      "other_db.dbo.orders",
			wantQuery:  "SELECT CASE WHEN OBJECT_ID(N'other_db.dbo.orders') IS NULL AND OBJECT_ID(N'[other_db].[dbo].[orders]') IS NULL THEN 0 ELSE 1 END",
			queryError: errors.New("The server principal is not able to access the database"),
			wantErr:    "failed to check whether table 'other_db.dbo.orders' exists: The server principal is not able to access the database",
		},
		{
			name:    "more than three parts",
			table:   "server.db.dbo.orders",
			wantErr: "table name must be in format `table`, `schema.table`, or `database.schema.table`, \"server.db.dbo.orders\" given",
		},
		{
			name:    "empty component",
			table:   "db..orders",
			wantErr: "table name \"db..orders\" contains an empty component",
		},
		{
			name:    "empty name",
			table:   "",
			wantErr: "table name \"\" contains an empty component",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock := newMockDB(t, sqlmock.QueryMatcherEqual)
			if tt.wantQuery != "" {
				expectation := mock.ExpectQuery(tt.wantQuery).WithoutArgs()
				if tt.queryError != nil {
					expectation.WillReturnError(tt.queryError)
				} else {
					expectation.WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(tt.result))
				}
			}

			got, err := db.RelationExists(t.Context(), tt.table)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestAutoCreateFirstRun runs the wrapped operator for every incremental
// strategy twice: while the table is missing the asset is built with
// create+replace, once it exists the strategy loads into it.
func TestAutoCreateFirstRun(t *testing.T) {
	t.Parallel()

	const itemsExistsQuery = "OBJECT_ID(N'dbo.items') IS NULL AND OBJECT_ID(N'[dbo].[items]') IS NULL"

	columns := []pipeline.Column{
		{Name: "id", PrimaryKey: true},
		{Name: "label"},
		{Name: "updated_at"},
	}
	typedColumns := []pipeline.Column{
		{Name: "id", Type: "INT", PrimaryKey: true},
		{Name: "label", Type: "NVARCHAR(100)"},
		{Name: "updated_at", Type: "DATETIME2"},
	}

	tests := []struct {
		name            string
		strategy        pipeline.MaterializationStrategy
		columns         []pipeline.Column
		incrementalKey  string
		timeGranularity pipeline.MaterializationTimeGranularity
		firstRun        string
		secondRun       string
	}{
		{name: "append", strategy: pipeline.MaterializationStrategyAppend, columns: columns, firstRun: "SELECT tmp.* INTO dbo.items FROM", secondRun: "INSERT INTO dbo.items SELECT"},
		{name: "merge", strategy: pipeline.MaterializationStrategyMerge, columns: columns, firstRun: "SELECT tmp.* INTO dbo.items FROM", secondRun: "MERGE INTO dbo.items target"},
		{name: "merge with typed columns", strategy: pipeline.MaterializationStrategyMerge, columns: typedColumns, firstRun: "CREATE TABLE [dbo].[items] (", secondRun: "MERGE INTO dbo.items target"},
		{name: "delete+insert", strategy: pipeline.MaterializationStrategyDeleteInsert, columns: columns, incrementalKey: "id", firstRun: "SELECT tmp.* INTO dbo.items FROM", secondRun: "DELETE FROM dbo.items WHERE id in"},
		{name: "truncate+insert", strategy: pipeline.MaterializationStrategyTruncateInsert, columns: columns, firstRun: "SELECT tmp.* INTO dbo.items FROM", secondRun: "TRUNCATE TABLE dbo.items"},
		{name: "time_interval", strategy: pipeline.MaterializationStrategyTimeInterval, columns: columns, incrementalKey: "updated_at", timeGranularity: pipeline.MaterializationTimeGranularityTimestamp, firstRun: "SELECT tmp.* INTO dbo.items FROM", secondRun: "DELETE FROM dbo.items WHERE updated_at BETWEEN '2024-01-01T00:00:00.000000Z'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			asset := &pipeline.Asset{
				Name:       "dbo.items",
				Type:       pipeline.AssetTypeMsSQLQuery,
				Connection: "ms",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        tt.strategy,
					IncrementalKey:  tt.incrementalKey,
					TimeGranularity: tt.timeGranularity,
				},
				Columns: tt.columns,
				ExecutableFile: pipeline.ExecutableFile{
					Content: "SELECT 1 AS id, 'a' AS label, CAST('2024-01-01 10:00:00' AS DATETIME2) AS updated_at",
				},
			}

			db, mock := newMockDB(t, sqlmock.QueryMatcherFunc(containsMatcher))
			mock.ExpectQuery(itemsExistsQuery).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(0))
			mock.ExpectExec(tt.firstRun).WillReturnResult(sqlmock.NewResult(0, 1))
			out := runAutoCreate(t, db, asset)
			assert.Contains(t, out, "Table 'dbo.items' does not exist yet, creating it from the query.")

			mock.ExpectQuery(itemsExistsQuery).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))
			mock.ExpectExec(tt.secondRun).WillReturnResult(sqlmock.NewResult(0, 1))
			out = runAutoCreate(t, db, asset)
			assert.NotContains(t, out, "does not exist yet")

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func containsMatcher(expectedSQL, actualSQL string) error {
	if !strings.Contains(actualSQL, expectedSQL) {
		return errors.New("query does not contain " + expectedSQL + ":\n" + actualSQL)
	}
	return nil
}

// runAutoCreate runs the asset through the operator bruin run registers for
// ms.sql assets and returns what it printed.
func runAutoCreate(t *testing.T, db *DB, asset *pipeline.Asset) string {
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

	connections := staticConnections{"ms": db}
	extractor := &query.WholeFileExtractor{
		Fs:       afero.NewMemMapFs(),
		Renderer: jinja.NewRendererWithStartEndDates(&start, &end, &start, "p", "run", nil),
	}
	parser, err := sqlparser.NewSQLParser(false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = parser.Close() })

	materializer := pipeline.HookWrapperMaterializer{Mat: NewMaterializer(false)}
	op := autocreate.Wrap(NewBasicOperator(connections, extractor, materializer, parser), connections)

	ti := &scheduler.AssetInstance{Pipeline: &pipeline.Pipeline{Name: "p"}, Asset: asset}
	require.NoError(t, op.Run(ctx, ti))

	return out.String()
}
