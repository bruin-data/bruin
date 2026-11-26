package fabric

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

func newAutoCreateMockDB(t *testing.T, matcher sqlmock.QueryMatcher) (*DB, sqlmock.Sqlmock) { //nolint:ireturn // sqlmock only exposes its mock as an interface.
	t.Helper()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	require.NoError(t, err)
	t.Cleanup(func() { _ = mockDB.Close() })

	return &DB{conn: sqlx.NewDb(mockDB, "sqlmock"), config: &Config{}, schemaCreator: NewSchemaCreator()}, mock
}

// The warehouse resolves the name with OBJECT_ID under its own collation, which
// is case-sensitive by default, so the name is passed with its case kept, as
// written and bracket-quoted like the materializers write it.
func TestDB_RelationExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		table    string
		query    string
		result   int
		want     bool
		queryErr error
		wantErr  string
	}{
		{name: "schema.table exists", table: "dbo.Orders", query: existsQuery("N'dbo.Orders'", "N'[dbo].[Orders]'"), result: 1, want: true},
		{name: "one-part name", table: "Orders", query: existsQuery("N'Orders'", "N'[Orders]'"), result: 1, want: true},
		{name: "warehouse.schema.table", table: "wh.dbo.Orders", query: existsQuery("N'wh.dbo.Orders'", "N'[wh].[dbo].[Orders]'"), result: 1, want: true},
		{name: "missing table or schema", table: "staging.Orders", query: existsQuery("N'staging.Orders'", "N'[staging].[Orders]'"), result: 0, want: false},
		{name: "query errors are returned", table: "dbo.Orders", query: existsQuery("N'dbo.Orders'", "N'[dbo].[Orders]'"), queryErr: errors.New("login failed"), wantErr: "failed to check whether table 'dbo.Orders' exists: login failed"},
		{name: "more than three parts", table: "a.b.c.d", wantErr: "table name must be in format `table`, `schema.table`, or `database.schema.table`, \"a.b.c.d\" given"},
		{name: "empty component", table: "dbo.", wantErr: "table name \"dbo.\" contains an empty component"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock := newAutoCreateMockDB(t, sqlmock.QueryMatcherEqual)
			if tt.query != "" {
				expectation := mock.ExpectQuery(tt.query).WithoutArgs()
				if tt.queryErr != nil {
					expectation.WillReturnError(tt.queryErr)
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

// TestAutoCreateFirstRun runs the wrapped fabric.sql operator for every
// incremental strategy the warehouse supports: while the table is missing the
// schema is created and the table is built with create+replace, once it exists
// the strategy loads into it.
func TestAutoCreateFirstRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		strategy  pipeline.MaterializationStrategy
		secondRun string
	}{
		{strategy: pipeline.MaterializationStrategyAppend, secondRun: "INSERT INTO [analytics].[items]\nSELECT"},
		{strategy: pipeline.MaterializationStrategyMerge, secondRun: "DELETE FROM [analytics].[items] WHERE EXISTS"},
		{strategy: pipeline.MaterializationStrategyDeleteInsert, secondRun: "DELETE FROM [analytics].[items] WHERE EXISTS"},
		{strategy: pipeline.MaterializationStrategyTruncateInsert, secondRun: "TRUNCATE TABLE [analytics].[items]"},
	}

	for _, tt := range tests {
		t.Run(string(tt.strategy), func(t *testing.T) {
			t.Parallel()

			asset := &pipeline.Asset{
				Name:       "analytics.items",
				Type:       pipeline.AssetTypeFabricQuery,
				Connection: "fabric",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: tt.strategy,
				},
				Columns: []pipeline.Column{{Name: "id", PrimaryKey: true}, {Name: "label"}},
				ExecutableFile: pipeline.ExecutableFile{
					Content: "SELECT 1 AS id, 'a' AS label",
				},
			}

			db, mock := newAutoCreateMockDB(t, sqlmock.QueryMatcherFunc(containsMatcher))
			noRows := func() *sqlmock.Rows { return sqlmock.NewRows([]string{"result"}) }

			itemsExistsQuery := existsQuery("N'analytics.items'", "N'[analytics].[items]'")
			mock.ExpectQuery(itemsExistsQuery).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(0))
			mock.ExpectQuery("IF SCHEMA_ID(N'analytics') IS NULL").WillReturnRows(noRows())
			mock.ExpectQuery("CREATE TABLE [analytics].[items__bruin_tmp] AS\nSELECT 1 AS id").WillReturnRows(noRows())
			out := runAutoCreate(t, db, asset)
			assert.Contains(t, out, "Table 'analytics.items' does not exist yet, creating it from the query.")

			mock.ExpectQuery(itemsExistsQuery).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))
			mock.ExpectQuery(tt.secondRun).WillReturnRows(noRows())
			out = runAutoCreate(t, db, asset)
			assert.NotContains(t, out, "does not exist yet")

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func existsQuery(name, quotedName string) string {
	return "SELECT CASE WHEN OBJECT_ID(" + name + ") IS NULL AND OBJECT_ID(" + quotedName + ") IS NULL THEN 0 ELSE 1 END"
}

func containsMatcher(expectedSQL, actualSQL string) error {
	if !strings.Contains(actualSQL, expectedSQL) {
		return errors.New("query does not contain " + expectedSQL + ":\n" + actualSQL)
	}
	return nil
}

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

	connections := staticConnections{"fabric": db}
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
