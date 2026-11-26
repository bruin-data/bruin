package snowflake

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/autocreate"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ autocreate.RelationChecker = (*DB)(nil)

var showObjectsColumns = []string{"created_on", "name", "kind", "database_name", "schema_name"}

func showObjectsRows(names ...string) *sqlmock.Rows {
	rows := sqlmock.NewRows(showObjectsColumns)
	for _, name := range names {
		rows.AddRow("2024-01-01 00:00:00", name, "TABLE", "MYDB", "ANALYTICS")
	}
	return rows
}

func countRows(count string) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(count)
}

func newRelationTestDB(t *testing.T, matcher sqlmock.QueryMatcher) (*DB, sqlmock.Sqlmock) { //nolint:ireturn // sqlmock only exposes its mock as an interface.
	t.Helper()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	require.NoError(t, err)
	t.Cleanup(func() { _ = mockDB.Close() })

	return &DB{
		conn:          sqlx.NewDb(mockDB, "sqlmock"),
		config:        &Config{Database: "MYDB", Schema: "PUBLIC"},
		schemaCreator: ansisql.NewSchemaCreatorWithContainer("DATABASE"),
		retryDelay:    func(int) time.Duration { return 0 },
	}, mock
}

func TestDB_RelationExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		table   string
		expect  func(mock sqlmock.Sqlmock)
		want    bool
		wantErr string
	}{
		{
			name:  "table in the session's schema",
			table: "orders",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA`).WillReturnRows(showObjectsRows("ORDERS"))
			},
			want: true,
		},
		{
			name:  "schema.table in the session's database",
			table: "analytics.orders",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA analytics`).WillReturnRows(showObjectsRows("ORDERS"))
			},
			want: true,
		},
		{
			name:  "database.schema.table",
			table: "other_db.analytics.orders",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA other_db.analytics`).WillReturnRows(showObjectsRows("ORDERS"))
			},
			want: true,
		},
		{
			name:  "mixed-case asset names resolve in upper case",
			table: "Analytics.Orders",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'Orders' IN SCHEMA Analytics`).WillReturnRows(showObjectsRows("ORDERS"))
			},
			want: true,
		},
		{
			// CREATE OR REPLACE TABLE analytics.orders creates ANALYTICS.ORDERS and leaves the
			// quoted "orders" alone, so the lower-case object does not count.
			name:  "a quoted lower-case object is a different table",
			table: "analytics.orders",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA analytics`).WillReturnRows(showObjectsRows("orders"))
				mock.ExpectQuery(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ANALYTICS' AND TABLE_NAME = 'ORDERS'`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
			},
			want: false,
		},
		{
			name:  "view",
			table: "analytics.orders",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA analytics`).
					WillReturnRows(sqlmock.NewRows(showObjectsColumns).AddRow("2024-01-01 00:00:00", "ORDERS", "VIEW", "MYDB", "ANALYTICS"))
			},
			want: true,
		},
		{
			name:  "table that SHOW OBJECTS does not list",
			table: "analytics.orders",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA analytics`).WillReturnRows(showObjectsRows())
				mock.ExpectQuery(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ANALYTICS' AND TABLE_NAME = 'ORDERS'`).
					WillReturnRows(countRows("1"))
			},
			want: true,
		},
		{
			name:  "missing table in the session's schema",
			table: "orders",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA`).WillReturnRows(showObjectsRows())
				mock.ExpectQuery(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = CURRENT_SCHEMA() AND TABLE_NAME = 'ORDERS'`).
					WillReturnRows(countRows("0"))
			},
			want: false,
		},
		{
			name:  "missing schema.table",
			table: "analytics.orders",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA analytics`).WillReturnRows(showObjectsRows())
				mock.ExpectQuery(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ANALYTICS' AND TABLE_NAME = 'ORDERS'`).
					WillReturnRows(countRows("0"))
			},
			want: false,
		},
		{
			name:  "missing table in another database",
			table: "other_db.analytics.orders",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA other_db.analytics`).WillReturnRows(showObjectsRows())
				mock.ExpectQuery(`SELECT COUNT(*) FROM other_db.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ANALYTICS' AND TABLE_NAME = 'ORDERS'`).
					WillReturnRows(countRows("0"))
			},
			want: false,
		},
		{
			name:  "only a LIKE wildcard match",
			table: "analytics.my_table",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'my_table' IN SCHEMA analytics`).WillReturnRows(showObjectsRows("MYXTABLE"))
				mock.ExpectQuery(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ANALYTICS' AND TABLE_NAME = 'MY_TABLE'`).
					WillReturnRows(countRows("0"))
			},
			want: false,
		},
		{
			name:  "missing schema",
			table: "analytics.orders",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA analytics`).
					WillReturnError(errors.New("002003 (02000): SQL compilation error:\nSchema 'MYDB.ANALYTICS' does not exist or not authorized."))
			},
			want: false,
		},
		{
			name:  "missing database",
			table: "other_db.analytics.orders",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA other_db.analytics`).
					WillReturnError(errors.New("002003 (02000): SQL compilation error:\nDatabase 'OTHER_DB' does not exist or not authorized."))
			},
			want: false,
		},
		{
			name:  "database dropped after SHOW OBJECTS",
			table: "other_db.analytics.orders",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA other_db.analytics`).WillReturnRows(showObjectsRows())
				mock.ExpectQuery(`SELECT COUNT(*) FROM other_db.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ANALYTICS' AND TABLE_NAME = 'ORDERS'`).
					WillReturnError(errors.New("002003 (02000): SQL compilation error:\nDatabase 'OTHER_DB' does not exist or not authorized."))
			},
			want: false,
		},
		{
			name:  "SHOW OBJECTS and the lookup fail",
			table: "analytics.orders",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA analytics`).
					WillReturnError(errors.New("090105 (22000): Cannot perform SHOW OBJECTS. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."))
				mock.ExpectQuery(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ANALYTICS' AND TABLE_NAME = 'ORDERS'`).
					WillReturnError(errors.New("090105 (22000): Cannot perform SELECT. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."))
			},
			wantErr: "This session does not have a current database",
		},
		{
			name:  "missing schema reported with another error text",
			table: "analytics.orders",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA analytics`).
					WillReturnError(errors.New("002043 (02000): SQL compilation error:\nObject does not exist, or operation cannot be performed."))
				mock.ExpectQuery(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ANALYTICS' AND TABLE_NAME = 'ORDERS'`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
			},
			want: false,
		},
		{
			name:  "SHOW OBJECTS fails but the lookup finds the table",
			table: "analytics.orders",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA analytics`).
					WillReturnError(errors.New("connection reset"))
				mock.ExpectQuery(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ANALYTICS' AND TABLE_NAME = 'ORDERS'`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
			},
			want: true,
		},
		{
			name:  "INFORMATION_SCHEMA lookup fails",
			table: "analytics.orders",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA analytics`).WillReturnRows(showObjectsRows())
				mock.ExpectQuery(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ANALYTICS' AND TABLE_NAME = 'ORDERS'`).
					WillReturnError(errors.New("000606 (57P03): No active warehouse selected in the current session.  Select an active warehouse with the 'use warehouse' command."))
			},
			wantErr: "No active warehouse selected",
		},
		{
			name:    "four-part name",
			table:   "a.b.c.d",
			wantErr: "table name must be in format",
		},
		{
			name:    "empty part",
			table:   "analytics..orders",
			wantErr: "is not an unquoted Snowflake identifier",
		},
		{
			name:    "quoted identifier",
			table:   `"Analytics".orders`,
			wantErr: "is not an unquoted Snowflake identifier",
		},
		{
			name:    "identifier with a quote",
			table:   "analytics.orders' OR '1",
			wantErr: "is not an unquoted Snowflake identifier",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock := newRelationTestDB(t, sqlmock.QueryMatcherEqual)
			if tt.expect != nil {
				tt.expect(mock)
			}

			got, err := db.RelationExists(t.Context(), tt.table)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// prefixMatcher matches a statement by its beginning, which is enough to tell
// the statements of a run apart.
var prefixMatcher = sqlmock.QueryMatcherFunc(func(expected, actual string) error {
	if !strings.HasPrefix(strings.TrimSpace(actual), expected) {
		return fmt.Errorf("query %q does not start with %q", actual, expected)
	}
	return nil
})

// TestAutoCreateFirstRun runs every incremental strategy twice through the
// wrapped operator: the first run finds no table and creates it, the second
// finds it and loads into it.
func TestAutoCreateFirstRun(t *testing.T) {
	t.Parallel()

	const (
		lookupSCD2Columns = "SELECT column_name, data_type FROM information_schema.columns WHERE UPPER(table_schema) = 'ANALYTICS' AND UPPER(table_name) = 'ORDERS'"
		lookupObjectType  = "SELECT TABLE_TYPE, IS_ICEBERG FROM MYDB.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ANALYTICS' AND TABLE_NAME = 'ORDERS'"
	)

	tests := []struct {
		name            string
		materialization pipeline.Materialization
		created         string
		// secondRunLookups are the metadata queries the operator runs before an incremental load.
		secondRunLookups []string
		loaded           string
	}{
		{
			name:            "append",
			materialization: pipeline.Materialization{Strategy: pipeline.MaterializationStrategyAppend},
			created:         "CREATE OR REPLACE TABLE analytics.orders  AS\nSELECT",
			loaded:          "INSERT INTO analytics.orders SELECT",
		},
		{
			name:            "merge",
			materialization: pipeline.Materialization{Strategy: pipeline.MaterializationStrategyMerge},
			created:         "CREATE OR REPLACE TABLE analytics.orders  AS\nSELECT",
			loaded:          "MERGE INTO analytics.orders target",
		},
		{
			name:            "delete+insert",
			materialization: pipeline.Materialization{Strategy: pipeline.MaterializationStrategyDeleteInsert, IncrementalKey: "id"},
			created:         "CREATE OR REPLACE TABLE analytics.orders  AS\nSELECT",
			loaded:          "BEGIN TRANSACTION;\nCREATE TEMP TABLE __bruin_tmp_",
		},
		{
			name:            "truncate+insert",
			materialization: pipeline.Materialization{Strategy: pipeline.MaterializationStrategyTruncateInsert},
			created:         "CREATE OR REPLACE TABLE analytics.orders  AS\nSELECT",
			loaded:          "BEGIN TRANSACTION;\nTRUNCATE TABLE analytics.orders;",
		},
		{
			name: "time_interval",
			materialization: pipeline.Materialization{
				Strategy:        pipeline.MaterializationStrategyTimeInterval,
				IncrementalKey:  "updated_at",
				TimeGranularity: pipeline.MaterializationTimeGranularityTimestamp,
			},
			created: "CREATE OR REPLACE TABLE analytics.orders  AS\nSELECT",
			loaded:  "BEGIN TRANSACTION;\nDELETE FROM analytics.orders WHERE updated_at BETWEEN '2024-01-01T00:00:00",
		},
		{
			name:             "scd2_by_column",
			materialization:  pipeline.Materialization{Strategy: pipeline.MaterializationStrategySCD2ByColumn},
			created:          "ALTER SESSION SET TIMEZONE = 'UTC';\nCREATE OR REPLACE TABLE analytics.orders CLUSTER BY (_is_current, id) AS",
			secondRunLookups: []string{lookupSCD2Columns},
			loaded:           "ALTER SESSION SET TIMEZONE = 'UTC';\nBEGIN TRANSACTION;",
		},
		{
			name:             "scd2_by_time",
			materialization:  pipeline.Materialization{Strategy: pipeline.MaterializationStrategySCD2ByTime, IncrementalKey: "updated_at"},
			created:          "ALTER SESSION SET TIMEZONE = 'UTC';\nCREATE OR REPLACE TABLE analytics.orders CLUSTER BY (_is_current, id) AS",
			secondRunLookups: []string{lookupSCD2Columns},
			loaded:           "ALTER SESSION SET TIMEZONE = 'UTC';\nBEGIN TRANSACTION;",
		},
		{
			name: "Iceberg merge with cluster_by",
			materialization: pipeline.Materialization{
				Strategy:    pipeline.MaterializationStrategyMerge,
				ClusterBy:   []string{"id"},
				TableFormat: pipeline.MaterializationTableFormatIceberg,
			},
			created:          "CREATE OR REPLACE ICEBERG TABLE analytics.orders CLUSTER BY (id) CATALOG = 'SNOWFLAKE' AS",
			secondRunLookups: []string{lookupObjectType},
			loaded:           "MERGE INTO analytics.orders target",
		},
		{
			name: "Iceberg scd2_by_time",
			materialization: pipeline.Materialization{
				Strategy:       pipeline.MaterializationStrategySCD2ByTime,
				IncrementalKey: "updated_at",
				TableFormat:    pipeline.MaterializationTableFormatIceberg,
			},
			created:          "ALTER SESSION SET TIMEZONE = 'UTC';\nCREATE OR REPLACE ICEBERG TABLE analytics.orders CLUSTER BY (_is_current, id) CATALOG = 'SNOWFLAKE' AS",
			secondRunLookups: []string{lookupObjectType},
			loaded:           "ALTER SESSION SET TIMEZONE = 'UTC';\nBEGIN TRANSACTION;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock := newRelationTestDB(t, prefixMatcher)
			mat := tt.materialization
			mat.Type = pipeline.MaterializationTypeTable
			asset := &pipeline.Asset{
				Name:            "analytics.orders",
				Type:            pipeline.AssetTypeSnowflakeQuery,
				Connection:      "sf",
				Materialization: mat,
				Columns: []pipeline.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "label", Type: "VARCHAR"},
					{Name: "updated_at", Type: "TIMESTAMP"},
				},
				ExecutableFile: pipeline.ExecutableFile{
					Content: "SELECT 1 AS id, 'a' AS label, CURRENT_TIMESTAMP() AS updated_at",
				},
			}

			// First run: nothing is there, so the table is built from the query.
			mock.ExpectQuery("SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA analytics").WillReturnRows(showObjectsRows())
			mock.ExpectQuery("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ANALYTICS' AND TABLE_NAME = 'ORDERS'").
				WillReturnRows(countRows("0"))
			mock.ExpectQuery("CREATE SCHEMA IF NOT EXISTS ANALYTICS").WillReturnRows(sqlmock.NewRows(nil))
			mock.ExpectQuery(lookupObjectType).WillReturnRows(sqlmock.NewRows([]string{"TABLE_TYPE", "IS_ICEBERG"}))
			mock.ExpectQuery(tt.created).WillReturnRows(sqlmock.NewRows(nil))

			out := runAutoCreate(t, db, asset)
			assert.Contains(t, out, "Table 'analytics.orders' does not exist yet, creating it from the query.")
			require.NoError(t, mock.ExpectationsWereMet())

			// Second run: the table is listed, so the strategy loads into it.
			mock.ExpectQuery("SHOW TERSE OBJECTS LIKE 'orders' IN SCHEMA analytics").WillReturnRows(showObjectsRows("ORDERS"))
			for _, lookup := range tt.secondRunLookups {
				mock.ExpectQuery(lookup).WillReturnRows(sqlmock.NewRows([]string{"name", "type"}))
			}
			mock.ExpectQuery(tt.loaded).WillReturnRows(sqlmock.NewRows(nil))

			out = runAutoCreate(t, db, asset)
			assert.NotContains(t, out, "does not exist yet")
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
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

	connections := staticConnections{"sf": db}
	extractor := &query.WholeFileExtractor{
		Fs:       afero.NewMemMapFs(),
		Renderer: jinja.NewRendererWithStartEndDates(&start, &end, &start, "p", "run", nil),
	}
	op := autocreate.Wrap(NewBasicOperator(connections, extractor, NewMaterializer(false), nil), connections)

	ti := &scheduler.AssetInstance{Pipeline: &pipeline.Pipeline{Name: "p"}, Asset: asset}
	require.NoError(t, op.Run(ctx, ti))

	return out.String()
}

type staticConnections map[string]any

func (c staticConnections) GetConnection(name string) any {
	return c[name]
}
