package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/autocreate"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pashagolub/pgxmock/v3"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ autocreate.RelationChecker = (*Client)(nil)

const (
	regclassQuery        = "SELECT to_regclass($1) IS NOT NULL"
	redshiftVisibleQuery = "SELECT count(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE lower(c.relname) = lower($1) AND pg_catalog.pg_table_is_visible(c.oid)"
	redshiftSchemaQuery  = "SELECT count(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE lower(c.relname) = lower($1) AND lower(n.nspname) = lower($2)"
	redshiftExternalAny  = "SELECT count(*) FROM svv_external_tables WHERE lower(tablename) = lower($1)"
	redshiftExternalIn   = "SELECT count(*) FROM svv_external_tables WHERE lower(tablename) = lower($1) AND lower(schemaname) = lower($2)"
)

// containsMatcher matches a statement that contains the expected text.
var containsMatcher = pgxmock.QueryMatcherFunc(func(expectedSQL, actualSQL string) error {
	if !strings.Contains(actualSQL, expectedSQL) {
		return fmt.Errorf("statement does not contain %q:\n%s", expectedSQL, actualSQL)
	}
	return nil
})

//nolint:ireturn
func newMockClient(t *testing.T, config PgConfig, matcher pgxmock.QueryMatcher) (*Client, pgxmock.PgxPoolIface) {
	t.Helper()

	mock, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(matcher))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mock.ExpectationsWereMet())
		mock.Close()
	})

	return &Client{connection: mock, config: config, schemaCache: &sync.Map{}}, mock
}

func boolRow(value bool) *pgxmock.Rows {
	return pgxmock.NewRows([]string{"exists"}).AddRow(value)
}

func countRow(value int64) *pgxmock.Rows {
	return pgxmock.NewRows([]string{"count"}).AddRow(value)
}

func TestClient_RelationExists_Postgres(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		table  string
		quoted string
		found  bool
	}{
		{name: "schema.table", table: "demo.items", quoted: `"demo"."items"`, found: true},
		{name: "case is kept, like the quoted name the materializer writes", table: "Demo.Items", quoted: `"Demo"."Items"`, found: false},
		{name: "one-part name resolves through search_path", table: "items", quoted: `"items"`, found: true},
		{name: "database.schema.table", table: "bruin.demo.items", quoted: `"bruin"."demo"."items"`, found: true},
		{name: "missing relation or schema", table: "nowhere.items", quoted: `"nowhere"."items"`, found: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, mock := newMockClient(t, Config{Database: "bruin"}, pgxmock.QueryMatcherEqual)
			mock.ExpectQuery(regclassQuery).WithArgs(tt.quoted).WillReturnRows(boolRow(tt.found))

			got, err := client.RelationExists(context.Background(), tt.table)
			require.NoError(t, err)
			assert.Equal(t, tt.found, got)
		})
	}

	t.Run("query error", func(t *testing.T) {
		t.Parallel()

		client, mock := newMockClient(t, Config{Database: "bruin"}, pgxmock.QueryMatcherEqual)
		mock.ExpectQuery(regclassQuery).WithArgs(`"other"."demo"."items"`).
			WillReturnError(errors.New("cross-database references are not implemented"))

		_, err := client.RelationExists(context.Background(), "other.demo.items")
		require.ErrorContains(t, err, "cross-database references are not implemented")
	})

	t.Run("no row", func(t *testing.T) {
		t.Parallel()

		client, mock := newMockClient(t, Config{Database: "bruin"}, pgxmock.QueryMatcherEqual)
		mock.ExpectQuery(regclassQuery).WithArgs(`"items"`).WillReturnRows(pgxmock.NewRows([]string{"exists"}))

		_, err := client.RelationExists(context.Background(), "items")
		require.Error(t, err)
	})
}

func TestClient_RelationExists_InvalidNames(t *testing.T) {
	t.Parallel()

	for _, config := range []PgConfig{Config{Database: "bruin"}, RedShiftConfig{Database: "dev"}} {
		for _, table := range []string{"", "demo.", "demo..items", "a.b.c.d", `demo."items"`} {
			t.Run(fmt.Sprintf("%T %q", config, table), func(t *testing.T) {
				t.Parallel()

				// No query is expected: the name is rejected before reaching the database.
				client, _ := newMockClient(t, config, pgxmock.QueryMatcherEqual)
				_, err := client.RelationExists(context.Background(), table)
				require.Error(t, err)
			})
		}
	}
}

func TestClient_RelationExists_Redshift(t *testing.T) {
	t.Parallel()

	queryErr := errors.New("permission denied")
	tests := []struct {
		name    string
		table   string
		expect  func(mock pgxmock.PgxPoolIface)
		want    bool
		wantErr string
	}{
		{
			name:  "one-part name visible through search_path",
			table: "items",
			expect: func(mock pgxmock.PgxPoolIface) {
				mock.ExpectQuery(redshiftVisibleQuery).WithArgs("items").WillReturnRows(countRow(1))
			},
			want: true,
		},
		{
			name:  "missing one-part name checks external tables in every schema",
			table: "items",
			expect: func(mock pgxmock.PgxPoolIface) {
				mock.ExpectQuery(redshiftVisibleQuery).WithArgs("items").WillReturnRows(countRow(0))
				mock.ExpectQuery(redshiftExternalAny).WithArgs("items").WillReturnRows(countRow(0))
			},
			want: false,
		},
		{
			name:  "schema.table compares case-insensitively",
			table: "Analytics.Orders",
			expect: func(mock pgxmock.PgxPoolIface) {
				mock.ExpectQuery(redshiftSchemaQuery).WithArgs("Orders", "Analytics").WillReturnRows(countRow(1))
			},
			want: true,
		},
		{
			name:  "missing schema.table",
			table: "analytics.orders",
			expect: func(mock pgxmock.PgxPoolIface) {
				mock.ExpectQuery(redshiftSchemaQuery).WithArgs("orders", "analytics").WillReturnRows(countRow(0))
				mock.ExpectQuery(redshiftExternalIn).WithArgs("orders", "analytics").WillReturnRows(countRow(0))
			},
			want: false,
		},
		{
			name:  "external table",
			table: "spectrum.events",
			expect: func(mock pgxmock.PgxPoolIface) {
				mock.ExpectQuery(redshiftSchemaQuery).WithArgs("events", "spectrum").WillReturnRows(countRow(0))
				mock.ExpectQuery(redshiftExternalIn).WithArgs("events", "spectrum").WillReturnRows(countRow(2))
			},
			want: true,
		},
		{
			name:  "database.schema.table in the connection's database",
			table: "DEV.analytics.orders",
			expect: func(mock pgxmock.PgxPoolIface) {
				mock.ExpectQuery(redshiftSchemaQuery).WithArgs("orders", "analytics").WillReturnRows(countRow(1))
			},
			want: true,
		},
		{
			name:    "database.schema.table in another database",
			table:   "other.analytics.orders",
			expect:  func(mock pgxmock.PgxPoolIface) {},
			wantErr: "not in the connection's database 'dev'",
		},
		{
			name:  "catalog query error",
			table: "analytics.orders",
			expect: func(mock pgxmock.PgxPoolIface) {
				mock.ExpectQuery(redshiftSchemaQuery).WithArgs("orders", "analytics").WillReturnError(queryErr)
			},
			wantErr: "permission denied",
		},
		{
			name:  "external table query error",
			table: "analytics.orders",
			expect: func(mock pgxmock.PgxPoolIface) {
				mock.ExpectQuery(redshiftSchemaQuery).WithArgs("orders", "analytics").WillReturnRows(countRow(0))
				mock.ExpectQuery(redshiftExternalIn).WithArgs("orders", "analytics").WillReturnError(queryErr)
			},
			wantErr: "permission denied",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, mock := newMockClient(t, RedShiftConfig{Database: "dev"}, pgxmock.QueryMatcherEqual)
			tt.expect(mock)

			got, err := client.RelationExists(context.Background(), tt.table)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestAutoCreateFirstRun runs the wrapped operator for every incremental
// strategy twice: when the table is missing it has to create it, and once it
// exists it has to load it with the strategy.
func TestAutoCreateFirstRun(t *testing.T) {
	t.Parallel()

	dialects := []struct {
		assetType   pipeline.AssetType
		config      PgConfig
		tableExists func(mock pgxmock.PgxPoolIface, exists bool)
	}{
		{
			assetType: pipeline.AssetTypePostgresQuery,
			config:    Config{Database: "bruin"},
			tableExists: func(mock pgxmock.PgxPoolIface, exists bool) {
				mock.ExpectQuery(regclassQuery).WithArgs(`"demo"."items"`).WillReturnRows(boolRow(exists))
			},
		},
		{
			assetType: pipeline.AssetTypeRedshiftQuery,
			config:    RedShiftConfig{Database: "dev"},
			tableExists: func(mock pgxmock.PgxPoolIface, exists bool) {
				if exists {
					mock.ExpectQuery(redshiftSchemaQuery).WithArgs("items", "demo").WillReturnRows(countRow(1))
					return
				}
				mock.ExpectQuery(redshiftSchemaQuery).WithArgs("items", "demo").WillReturnRows(countRow(0))
				mock.ExpectQuery(redshiftExternalIn).WithArgs("items", "demo").WillReturnRows(countRow(0))
			},
		},
	}

	strategies := []struct {
		strategy        pipeline.MaterializationStrategy
		incrementalKey  string
		timeGranularity pipeline.MaterializationTimeGranularity
		// incremental is part of the statement the strategy runs on a later run.
		incremental map[pipeline.AssetType]string
	}{
		{strategy: pipeline.MaterializationStrategyAppend, incremental: sameForBoth(`INSERT INTO "demo"."items" SELECT`)},
		{strategy: pipeline.MaterializationStrategyMerge, incremental: sameForBoth(`MERGE INTO "demo"."items"`)},
		{strategy: pipeline.MaterializationStrategyDeleteInsert, incrementalKey: "id", incremental: sameForBoth(`DELETE FROM "demo"."items" WHERE "id" in`)},
		{strategy: pipeline.MaterializationStrategyTruncateInsert, incremental: sameForBoth(`TRUNCATE TABLE "demo"."items"`)},
		{
			strategy:        pipeline.MaterializationStrategyTimeInterval,
			incrementalKey:  "updated_at",
			timeGranularity: pipeline.MaterializationTimeGranularityTimestamp,
			incremental:     sameForBoth(`DELETE FROM "demo"."items" WHERE "updated_at" BETWEEN '2024-01-01`),
		},
		{
			strategy: pipeline.MaterializationStrategySCD2ByColumn,
			incremental: map[pipeline.AssetType]string{
				pipeline.AssetTypePostgresQuery: `MERGE INTO "demo"."items" AS target`,
				pipeline.AssetTypeRedshiftQuery: `UPDATE "demo"."items" AS target`,
			},
		},
		{
			strategy:       pipeline.MaterializationStrategySCD2ByTime,
			incrementalKey: "updated_at",
			incremental: map[pipeline.AssetType]string{
				pipeline.AssetTypePostgresQuery: `MERGE INTO "demo"."items" AS target`,
				pipeline.AssetTypeRedshiftQuery: `UPDATE "demo"."items" AS target`,
			},
		},
	}

	for _, dialect := range dialects {
		for _, tt := range strategies {
			t.Run(fmt.Sprintf("%s %s", dialect.assetType, tt.strategy), func(t *testing.T) {
				t.Parallel()

				client, mock := newMockClient(t, dialect.config, containsMatcher)
				asset := &pipeline.Asset{
					Name:       "demo.items",
					Type:       dialect.assetType,
					Connection: "conn",
					Materialization: pipeline.Materialization{
						Type:            pipeline.MaterializationTypeTable,
						Strategy:        tt.strategy,
						IncrementalKey:  tt.incrementalKey,
						TimeGranularity: tt.timeGranularity,
					},
					Columns: []pipeline.Column{
						{Name: "id", Type: "INTEGER", PrimaryKey: true},
						{Name: "label", Type: "VARCHAR"},
						{Name: "updated_at", Type: "TIMESTAMP"},
					},
					ExecutableFile: pipeline.ExecutableFile{Content: "SELECT id, label, updated_at FROM raw.items"},
				}

				// The first run creates the schema and the table from the query, and skips the SCD2 column migration.
				dialect.tableExists(mock, false)
				mock.ExpectExec(`CREATE SCHEMA IF NOT EXISTS "demo"`).WillReturnResult(pgxmock.NewResult("CREATE", 0))
				mock.ExpectExec(`CREATE TABLE "demo"."items" AS`).WillReturnResult(pgxmock.NewResult("CREATE", 0))

				out := runAutoCreate(t, client, asset)
				assert.Contains(t, out, "Table 'demo.items' does not exist yet, creating it from the query.")

				// The second run finds the table and loads it with the strategy.
				dialect.tableExists(mock, true)
				if asset.Materialization.IsSCD2() {
					mock.ExpectQuery("FROM information_schema.columns").
						WillReturnRows(pgxmock.NewRows([]string{"column_name", "data_type"}))
				}
				mock.ExpectExec(tt.incremental[dialect.assetType]).WillReturnResult(pgxmock.NewResult("INSERT", 2))

				out = runAutoCreate(t, client, asset)
				assert.NotContains(t, out, "does not exist yet")
			})
		}
	}
}

func sameForBoth(statement string) map[pipeline.AssetType]string {
	return map[pipeline.AssetType]string{
		pipeline.AssetTypePostgresQuery: statement,
		pipeline.AssetTypeRedshiftQuery: statement,
	}
}

type staticConnections map[string]any

func (c staticConnections) GetConnection(name string) any {
	return c[name]
}

func runAutoCreate(t *testing.T, client *Client, asset *pipeline.Asset) string {
	t.Helper()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	var out bytes.Buffer
	ctx := context.WithValue(context.Background(), executor.KeyPrinter, &out)
	ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, start)
	ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, end)
	ctx = context.WithValue(ctx, pipeline.RunConfigExecutionDate, start)
	ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "run")
	ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, false)

	connections := staticConnections{"conn": client}
	extractor := &query.WholeFileExtractor{
		Fs:       afero.NewMemMapFs(),
		Renderer: jinja.NewRendererWithStartEndDates(&start, &end, &start, "p", "run", nil),
	}
	op := autocreate.Wrap(NewBasicOperator(connections, extractor, NewMaterializer(false), nil), connections)

	ti := &scheduler.AssetInstance{Pipeline: &pipeline.Pipeline{Name: "p"}, Asset: asset}
	require.NoError(t, op.Run(ctx, ti))

	return out.String()
}
