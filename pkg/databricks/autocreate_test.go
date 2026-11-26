package databricks

import (
	"bytes"
	"context"
	"errors"
	"regexp"
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

var showTablesColumns = []string{"database", "tableName", "isTemporary"}

func newMockDB(t *testing.T, matcher sqlmock.QueryMatcher) (*DB, sqlmock.Sqlmock) { //nolint:ireturn // sqlmock exposes its mock only as an interface.
	t.Helper()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	require.NoError(t, err)
	t.Cleanup(func() { _ = mockDB.Close() })

	return &DB{
		conn:          sqlx.NewDb(mockDB, "sqlmock"),
		config:        &Config{},
		schemaCreator: ansisql.NewSchemaCreatorWithContainer("CATALOG"),
	}, mock
}

func TestDB_RelationExists(t *testing.T) {
	t.Parallel()

	errMissingSchema := errors.New("[SCHEMA_NOT_FOUND] The schema `sales` cannot be found")
	errMissingCatalog := errors.New("[NO_SUCH_CATALOG_EXCEPTION] Catalog 'nowhere' was not found")

	tests := []struct {
		name      string
		tableName string
		setup     func(mock sqlmock.Sqlmock)
		want      bool
		wantErr   string
	}{
		{
			name:      "schema.table in the current catalog",
			tableName: "sales.orders",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `sales` LIKE 'orders'").
					WillReturnRows(sqlmock.NewRows(showTablesColumns).AddRow("sales", "orders", false))
			},
			want: true,
		},
		{
			name:      "catalog.schema.table",
			tableName: "main.sales.orders",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `main`.`sales` LIKE 'orders'").
					WillReturnRows(sqlmock.NewRows(showTablesColumns).AddRow("sales", "orders", false))
			},
			want: true,
		},
		{
			name:      "identifiers are case-insensitive",
			tableName: "Main.Sales.Orders",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `Main`.`Sales` LIKE 'orders'").
					WillReturnRows(sqlmock.NewRows(showTablesColumns).AddRow("sales", "orders", false))
			},
			want: true,
		},
		{
			name:      "views are listed like tables",
			tableName: "sales.orders_view",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `sales` LIKE 'orders_view'").
					WillReturnRows(sqlmock.NewRows(showTablesColumns).AddRow("sales", "orders_view", false))
			},
			want: true,
		},
		{
			name:      "a temporary view with the name counts as existing",
			tableName: "sales.orders",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `sales` LIKE 'orders'").
					WillReturnRows(sqlmock.NewRows(showTablesColumns).AddRow("", "orders", true))
			},
			want: true,
		},
		{
			name:      "missing table in an existing schema",
			tableName: "sales.orders",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `sales` LIKE 'orders'").
					WillReturnRows(sqlmock.NewRows(showTablesColumns))
			},
			want: false,
		},
		{
			name:      "names that only match the LIKE pattern do not count",
			tableName: "sales.order_items",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `sales` LIKE 'order_items'").
					WillReturnRows(sqlmock.NewRows(showTablesColumns).AddRow("sales", "orderxitems", false))
			},
			want: false,
		},
		{
			name:      "missing schema in the current catalog",
			tableName: "sales.orders",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `sales` LIKE 'orders'").WillReturnError(errMissingSchema)
				mock.ExpectQuery("SHOW SCHEMAS LIKE 'sales'").
					WillReturnRows(sqlmock.NewRows([]string{"databaseName"}))
			},
			want: false,
		},
		{
			name:      "missing schema in a named catalog",
			tableName: "main.sales.orders",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `main`.`sales` LIKE 'orders'").WillReturnError(errMissingSchema)
				mock.ExpectQuery("SHOW SCHEMAS IN `main` LIKE 'sales'").
					WillReturnRows(sqlmock.NewRows([]string{"databaseName"}).AddRow("sales_archive"))
			},
			want: false,
		},
		{
			name:      "missing catalog",
			tableName: "nowhere.sales.orders",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `nowhere`.`sales` LIKE 'orders'").WillReturnError(errMissingCatalog)
				mock.ExpectQuery("SHOW SCHEMAS IN `nowhere` LIKE 'sales'").WillReturnError(errMissingCatalog)
				mock.ExpectQuery("SHOW CATALOGS LIKE 'nowhere'").
					WillReturnRows(sqlmock.NewRows([]string{"catalog"}).AddRow("main"))
			},
			want: false,
		},
		{
			name:      "path-based table",
			tableName: "delta.`/mnt/orders`",
			wantErr:   "path-based table `/mnt/orders` cannot be checked",
		},
		{
			name:      "spark_catalog never counts as a missing catalog",
			tableName: "spark_catalog.sales.orders",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `spark_catalog`.`sales` LIKE 'orders'").WillReturnError(errors.New("connection reset"))
				mock.ExpectQuery("SHOW SCHEMAS IN `spark_catalog` LIKE 'sales'").WillReturnError(errors.New("connection reset"))
			},
			wantErr: "failed to list the tables in `spark_catalog`.`sales`: connection reset",
		},
		{
			name:      "listing the tables fails in an existing schema",
			tableName: "sales.orders",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `sales` LIKE 'orders'").WillReturnError(errors.New("PERMISSION_DENIED"))
				mock.ExpectQuery("SHOW SCHEMAS LIKE 'sales'").
					WillReturnRows(sqlmock.NewRows([]string{"databaseName"}).AddRow("SALES"))
			},
			wantErr: "failed to list the tables in `sales`: PERMISSION_DENIED",
		},
		{
			name:      "listing the tables and the schemas fails",
			tableName: "sales.orders",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `sales` LIKE 'orders'").WillReturnError(errors.New("connection reset"))
				mock.ExpectQuery("SHOW SCHEMAS LIKE 'sales'").WillReturnError(errors.New("connection reset"))
			},
			wantErr: "failed to list the tables in `sales`: connection reset",
		},
		{
			name:      "listing the schemas fails in an existing catalog",
			tableName: "main.sales.orders",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `main`.`sales` LIKE 'orders'").WillReturnError(errors.New("timeout"))
				mock.ExpectQuery("SHOW SCHEMAS IN `main` LIKE 'sales'").WillReturnError(errors.New("timeout"))
				mock.ExpectQuery("SHOW CATALOGS LIKE 'main'").
					WillReturnRows(sqlmock.NewRows([]string{"catalog"}).AddRow("main"))
			},
			wantErr: "failed to list the tables in `main`.`sales`: timeout",
		},
		{
			name:      "every listing fails",
			tableName: "main.sales.orders",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `main`.`sales` LIKE 'orders'").WillReturnError(errors.New("timeout"))
				mock.ExpectQuery("SHOW SCHEMAS IN `main` LIKE 'sales'").WillReturnError(errors.New("timeout"))
				mock.ExpectQuery("SHOW CATALOGS LIKE 'main'").WillReturnError(errors.New("timeout"))
			},
			wantErr: "failed to list the tables in `main`.`sales`: timeout",
		},
		{
			name:      "backtick-quoted identifiers",
			tableName: "`my-catalog`.sales.`order-items`",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `my-catalog`.`sales` LIKE 'order-items'").
					WillReturnRows(sqlmock.NewRows(showTablesColumns).AddRow("sales", "order-items", false))
			},
			want: true,
		},
		{
			name:      "names that are unsafe in a pattern are listed without LIKE",
			tableName: "sales.`order items`",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `sales`").
					WillReturnRows(sqlmock.NewRows(showTablesColumns).
						AddRow("sales", "customers", false).
						AddRow("sales", "Order Items", false))
			},
			want: true,
		},
		{
			name:      "names that are unsafe in a pattern and missing",
			tableName: "sales.`order items`",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `sales`").
					WillReturnRows(sqlmock.NewRows(showTablesColumns).AddRow("sales", "order_items", false))
			},
			want: false,
		},
		{
			name:      "output without a tableName column compares every column",
			tableName: "sales.orders",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `sales` LIKE 'orders'").
					WillReturnRows(sqlmock.NewRows([]string{"namespace", "name"}).AddRow("sales", "orders"))
			},
			want: true,
		},
		{
			name:      "output that holds no names is an error",
			tableName: "sales.orders",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW TABLES IN `sales` LIKE 'orders'").
					WillReturnRows(sqlmock.NewRows(showTablesColumns).AddRow("sales", nil, false))
				mock.ExpectQuery("SHOW SCHEMAS LIKE 'sales'").
					WillReturnRows(sqlmock.NewRows([]string{"databaseName"}).AddRow("sales"))
			},
			wantErr: "unexpected output from SHOW TABLES IN `sales` LIKE 'orders'",
		},
		{
			name:      "a one-part name is an error",
			tableName: "orders",
			wantErr:   "table name must be in format `schema.table` or `catalog.schema.table`",
		},
		{
			name:      "a four-part name is an error",
			tableName: "a.b.c.d",
			wantErr:   "table name must be in format `schema.table` or `catalog.schema.table`",
		},
		{
			name:      "an empty component is an error",
			tableName: "sales..orders",
			wantErr:   "contains an empty component",
		},
		{
			name:      "an unquoted identifier with special characters is an error",
			tableName: "sales.order-items",
			wantErr:   "has to be quoted with backticks",
		},
		{
			name:      "an unterminated quoted identifier is an error",
			tableName: "sales.`orders",
			wantErr:   "has to be quoted with backticks",
		},
		{
			name:      "a quoted identifier with a lone backtick is an error",
			tableName: "sales.`or`ders`",
			wantErr:   "invalid quoted identifier",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock := newMockDB(t, sqlmock.QueryMatcherEqual)
			if tt.setup != nil {
				tt.setup(mock)
			}

			got, err := db.RelationExists(t.Context(), tt.tableName)
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

func TestParseRelationName(t *testing.T) {
	t.Parallel()

	got, err := parseRelationName("`a``b`.sales.orders")
	require.NoError(t, err)
	assert.Equal(t, relationName{catalog: "a`b", schema: "sales", table: "orders"}, got)
	assert.Equal(t, "`a``b`.`sales`", got.schemaRef())
}

// TestAutoCreateFirstRun runs every incremental strategy twice through the
// wrapped operator: the first run finds no table and has to create it from the
// query, the second finds the table and has to load into it.
func TestAutoCreateFirstRun(t *testing.T) {
	t.Parallel()

	createReplace := []string{
		`CREATE TABLE sales\.__bruin_tmp_\w+ AS SELECT id, label, updated_at FROM raw\.orders;`,
		`DROP TABLE IF EXISTS sales\.orders;`,
		`ALTER TABLE sales\.__bruin_tmp_\w+ RENAME TO sales\.orders;`,
	}
	scd2FullRefresh := []string{`CREATE OR REPLACE TABLE sales\.orders AS\s+SELECT`}

	tests := []struct {
		strategy        pipeline.MaterializationStrategy
		incrementalKey  string
		timeGranularity pipeline.MaterializationTimeGranularity
		firstRun        []string
		secondRun       []string
	}{
		{
			strategy:  pipeline.MaterializationStrategyAppend,
			firstRun:  createReplace,
			secondRun: []string{`INSERT INTO sales\.orders SELECT`},
		},
		{
			strategy:  pipeline.MaterializationStrategyMerge,
			firstRun:  createReplace,
			secondRun: []string{`MERGE INTO sales\.orders target`},
		},
		{
			strategy:       pipeline.MaterializationStrategyDeleteInsert,
			incrementalKey: "id",
			firstRun:       createReplace,
			secondRun: []string{
				`CREATE TEMPORARY VIEW __bruin_tmp_\w+ AS SELECT`,
				`DELETE FROM sales\.orders WHERE id in`,
				`INSERT INTO sales\.orders SELECT \* FROM __bruin_tmp_\w+`,
				`DROP VIEW IF EXISTS __bruin_tmp_\w+`,
			},
		},
		{
			strategy:  pipeline.MaterializationStrategyTruncateInsert,
			firstRun:  createReplace,
			secondRun: []string{`TRUNCATE TABLE sales\.orders`, `INSERT INTO sales\.orders SELECT`},
		},
		{
			strategy:        pipeline.MaterializationStrategyTimeInterval,
			incrementalKey:  "updated_at",
			timeGranularity: pipeline.MaterializationTimeGranularityTimestamp,
			firstRun:        createReplace,
			secondRun: []string{
				`DELETE FROM sales\.orders WHERE updated_at BETWEEN '2024-01-01T00:00:00\.000000Z' AND '2024-01-01T23:59:59\.999999Z'`,
				`INSERT INTO sales\.orders SELECT`,
			},
		},
		{
			strategy:  pipeline.MaterializationStrategySCD2ByColumn,
			firstRun:  scd2FullRefresh,
			secondRun: []string{`MERGE INTO sales\.orders AS target`},
		},
		{
			strategy:       pipeline.MaterializationStrategySCD2ByTime,
			incrementalKey: "updated_at",
			firstRun:       scd2FullRefresh,
			secondRun:      []string{`MERGE INTO sales\.orders AS target`},
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.strategy), func(t *testing.T) {
			t.Parallel()

			db, mock := newMockDB(t, sqlmock.QueryMatcherRegexp)
			asset := &pipeline.Asset{
				Name:       "sales.orders",
				Type:       pipeline.AssetTypeDatabricksQuery,
				Connection: "dbx",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        tt.strategy,
					IncrementalKey:  tt.incrementalKey,
					TimeGranularity: tt.timeGranularity,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT", PrimaryKey: true},
					{Name: "label", Type: "STRING", UpdateOnMerge: true},
					{Name: "updated_at", Type: "TIMESTAMP"},
				},
				ExecutableFile: pipeline.ExecutableFile{
					Content: "SELECT id, label, updated_at FROM raw.orders",
				},
			}

			mock.ExpectQuery(regexp.QuoteMeta("SHOW TABLES IN `sales` LIKE 'orders'")).
				WillReturnRows(sqlmock.NewRows(showTablesColumns))
			mock.ExpectQuery(`CREATE SCHEMA IF NOT EXISTS SALES`).WillReturnRows(sqlmock.NewRows(nil))
			expectStatements(mock, tt.firstRun)

			out := runAutoCreate(t, db, asset)
			assert.Contains(t, out, "Table 'sales.orders' does not exist yet, creating it from the query.")
			require.NoError(t, mock.ExpectationsWereMet())

			mock.ExpectQuery(regexp.QuoteMeta("SHOW TABLES IN `sales` LIKE 'orders'")).
				WillReturnRows(sqlmock.NewRows(showTablesColumns).AddRow("sales", "orders", false))
			expectStatements(mock, tt.secondRun)

			out = runAutoCreate(t, db, asset)
			assert.NotContains(t, out, "does not exist yet")
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestAutoCreateFirstRunRendersFullRefreshBranch(t *testing.T) {
	t.Parallel()

	db, mock := newMockDB(t, sqlmock.QueryMatcherRegexp)
	asset := &pipeline.Asset{
		Name:       "sales.events",
		Type:       pipeline.AssetTypeDatabricksQuery,
		Connection: "dbx",
		Materialization: pipeline.Materialization{
			Type:     pipeline.MaterializationTypeTable,
			Strategy: pipeline.MaterializationStrategyAppend,
		},
		// The incremental branch reads the table, which fails while it does not exist.
		ExecutableFile: pipeline.ExecutableFile{
			Content: "{% if full_refresh %}SELECT 1 AS id{% else %}SELECT max(id) + 1 AS id FROM sales.events{% endif %}",
		},
	}

	mock.ExpectQuery(regexp.QuoteMeta("SHOW TABLES IN `sales` LIKE 'events'")).
		WillReturnError(errors.New("[SCHEMA_NOT_FOUND] The schema `sales` cannot be found"))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW SCHEMAS LIKE 'sales'")).
		WillReturnRows(sqlmock.NewRows([]string{"databaseName"}))
	mock.ExpectQuery(`CREATE SCHEMA IF NOT EXISTS SALES`).WillReturnRows(sqlmock.NewRows(nil))
	expectStatements(mock, []string{
		`CREATE TABLE sales\.__bruin_tmp_\w+ AS SELECT 1 AS id;`,
		`DROP TABLE IF EXISTS sales\.events;`,
		`ALTER TABLE sales\.__bruin_tmp_\w+ RENAME TO sales\.events;`,
	})
	runAutoCreate(t, db, asset)
	require.NoError(t, mock.ExpectationsWereMet())

	mock.ExpectQuery(regexp.QuoteMeta("SHOW TABLES IN `sales` LIKE 'events'")).
		WillReturnRows(sqlmock.NewRows(showTablesColumns).AddRow("sales", "events", false))
	expectStatements(mock, []string{regexp.QuoteMeta("INSERT INTO sales.events SELECT max(id) + 1 AS id FROM sales.events")})
	runAutoCreate(t, db, asset)
	require.NoError(t, mock.ExpectationsWereMet())
}

func expectStatements(mock sqlmock.Sqlmock, statements []string) {
	for _, statement := range statements {
		mock.ExpectQuery(statement).WillReturnRows(sqlmock.NewRows(nil))
	}
}

func runAutoCreate(t *testing.T, db *DB, asset *pipeline.Asset) string {
	t.Helper()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 23, 59, 59, 999999999, time.UTC)

	var out bytes.Buffer
	ctx := context.WithValue(t.Context(), executor.KeyPrinter, &out)
	ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, start)
	ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, end)
	ctx = context.WithValue(ctx, pipeline.RunConfigExecutionDate, start)
	ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "run")
	ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, false)

	connections := staticConnections{"dbx": db}
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
