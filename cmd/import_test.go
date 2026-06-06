package cmd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/mssql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/postgres"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Mock connection for testing.
type mockConnection struct {
	mock.Mock
}

func (m *mockConnection) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	args := m.Called(ctx)
	return args.Get(0).(*ansisql.DBDatabase), args.Error(1)
}

func (m *mockConnection) SelectWithSchema(ctx context.Context, q *query.Query) (*query.QueryResult, error) {
	args := m.Called(ctx, q)
	return args.Get(0).(*query.QueryResult), args.Error(1)
}

func (m *mockConnection) GetDatabases(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

// Mock connection without SelectWithSchema method for testing.
type mockConnectionNoSchema struct {
	mock.Mock
}

func (m *mockConnectionNoSchema) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	args := m.Called(ctx)
	return args.Get(0).(*ansisql.DBDatabase), args.Error(1)
}

func (m *mockConnectionNoSchema) GetDatabases(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

type mockColumnMetadataConnection struct {
	mock.Mock
}

func (m *mockColumnMetadataConnection) GetColumnsForTable(ctx context.Context, schemaName, tableName string) ([]*ansisql.DBColumn, error) {
	args := m.Called(ctx, schemaName, tableName)
	if v := args.Get(0); v != nil {
		return v.([]*ansisql.DBColumn), args.Error(1)
	}
	return nil, args.Error(1)
}

// nolint
func TestCreateAsset(t *testing.T) {
	t.Parallel()

	testAssetsPath := filepath.Join("test", "assets")

	tests := []struct {
		name              string
		schemaName        string
		tableName         string
		assetType         pipeline.AssetType
		fillColumns       bool
		table             *ansisql.DBTable
		want              *pipeline.Asset
		descriptionPrefix string // Expected prefix of the description (since it now includes dynamic timestamp)
	}{
		{
			name:        "successful asset creation without columns (fillColumns false)",
			schemaName:  "public",
			tableName:   "users",
			assetType:   pipeline.AssetTypePostgresSource,
			fillColumns: false,
			table: &ansisql.DBTable{
				Name:    "users",
				Type:    ansisql.DBTableTypeTable,
				Columns: nil,
			},
			want: &pipeline.Asset{
				Name: "public.users",
				Type: pipeline.AssetTypePostgresSource,
				ExecutableFile: pipeline.ExecutableFile{
					Name: "users.asset.yml",
					Path: filepath.Join(testAssetsPath, "public", "users.asset.yml"),
				},
				Columns: nil,
			},
			descriptionPrefix: "Imported table: public.users",
		},
		{
			name:        "successful asset creation with pre-fetched columns",
			schemaName:  "public",
			tableName:   "products",
			assetType:   pipeline.AssetTypePostgresSource,
			fillColumns: true,
			table: &ansisql.DBTable{
				Name: "products",
				Type: ansisql.DBTableTypeTable,
				Columns: []*ansisql.DBColumn{
					{Name: "id", Type: "INTEGER"},
					{Name: "name", Type: "VARCHAR"},
					{Name: "price", Type: "DECIMAL"},
				},
			},
			want: &pipeline.Asset{
				Name: "public.products",
				Type: pipeline.AssetTypePostgresSource,
				ExecutableFile: pipeline.ExecutableFile{
					Name: "products.asset.yml",
					Path: filepath.Join(testAssetsPath, "public", "products.asset.yml"),
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
					{Name: "name", Type: "VARCHAR", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
					{Name: "price", Type: "DECIMAL", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				},
			},
			descriptionPrefix: "Imported table: public.products",
		},
		{
			name:        "fillColumns true but no pre-fetched columns and no conn returns empty",
			schemaName:  "public",
			tableName:   "orders",
			assetType:   pipeline.AssetTypePostgresSource,
			fillColumns: true,
			table: &ansisql.DBTable{
				Name:    "orders",
				Type:    ansisql.DBTableTypeTable,
				Columns: []*ansisql.DBColumn{},
			},
			want: &pipeline.Asset{
				Name: "public.orders",
				Type: pipeline.AssetTypePostgresSource,
				ExecutableFile: pipeline.ExecutableFile{
					Name: "orders.asset.yml",
					Path: filepath.Join(testAssetsPath, "public", "orders.asset.yml"),
				},
				Columns: nil,
			},
			descriptionPrefix: "Imported table: public.orders",
		},
		{
			name:        "view asset creation with view definition",
			schemaName:  "public",
			tableName:   "active_users",
			assetType:   pipeline.AssetTypePostgresSource,
			fillColumns: false,
			table: &ansisql.DBTable{
				Name:           "active_users",
				Type:           ansisql.DBTableTypeView,
				ViewDefinition: "SELECT * FROM users WHERE active = true",
				Columns:        nil,
			},
			want: &pipeline.Asset{
				Name: "public.active_users",
				Type: pipeline.AssetTypePostgresQuery,
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "active_users.sql",
					Path:    filepath.Join(testAssetsPath, "public", "active_users.sql"),
					Content: "SELECT * FROM users WHERE active = true",
				},
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeView,
				},
				Columns: nil,
			},
			descriptionPrefix: "Imported view: public.active_users",
		},
		{
			name:        "view without view definition is not treated as view",
			schemaName:  "public",
			tableName:   "some_view",
			assetType:   pipeline.AssetTypePostgresSource,
			fillColumns: false,
			table: &ansisql.DBTable{
				Name:           "some_view",
				Type:           ansisql.DBTableTypeView,
				ViewDefinition: "", // Empty view definition
				Columns:        nil,
			},
			want: &pipeline.Asset{
				Name: "public.some_view",
				Type: pipeline.AssetTypePostgresSource,
				ExecutableFile: pipeline.ExecutableFile{
					Name: "some_view.asset.yml",
					Path: filepath.Join(testAssetsPath, "public", "some_view.asset.yml"),
				},
				Columns: nil,
			},
			descriptionPrefix: "Imported view: public.some_view",
		},
		{
			name:        "bigquery view asset creation",
			schemaName:  "analytics",
			tableName:   "daily_metrics",
			assetType:   pipeline.AssetTypeBigquerySource,
			fillColumns: false,
			table: &ansisql.DBTable{
				Name:           "daily_metrics",
				Type:           ansisql.DBTableTypeView,
				ViewDefinition: "SELECT date, COUNT(*) as cnt FROM events GROUP BY date",
				Columns:        nil,
			},
			want: &pipeline.Asset{
				Name: "analytics.daily_metrics",
				Type: pipeline.AssetTypeBigqueryQuery,
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "daily_metrics.sql",
					Path:    filepath.Join(testAssetsPath, "analytics", "daily_metrics.sql"),
					Content: "SELECT date, COUNT(*) as cnt FROM events GROUP BY date",
				},
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeView,
				},
				Columns: nil,
			},
			descriptionPrefix: "Imported view: analytics.daily_metrics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()

			// Pass nil for conn since we're testing the pre-fetched columns path
			// When dbColumns is empty and conn is nil, it will try fillAssetColumnsFromDB
			// which will fail, but for these tests we're just checking the pre-fetched path
			got, warning := createAsset(ctx, testAssetsPath, tt.schemaName, tt.tableName, tt.assetType, nil, tt.fillColumns, tt.table)

			// When dbColumns is empty and conn is nil, we get a warning about failing to fill columns
			if tt.fillColumns && len(tt.table.Columns) == 0 {
				assert.Contains(t, warning, "Could not fill columns")
			} else {
				assert.Equal(t, "", warning)
			}

			// Check the description contains the expected prefix and "Extracted at:" timestamp
			assert.True(t, strings.Contains(got.Description, tt.descriptionPrefix),
				"Expected description to contain %q, got %q", tt.descriptionPrefix, got.Description)
			assert.True(t, strings.Contains(got.Description, "Extracted at:"),
				"Expected description to contain 'Extracted at:', got %q", got.Description)

			// Compare all other fields except Description
			tt.want.Description = got.Description // Copy description to make comparison work
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDetermineAssetTypeFromConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		connectionName string
		conn           interface{}
		want           pipeline.AssetType
	}{
		// Test connection type detection
		{
			name:           "mssql connection type overrides name",
			connectionName: "prod",
			conn:           &mssql.DB{},
			want:           pipeline.AssetTypeMsSQLSource,
		},
		{
			name:           "postgres connection type overrides name",
			connectionName: "prod",
			conn:           &postgres.Client{},
			want:           pipeline.AssetTypePostgresSource,
		},
		{
			name:           "connection type not detected (embedded mock)",
			connectionName: "test-conn",
			conn:           &struct{ mockConnection }{},
			want:           pipeline.AssetTypeEmpty,
		},
		{
			name:           "duckdb connection type (mock - undefined connection)",
			connectionName: "test-conn",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypeEmpty, // Default fallback
		},

		// Test connection name detection
		{
			name:           "snowflake by name",
			connectionName: "snowflake-prod",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypeSnowflakeSource,
		},
		{
			name:           "sf by name",
			connectionName: "sf-dev",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypeSnowflakeSource,
		},
		{
			name:           "bigquery by name",
			connectionName: "bigquery-analytics",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypeBigquerySource,
		},
		{
			name:           "bq by name",
			connectionName: "bq-data",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypeBigquerySource,
		},
		{
			name:           "postgres by name",
			connectionName: "postgres-main",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypePostgresSource,
		},
		{
			name:           "pg by name",
			connectionName: "pg-replica",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypePostgresSource,
		},
		{
			name:           "redshift by name",
			connectionName: "redshift-warehouse",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypeRedshiftSource,
		},
		{
			name:           "rs by name",
			connectionName: "rs-cluster",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypeRedshiftSource,
		},
		{
			name:           "athena by name",
			connectionName: "athena-queries",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypeAthenaSource,
		},
		{
			name:           "databricks by name",
			connectionName: "databricks-lakehouse",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypeDatabricksSource,
		},
		{
			name:           "duckdb by name",
			connectionName: "duckdb-local",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypeDuckDBSource,
		},
		{
			name:           "clickhouse by name",
			connectionName: "clickhouse-events",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypeClickHouseSource,
		},
		{
			name:           "synapse by name",
			connectionName: "synapse-analytics",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypeSynapseSource,
		},
		{
			name:           "mssql by name",
			connectionName: "mssql-server",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypeMsSQLSource,
		},
		{
			name:           "sqlserver by name",
			connectionName: "sqlserver-prod",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypeMsSQLSource,
		},
		{
			name:           "unknown connection defaults to empty",
			connectionName: "unknown-db",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypeEmpty,
		},
	}

	for _, tt := range tests { //nolint:paralleltest
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := determineAssetTypeFromConnection(tt.connectionName, tt.conn)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFillAssetColumnsFromDB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupConn  func() interface{}
		setupAsset func() *pipeline.Asset
		schemaName string
		tableName  string
		wantErr    string
		wantCols   []pipeline.Column
	}{
		{
			name: "successful column filling",
			setupConn: func() interface{} {
				conn := &mockConnection{}
				conn.On("SelectWithSchema", mock.Anything, mock.MatchedBy(func(q *query.Query) bool {
					return strings.Contains(q.Query, "SELECT * FROM test_schema.test_table WHERE 1=0 LIMIT 0")
				})).Return(&query.QueryResult{
					Columns:     []string{"id", "name", "email"},
					ColumnTypes: []string{"INTEGER", "VARCHAR", "VARCHAR"},
				}, nil)
				return conn
			},
			setupAsset: func() *pipeline.Asset {
				return &pipeline.Asset{
					Name: "test_asset",
				}
			},
			schemaName: "test_schema",
			tableName:  "test_table",
			wantCols: []pipeline.Column{
				{Name: "id", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "name", Type: "VARCHAR", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "email", Type: "VARCHAR", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
			},
		},
		{
			name: "uses metadata column fetcher when available",
			setupConn: func() interface{} {
				conn := &mockColumnMetadataConnection{}
				conn.On("GetColumnsForTable", mock.Anything, "test_schema", "test_table").Return([]*ansisql.DBColumn{
					{Name: "id", Type: "integer"},
					{Name: "_IS_CURRENT", Type: "boolean"},
					{Name: "name", Type: "text", Description: "Customer name"},
				}, nil)
				return conn
			},
			setupAsset: func() *pipeline.Asset {
				return &pipeline.Asset{Name: "test_asset"}
			},
			schemaName: "test_schema",
			tableName:  "test_table",
			wantCols: []pipeline.Column{
				{Name: "id", Type: "integer", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "name", Type: "text", Description: "Customer name", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
			},
		},
		{
			name: "metadata column fetcher error with nil columns",
			setupConn: func() interface{} {
				conn := &mockColumnMetadataConnection{}
				conn.On("GetColumnsForTable", mock.Anything, "test_schema", "test_table").Return(nil, errors.New("metadata failed"))
				return conn
			},
			setupAsset: func() *pipeline.Asset {
				return &pipeline.Asset{Name: "test_asset"}
			},
			schemaName: "test_schema",
			tableName:  "test_table",
			wantErr:    "failed to query columns for table test_schema.test_table",
		},
		{
			name: "connection doesn't support schema introspection",
			setupConn: func() interface{} {
				// Return a connection that doesn't implement SelectWithSchema
				return &mockConnectionNoSchema{}
			},
			setupAsset: func() *pipeline.Asset {
				return &pipeline.Asset{Name: "test_asset"}
			},
			schemaName: "test_schema",
			tableName:  "test_table",
			wantErr:    "connection does not support schema introspection",
		},
		{
			name: "query fails",
			setupConn: func() interface{} {
				conn := &mockConnection{}
				conn.On("SelectWithSchema", mock.Anything, mock.Anything).Return((*query.QueryResult)(nil), errors.New("query failed"))
				return conn
			},
			setupAsset: func() *pipeline.Asset {
				return &pipeline.Asset{Name: "test_asset"}
			},
			schemaName: "test_schema",
			tableName:  "test_table",
			wantErr:    "failed to query columns for table test_schema.test_table",
		},
		{
			name: "no columns found",
			setupConn: func() interface{} {
				conn := &mockConnection{}
				conn.On("SelectWithSchema", mock.Anything, mock.Anything).Return(&query.QueryResult{
					Columns:     []string{},
					ColumnTypes: []string{},
				}, nil)
				return conn
			},
			setupAsset: func() *pipeline.Asset {
				return &pipeline.Asset{Name: "test_asset"}
			},
			schemaName: "test_schema",
			tableName:  "test_table",
			wantErr:    "no columns found for table test_schema.test_table",
		},
		{
			name: "filters out special columns",
			setupConn: func() interface{} {
				conn := &mockConnection{}
				conn.On("SelectWithSchema", mock.Anything, mock.Anything).Return(&query.QueryResult{
					Columns:     []string{"id", "_IS_CURRENT", "name", "_VALID_UNTIL", "_VALID_FROM", "status"},
					ColumnTypes: []string{"INTEGER", "BOOLEAN", "VARCHAR", "TIMESTAMP", "TIMESTAMP", "VARCHAR"},
				}, nil)
				return conn
			},
			setupAsset: func() *pipeline.Asset {
				return &pipeline.Asset{Name: "test_asset"}
			},
			schemaName: "test_schema",
			tableName:  "test_table",
			wantCols: []pipeline.Column{
				{Name: "id", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "name", Type: "VARCHAR", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "status", Type: "VARCHAR", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
			},
		},
	}

	for _, tt := range tests { //nolint:paralleltest
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			conn := tt.setupConn()
			asset := tt.setupAsset()

			err := fillAssetColumnsFromDB(ctx, asset, conn, tt.schemaName, tt.tableName)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantCols, asset.Columns)

			// Only assert expectations for mockConnection, not mockConnectionNoSchema
			if mockConn, ok := conn.(*mockConnection); ok {
				mockConn.AssertExpectations(t)
			}
			if mockConn, ok := conn.(*mockColumnMetadataConnection); ok {
				mockConn.AssertExpectations(t)
			}
		})
	}
}

func TestColumnDescriptionQuery(t *testing.T) {
	t.Parallel()

	schemaName := "hw'; DROP TABLE important; --"
	tableName := "staging_honor_event'; DROP TABLE events; --"

	tests := []struct {
		name             string
		conn             interface{}
		wantSchemaMarker string
		wantTableMarker  string
	}{
		{
			name:             "postgres uses bind args",
			conn:             &postgres.Client{},
			wantSchemaMarker: "n.nspname = $1",
			wantTableMarker:  "c.relname = $2",
		},
		{
			name:             "mssql uses bind args",
			conn:             &mssql.DB{},
			wantSchemaMarker: "s.name = @p1",
			wantTableMarker:  "t.name = @p2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := columnDescriptionQuery(tt.conn, schemaName, tableName)
			require.True(t, ok)
			assert.Contains(t, got.Query, tt.wantSchemaMarker)
			assert.Contains(t, got.Query, tt.wantTableMarker)
			assert.NotContains(t, got.Query, schemaName)
			assert.NotContains(t, got.Query, tableName)
			assert.Equal(t, []any{schemaName, tableName}, got.Args)
		})
	}
}

func TestBuildShowSnowflakeTasksQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		database    string
		schema      string
		taskPattern string
		want        string
		wantErr     string
	}{
		{
			name:     "database scope",
			database: "raw_db",
			want:     `SHOW TASKS IN DATABASE "RAW_DB"`,
		},
		{
			name:        "schema scope with task pattern",
			database:    "raw_db",
			schema:      "analytics",
			taskPattern: "load_%'daily",
			want:        `SHOW TASKS LIKE 'load_%''daily' IN SCHEMA "RAW_DB"."ANALYTICS"`,
		},
		{
			name:     "preserves quoted database",
			database: `"MixedDb"`,
			want:     `SHOW TASKS IN DATABASE "MixedDb"`,
		},
		{
			name:    "schema requires database",
			schema:  "analytics",
			wantErr: "database is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := buildShowSnowflakeTasksQuery(tt.database, tt.schema, tt.taskPattern)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSnowflakeTasksFromQueryResult(t *testing.T) {
	t.Parallel()

	got := snowflakeTasksFromQueryResult(&query.QueryResult{
		Columns: []string{
			"database_name",
			"schema_name",
			"name",
			"owner",
			"comment",
			"warehouse",
			"schedule",
			"predecessors",
			"state",
			"definition",
			"condition",
			"allow_overlapping_execution",
		},
		Rows: [][]interface{}{
			{
				"RAW_DB",
				"ANALYTICS",
				"LOAD_DAILY",
				"TRANSFORMER",
				"daily load",
				"COMPUTE_WH",
				"USING CRON 0 5 * * * UTC",
				`["RAW_DB.ANALYTICS.ROOT_TASK"]`,
				"started",
				"INSERT INTO mart.daily SELECT * FROM raw.events",
				"SYSTEM$STREAM_HAS_DATA('RAW.EVENTS_STREAM')",
				"true",
			},
			{
				"RAW_DB",
				"ANALYTICS",
				"",
				"TRANSFORMER",
				"",
				"",
				"",
				"",
				"",
				"SELECT 1",
				"",
				nil,
			},
		},
	})

	require.Len(t, got, 1)
	assert.Equal(t, SnowflakeTask{
		Database:     "RAW_DB",
		Schema:       "ANALYTICS",
		Name:         "LOAD_DAILY",
		Owner:        "TRANSFORMER",
		Comment:      "daily load",
		Warehouse:    "COMPUTE_WH",
		Schedule:     "USING CRON 0 5 * * * UTC",
		Predecessors: `["RAW_DB.ANALYTICS.ROOT_TASK"]`,
		State:        "started",
		Definition:   "INSERT INTO mart.daily SELECT * FROM raw.events",
		Condition:    "SYSTEM$STREAM_HAS_DATA('RAW.EVENTS_STREAM')",
	}, SnowflakeTask{
		Database:     got[0].Database,
		Schema:       got[0].Schema,
		Name:         got[0].Name,
		Owner:        got[0].Owner,
		Comment:      got[0].Comment,
		Warehouse:    got[0].Warehouse,
		Schedule:     got[0].Schedule,
		Predecessors: got[0].Predecessors,
		State:        got[0].State,
		Definition:   got[0].Definition,
		Condition:    got[0].Condition,
	})
	require.NotNil(t, got[0].AllowOverlappingExecution)
	assert.True(t, *got[0].AllowOverlappingExecution)
}

func TestCreateAssetFromSnowflakeTask(t *testing.T) {
	t.Parallel()

	assetsPath := filepath.Join("pipeline", "assets")
	allowOverlap := false
	task := SnowflakeTask{
		Database:                  "RAW_DB",
		Schema:                    "Analytics",
		Name:                      "Daily Refresh",
		Owner:                     "TRANSFORMER",
		Comment:                   "Refreshes daily analytics.",
		Warehouse:                 "COMPUTE_WH",
		Schedule:                  "USING CRON 0 5 * * * UTC",
		Predecessors:              `["RAW_DB.ANALYTICS.ROOT_TASK"]`,
		State:                     "started",
		Definition:                "\nINSERT INTO mart.daily SELECT * FROM raw.events\n",
		Condition:                 "SYSTEM$STREAM_HAS_DATA('RAW.EVENTS_STREAM')",
		AllowOverlappingExecution: &allowOverlap,
	}

	got := createAssetFromSnowflakeTask(task, assetsPath)

	assert.Equal(t, "analytics.daily_refresh", got.Name)
	assert.Equal(t, pipeline.AssetTypeSnowflakeQuery, got.Type)
	assert.Equal(t, "daily_refresh.sql", got.ExecutableFile.Name)
	assert.Equal(t, filepath.Join(assetsPath, "analytics", "daily_refresh.sql"), got.ExecutableFile.Path)
	assert.Equal(t, "INSERT INTO mart.daily SELECT * FROM raw.events", got.ExecutableFile.Content)
	assert.Equal(t, []pipeline.Upstream{
		{
			Type:  "asset",
			Value: "analytics.root_task",
			Mode:  pipeline.UpstreamModeFull,
		},
	}, got.Upstreams)
	assert.Contains(t, got.Description, "Refreshes daily analytics.")
	assert.Contains(t, got.Description, "Imported Snowflake task: RAW_DB.Analytics.Daily Refresh")
	assert.Contains(t, got.Description, "Warehouse: COMPUTE_WH")
	assert.Contains(t, got.Description, "Schedule: USING CRON 0 5 * * * UTC")
	assert.Contains(t, got.Description, "Allow overlapping execution: false")
}

func TestSnowflakeTaskPredecessorAssetNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		predecessors string
		schema       string
		want         []string
	}{
		{
			name:         "json fully qualified predecessors",
			predecessors: `["RAW_DB.ANALYTICS.ROOT_TASK","RAW_DB.ANALYTICS.OTHER TASK"]`,
			schema:       "IGNORED",
			want:         []string{"analytics.root_task", "analytics.other_task"},
		},
		{
			name:         "unqualified predecessor uses current schema",
			predecessors: "ROOT_TASK",
			schema:       "Analytics",
			want:         []string{"analytics.root_task"},
		},
		{
			name:         "quoted predecessor keeps dots inside identifiers",
			predecessors: `"RAW.DB"."Data.Schema"."Root Task"`,
			schema:       "IGNORED",
			want:         []string{"data_schema.root_task"},
		},
		{
			name:         "duplicates are removed",
			predecessors: `["RAW_DB.ANALYTICS.ROOT_TASK","RAW_DB.ANALYTICS.ROOT_TASK"]`,
			schema:       "IGNORED",
			want:         []string{"analytics.root_task"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := snowflakeTaskPredecessorAssetNames(tt.predecessors, tt.schema)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestImportSelectedSnowflakeTasksSkipsExistingFilePath(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipelinePath := t.TempDir()
	assetsPath := filepath.Join(pipelinePath, "assets", "analytics")
	existingAssetPath := filepath.Join(assetsPath, "daily_refresh.sql")

	require.NoError(t, os.Mkdir(filepath.Join(pipelinePath, ".git"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pipelinePath, "pipeline.yml"), []byte("name: test-pipeline\n"), 0o644))
	require.NoError(t, os.MkdirAll(assetsPath, 0o755))

	originalContent := `/* @bruin

name: analytics.custom_existing_name
type: sf.sql

@bruin */

select 42
`
	require.NoError(t, os.WriteFile(existingAssetPath, []byte(originalContent), 0o644))

	err := importSelectedSnowflakeTasks(ctx, pipelinePath, []SnowflakeTask{
		{
			Schema:     "analytics",
			Name:       "daily_refresh",
			Definition: "select 1",
		},
	}, afero.NewOsFs())

	require.NoError(t, err)

	got, err := os.ReadFile(existingAssetPath)
	require.NoError(t, err)
	assert.Equal(t, originalContent, string(got))
}

func TestGetPipelinefromPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		inputPath string
		wantErr   string
	}{
		{
			name:      "invalid path",
			inputPath: "/nonexistent/path",
			wantErr:   "cannot find a pipeline the given task belongs to",
		},
		{
			name:      "empty path",
			inputPath: "",
			wantErr:   "cannot find a pipeline the given task belongs to",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			got, err := GetPipelinefromPath(ctx, tt.inputPath)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, got)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)
		})
	}
}
