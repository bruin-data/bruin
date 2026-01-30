package cmd

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/mssql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/postgres"
	"github.com/bruin-data/bruin/pkg/query"
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

	for _, tt := range tests {
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

	for _, tt := range tests {
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
		})
	}
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
