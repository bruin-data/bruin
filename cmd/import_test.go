package cmd

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
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
		name        string
		schemaName  string
		tableName   string
		assetType   pipeline.AssetType
		fillColumns bool
		setupConn   func() *mockConnection
		want        *pipeline.Asset
		wantWarning string
	}{
		{
			name:        "successful asset creation without columns",
			schemaName:  "public",
			tableName:   "users",
			assetType:   pipeline.AssetTypePostgresSource,
			fillColumns: false,
			setupConn: func() *mockConnection {
				return &mockConnection{}
			},
			want: &pipeline.Asset{
				Type: pipeline.AssetTypePostgresSource,
				ExecutableFile: pipeline.ExecutableFile{
					Name: "users.asset.yml",
					Path: filepath.Join(testAssetsPath, "public", "users.asset.yml"),
				},
				Description: "Imported table public.users",
				Columns:     nil,
			},
		},
		{
			name:        "successful asset creation with columns",
			schemaName:  "public",
			tableName:   "products",
			assetType:   pipeline.AssetTypePostgresSource,
			fillColumns: true,
			setupConn: func() *mockConnection {
				conn := &mockConnection{}
				conn.On("SelectWithSchema", mock.Anything, mock.MatchedBy(func(q *query.Query) bool {
					return strings.Contains(q.Query, "SELECT * FROM public.products WHERE 1=0 LIMIT 0")
				})).Return(&query.QueryResult{
					Columns:     []string{"id", "name", "price"},
					ColumnTypes: []string{"INTEGER", "VARCHAR", "DECIMAL"},
				}, nil)
				return conn
			},
			want: &pipeline.Asset{
				Type: pipeline.AssetTypePostgresSource,
				ExecutableFile: pipeline.ExecutableFile{
					Name: "products.asset.yml",
					Path: filepath.Join(testAssetsPath, "public", "products.asset.yml"),
				},
				Description: "Imported table public.products",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
					{Name: "name", Type: "VARCHAR", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
					{Name: "price", Type: "DECIMAL", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				},
			},
		},
		{
			name:        "asset creation with columns but connection fails",
			schemaName:  "public",
			tableName:   "orders",
			assetType:   pipeline.AssetTypePostgresSource,
			fillColumns: true,
			setupConn: func() *mockConnection {
				conn := &mockConnection{}
				conn.On("SelectWithSchema", mock.Anything, mock.Anything).Return((*query.QueryResult)(nil), errors.New("connection failed"))
				return conn
			},
			wantWarning: "Could not fill columns: connection failed",
		},
		{
			name:        "asset creation with special column names filtered out",
			schemaName:  "public",
			tableName:   "temporal_table",
			assetType:   pipeline.AssetTypePostgresSource,
			fillColumns: true,
			setupConn: func() *mockConnection {
				conn := &mockConnection{}
				conn.On("SelectWithSchema", mock.Anything, mock.Anything).Return(&query.QueryResult{
					Columns:     []string{"id", "_IS_CURRENT", "name", "_VALID_UNTIL", "_VALID_FROM"},
					ColumnTypes: []string{"INTEGER", "BOOLEAN", "VARCHAR", "TIMESTAMP", "TIMESTAMP"},
				}, nil)
				return conn
			},
			want: &pipeline.Asset{
				Type: pipeline.AssetTypePostgresSource,
				ExecutableFile: pipeline.ExecutableFile{
					Name: "temporal_table.asset.yml",
					Path: filepath.Join(testAssetsPath, "public", "temporal_table.asset.yml"),
				},
				Description: "Imported table public.temporal_table",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
					{Name: "name", Type: "VARCHAR", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			conn := tt.setupConn()

			got, warning := createAsset(ctx, testAssetsPath, tt.schemaName, tt.tableName, tt.assetType, conn, tt.fillColumns)

			if tt.wantWarning != "" {
				assert.Contains(t, warning, tt.wantWarning)
				return
			}

			assert.Equal(t, "", warning)
			assert.Equal(t, tt.want, got)

			if tt.fillColumns {
				conn.AssertExpectations(t)
			}
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
			name:           "snowflake connection type",
			connectionName: "test-conn",
			conn:           &struct{ mockConnection }{},
			want:           pipeline.AssetTypeSnowflakeSource,
		},
		{
			name:           "duckdb connection type",
			connectionName: "test-conn",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypeSnowflakeSource, // Default fallback
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
			name:           "unknown connection defaults to snowflake",
			connectionName: "unknown-db",
			conn:           &mockConnection{},
			want:           pipeline.AssetTypeSnowflakeSource,
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
