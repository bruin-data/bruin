package ingestr

import (
	"context"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockConnection struct {
	mock.Mock
}

func (m *mockConnection) GetIngestrURI() (string, error) {
	res := m.Called()
	return res.String(0), res.Error(1)
}

type simpleConnectionFetcher struct {
	connections       map[string]*mockConnection
	connectionDetails map[string]any
}

var repo = &git.Repo{
	Path: "/the/repo",
}

type mockFinder struct{}

func (m *mockFinder) Repo(path string) (*git.Repo, error) {
	return repo, nil
}

func (s simpleConnectionFetcher) GetConnection(name string) any {
	conn, ok := s.connections[name]
	if !ok {
		return nil
	}

	return conn
}

func (s simpleConnectionFetcher) GetConnectionDetails(name string) any {
	if s.connectionDetails == nil {
		return nil
	}
	return s.connectionDetails[name]
}

func (s simpleConnectionFetcher) GetConnectionType(name string) string {
	return ""
}

type mockRunner struct {
	mock.Mock
}

func (m *mockRunner) RunIngestr(ctx context.Context, args, extraPackages []string, repo *git.Repo) error {
	return m.Called(ctx, args, extraPackages, repo).Error(0)
}

func TestApplyClickHouseEngineParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		uri    string
		params map[string]string
		want   string
	}{
		{
			name:   "no engine params",
			uri:    "clickhouse://user:pass@localhost:9000",
			params: map[string]string{},
			want:   "clickhouse://user:pass@localhost:9000",
		},
		{
			name: "engine only",
			uri:  "clickhouse://user:pass@localhost:9000",
			params: map[string]string{
				"engine": "merge_tree",
			},
			want: "clickhouse://user:pass@localhost:9000?engine=merge_tree",
		},
		{
			name: "engine with settings",
			uri:  "clickhouse://user:pass@localhost:9000",
			params: map[string]string{
				"engine":                   "merge_tree",
				"engine.index_granularity": "8125",
			},
			want: "clickhouse://user:pass@localhost:9000?engine=merge_tree&engine.index_granularity=8125",
		},
		{
			name: "engine settings without engine",
			uri:  "clickhouse://user:pass@localhost:9000",
			params: map[string]string{
				"engine.index_granularity": "8125",
			},
			want: "clickhouse://user:pass@localhost:9000?engine.index_granularity=8125",
		},
		{
			name: "preserves existing query params",
			uri:  "clickhouse://user:pass@localhost:9000?http_port=8123",
			params: map[string]string{
				"engine": "merge_tree",
			},
			want: "clickhouse://user:pass@localhost:9000?engine=merge_tree&http_port=8123",
		},
		{
			name: "empty engine value is ignored",
			uri:  "clickhouse://user:pass@localhost:9000",
			params: map[string]string{
				"engine": "",
			},
			want: "clickhouse://user:pass@localhost:9000",
		},
		{
			name: "empty engine setting value is ignored",
			uri:  "clickhouse://user:pass@localhost:9000",
			params: map[string]string{
				"engine.index_granularity": "",
			},
			want: "clickhouse://user:pass@localhost:9000",
		},
		{
			name: "non-engine params are not added",
			uri:  "clickhouse://user:pass@localhost:9000",
			params: map[string]string{
				"source_connection": "sf",
				"source_table":      "some_table",
				"engine":            "merge_tree",
			},
			want: "clickhouse://user:pass@localhost:9000?engine=merge_tree",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := applyClickHouseEngineParams(tt.uri, tt.params)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestBasicOperator_ConvertTaskInstanceToIngestrCommand(t *testing.T) {
	t.Parallel()

	mockBq := new(mockConnection)
	mockBq.On("GetIngestrURI").Return("bigquery://uri-here", nil)
	mockSf := new(mockConnection)
	mockSf.On("GetIngestrURI").Return("snowflake://uri-here", nil)
	mockDuck := new(mockConnection)
	mockDuck.On("GetIngestrURI").Return("duckdb:////some/path", nil)
	mockCh := new(mockConnection)
	mockCh.On("GetIngestrURI").Return("clickhouse://user:pass@localhost:9000", nil)

	fetcher := simpleConnectionFetcher{
		connections: map[string]*mockConnection{
			"bq":   mockBq,
			"sf":   mockSf,
			"duck": mockDuck,
			"ch":   mockCh,
		},
	}

	finder := new(mockFinder)

	tests := []struct {
		name          string
		asset         *pipeline.Asset
		fullRefresh   bool
		want          []string
		extraPackages []string
	}{
		{
			name: "clickhouse dest with engine params",
			asset: &pipeline.Asset{
				Name:       "public.table",
				Connection: "ch",
				Parameters: map[string]string{
					"source_connection":        "sf",
					"source_table":             "source-table",
					"engine":                   "merge_tree",
					"engine.index_granularity": "8125",
				},
			},
			want: []string{
				"ingest",
				"--source-uri", "snowflake://uri-here",
				"--source-table", "source-table",
				"--dest-uri", "clickhouse://user:pass@localhost:9000?engine=merge_tree&engine.index_granularity=8125",
				"--dest-table", "public.table",
				"--yes",
				"--progress", "log",
			},
		},
		{
			name: "create+replace, basic scenario",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "bq",
				Parameters: map[string]string{
					"source_connection": "sf",
					"source_table":      "source-table",
					"destination":       "bigquery",
				},
			},
			want: []string{"ingest", "--source-uri", "snowflake://uri-here", "--source-table", "source-table", "--dest-uri", "bigquery://uri-here", "--dest-table", "asset-name", "--yes", "--progress", "log"},
		},
		{
			name: "duck db source, basic scenario",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "bq",
				Parameters: map[string]string{
					"source_connection": "duck",
					"source_table":      "source-table",
					"destination":       "bigquery",
					"enforce_schema":    "true",
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "integer"},
					{Name: "name", Type: "string"},
					{Name: "DateOfBirth", Type: "integer"},
				},
			},
			want: []string{"ingest", "--source-uri", "duckdb:////some/path", "--source-table", "source-table", "--dest-uri", "bigquery://uri-here", "--dest-table", "asset-name", "--yes", "--progress", "log", "--columns", "id:bigint,name:text,DateOfBirth:bigint"},
		},
		{
			name: "duck db dest, basic scenario",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "duck",
				Parameters: map[string]string{
					"source_connection": "sf",
					"source_table":      "source-table",
					"destination":       "duckdb",
				},
			},
			want: []string{"ingest", "--source-uri", "snowflake://uri-here", "--source-table", "source-table", "--dest-uri", "duckdb:////some/path", "--dest-table", "asset-name", "--yes", "--progress", "log"},
		},
		{
			name: "basic scenario with Google Sheets override",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "sf",
				Parameters: map[string]string{
					"source_connection": "bq",
					"source":            "gsheets",
					"source_table":      "source-table",
					"destination":       "bigquery",
				},
			},
			want: []string{"ingest", "--source-uri", "gsheets://uri-here", "--source-table", "source-table", "--dest-uri", "snowflake://uri-here", "--dest-table", "asset-name", "--yes", "--progress", "log"},
		},
		{
			name: "incremental strategy, incremental key updated_at",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "bq",
				Parameters: map[string]string{
					"source_connection":    "sf",
					"source_table":         "source-table",
					"destination":          "bigquery",
					"incremental_strategy": "merge",
					"incremental_key":      "updated_at",
				},
			},
			want: []string{
				"ingest",
				"--source-uri", "snowflake://uri-here",
				"--source-table", "source-table",
				"--dest-uri", "bigquery://uri-here",
				"--dest-table", "asset-name",
				"--yes",
				"--progress", "log",
				"--incremental-key", "updated_at",
				"--incremental-strategy", "merge",
			},
		},
		{
			name: "incremental strategy, incremental key updated_at, single pk",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "bq",
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "name"},
					{Name: "updated_at"},
				},
				Parameters: map[string]string{
					"source_connection":    "sf",
					"source_table":         "source-table",
					"destination":          "bigquery",
					"incremental_strategy": "merge",
					"incremental_key":      "updated_at",
				},
			},
			want: []string{
				"ingest",
				"--source-uri", "snowflake://uri-here",
				"--source-table", "source-table",
				"--dest-uri", "bigquery://uri-here",
				"--dest-table", "asset-name",
				"--yes",
				"--progress", "log",
				"--incremental-key", "updated_at",
				"--incremental-strategy", "merge",
				"--primary-key", "id",
			},
		},
		{
			name: "incremental strategy, incremental key updated_at, multiple pk",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "bq",
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "date", PrimaryKey: true},
					{Name: "name"},
					{Name: "updated_at"},
				},
				Parameters: map[string]string{
					"source_connection":    "sf",
					"source_table":         "source-table",
					"destination":          "bigquery",
					"incremental_strategy": "merge",
					"incremental_key":      "updated_at",
				},
			},
			want: []string{
				"ingest",
				"--source-uri", "snowflake://uri-here",
				"--source-table", "source-table",
				"--dest-uri", "bigquery://uri-here",
				"--dest-table", "asset-name",
				"--yes",
				"--progress", "log",
				"--incremental-key", "updated_at",
				"--incremental-strategy", "merge",
				"--primary-key", "id",
				"--primary-key", "date",
			},
		},
		{
			name:        "full refresh - incremental strategy, incremental key updated_at, multiple pk",
			fullRefresh: true,
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "bq",
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "date", PrimaryKey: true},
					{Name: "name"},
					{Name: "updated_at"},
				},
				Parameters: map[string]string{
					"source_connection":    "sf",
					"source_table":         "source-table",
					"destination":          "bigquery",
					"incremental_strategy": "merge",
					"incremental_key":      "updated_at",
				},
			},
			want: []string{
				"ingest",
				"--source-uri", "snowflake://uri-here",
				"--source-table", "source-table",
				"--dest-uri", "bigquery://uri-here",
				"--dest-table", "asset-name",
				"--yes",
				"--progress", "log",
				"--incremental-key", "updated_at",
				"--incremental-strategy", "merge",
				"--primary-key", "id",
				"--primary-key", "date",
				"--full-refresh",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := new(mockRunner)
			runner.On("RunIngestr", mock.Anything, tt.want, tt.extraPackages, repo).Return(nil)

			startDate := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
			executionDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

			o := &BasicOperator{
				conn:          &fetcher,
				finder:        finder,
				runner:        runner,
				jinjaRenderer: jinja.NewRendererWithStartEndDatesAndMacros(&startDate, &endDate, &executionDate, "ingestr-test", "ingestr-test", nil, ""),
			}

			ti := scheduler.AssetInstance{
				Pipeline: &pipeline.Pipeline{},
				Asset:    tt.asset,
			}

			ctx := context.WithValue(t.Context(), pipeline.RunConfigFullRefresh, tt.fullRefresh)

			err := o.Run(ctx, &ti)
			require.NoError(t, err)
		})
	}
}

func TestBasicOperator_ConvertTaskInstanceToIngestrCommand_IntervalStartAndEnd(t *testing.T) {
	t.Parallel()
	mockSf := new(mockConnection)
	mockSf.On("GetIngestrURI").Return("snowflake://uri-here", nil)
	mockMS := new(mockConnection)
	mockMS.On("GetIngestrURI").Return("mssql://uri-here", nil)

	fetcher := simpleConnectionFetcher{
		connections: map[string]*mockConnection{
			"sf": mockSf,
			"ms": mockMS,
		},
	}

	finder := new(mockFinder)

	runner := new(mockRunner)
	runner.On("RunIngestr", mock.Anything, []string{
		"ingest",
		"--source-uri", "snowflake://uri-here",
		"--source-table", "source-table",
		"--dest-uri", "mssql://uri-here",
		"--dest-table", "asset-name",
		"--yes",
		"--progress", "log",
		"--interval-start", "2025-01-01T00:00:00Z",
		"--interval-end", "2025-01-02T00:00:00Z",
	}, []string{"pyodbc==5.1.0"}, repo).Return(nil)

	o := &BasicOperator{
		conn:          &fetcher,
		finder:        finder,
		runner:        runner,
		jinjaRenderer: jinja.NewRendererWithYesterday("ingestr-test", "ingestr-test"),
	}

	ti := scheduler.AssetInstance{
		Pipeline: &pipeline.Pipeline{},
		Asset: &pipeline.Asset{
			Name:       "asset-name",
			Type:       pipeline.AssetTypeMsSQLQuery,
			Connection: "ms",
			Parameters: map[string]string{
				"source_connection": "sf",
				"source_table":      "source-table",
				"destination":       "mssql",
			},
		},
	}

	ctx := context.WithValue(t.Context(), pipeline.RunConfigFullRefresh, false)
	ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC))
	ctx = context.WithValue(ctx, pipeline.RunConfigExecutionDate, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	err := o.Run(ctx, &ti)
	require.NoError(t, err)
}

func TestBasicOperator_ConvertSeedTaskInstanceToIngestrCommand(t *testing.T) {
	t.Parallel()

	mockBq := new(mockConnection)
	mockBq.On("GetIngestrURI").Return("bigquery://uri-here", nil)
	mockSf := new(mockConnection)
	mockSf.On("GetIngestrURI").Return("snowflake://uri-here", nil)
	mockDuck := new(mockConnection)
	mockDuck.On("GetIngestrURI").Return("duckdb:////some/path", nil)
	mockAthena := new(mockConnection)
	mockAthena.On("GetIngestrURI").Return("athena://?bucket=s3://bucket/path&region_name=us-west-2", nil)

	fetcher := simpleConnectionFetcher{
		connections: map[string]*mockConnection{
			"bq":     mockBq,
			"sf":     mockSf,
			"duck":   mockDuck,
			"athena": mockAthena,
		},
		connectionDetails: map[string]any{
			"athena": &config.AthenaConnection{
				Name:     "athena",
				Database: "analytics",
			},
		},
	}

	finder := new(mockFinder)

	tests := []struct {
		name          string
		asset         *pipeline.Asset
		fullRefresh   bool
		want          []string
		extraPackages []string
	}{
		{
			name: "create+replace, basic scenario",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "bq",
				Type:       pipeline.AssetTypeBigquerySeed,
				Parameters: map[string]string{
					"path": "seed.csv",
				},
			},
			want: []string{"ingest", "--source-uri", "csv://seed.csv", "--source-table", "seed.raw", "--dest-uri", "bigquery://uri-here", "--dest-table", "asset-name", "--yes", "--progress", "log"},
		},
		{
			name: "duck db source, basic scenario",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Type:       pipeline.AssetTypeDuckDBSeed,
				Connection: "duck",
				Parameters: map[string]string{
					"path": "seed.csv",
				},
			},
			want: []string{"ingest", "--source-uri", "csv://seed.csv", "--source-table", "seed.raw", "--dest-uri", "duckdb:////some/path", "--dest-table", "asset-name", "--yes", "--progress", "log"},
		},
		{
			name: "snowflake seed, type hints test",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "duck",
				Parameters: map[string]string{
					"path": "seed.csv",
				},
				Columns: []pipeline.Column{
					{
						Name: "id",
						Type: "integer",
					},
					{
						Name: "load_date",
						Type: "timestamp_tz",
					},
					{
						Name: "percent",
						Type: "float4",
					},
				},
			},
			want: []string{
				"ingest",
				"--source-uri",
				"csv://seed.csv",
				"--source-table",
				"seed.raw",
				"--dest-uri",
				"duckdb:////some/path",
				"--dest-table",
				"asset-name",
				"--yes",
				"--progress",
				"log",
				"--columns",
				"id:bigint,load_date:timestamp,percent:double",
			},
		},
		{
			name: "athena seed infers database from connection details",
			asset: &pipeline.Asset{
				Name:       "events",
				Connection: "athena",
				Type:       pipeline.AssetTypeAthenaSeed,
				Parameters: map[string]string{
					"path": "seed.csv",
				},
			},
			want: []string{
				"ingest",
				"--source-uri",
				"csv://seed.csv",
				"--source-table",
				"seed.raw",
				"--dest-uri",
				"athena://?bucket=s3://bucket/path&region_name=us-west-2",
				"--dest-table",
				"analytics.events",
				"--yes",
				"--progress",
				"log",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := new(mockRunner)
			runner.On("RunIngestr", mock.Anything, tt.want, tt.extraPackages, repo).Return(nil)

			o := &SeedOperator{
				conn:   &fetcher,
				finder: finder,
				runner: runner,
			}

			ti := scheduler.AssetInstance{
				Pipeline: &pipeline.Pipeline{},
				Asset:    tt.asset,
			}

			ctx := context.WithValue(t.Context(), pipeline.RunConfigFullRefresh, tt.fullRefresh)

			err := o.Run(ctx, &ti)
			require.NoError(t, err)
		})
	}
}

func TestSeedOperator_ResolveSeedDestinationTableName(t *testing.T) {
	t.Parallel()

	fetcher := simpleConnectionFetcher{
		connectionDetails: map[string]any{
			"athena": &config.AthenaConnection{
				Name:     "athena",
				Database: "analytics",
			},
			"pg": &config.PostgresConnection{
				Name:   "pg",
				Schema: "mart",
			},
			"rs": &config.RedshiftConnection{
				Name:   "rs",
				Schema: "warehouse",
			},
			"sf": &config.SnowflakeConnection{
				Name:   "sf",
				Schema: "RAW",
			},
			"ch": &config.ClickHouseConnection{
				Name:     "ch",
				Database: "events",
			},
		},
	}

	o := &SeedOperator{conn: &fetcher}

	tests := []struct {
		name           string
		connectionName string
		destURI        string
		tableName      string
		want           string
	}{
		{
			name:           "athena table gets default database from connection",
			connectionName: "athena",
			destURI:        "athena://?bucket=s3://bucket/path",
			tableName:      "events",
			want:           "analytics.events",
		},
		{
			name:           "athena falls back to default database when details are missing",
			connectionName: "missing-athena",
			destURI:        "athena://?bucket=s3://bucket/path",
			tableName:      "events",
			want:           "default.events",
		},
		{
			name:           "postgresql table gets schema from connection",
			connectionName: "pg",
			destURI:        "postgresql://user:pass@localhost:5432/db",
			tableName:      "users",
			want:           "mart.users",
		},
		{
			name:           "redshift table gets schema from connection",
			connectionName: "rs",
			destURI:        "redshift://user:pass@localhost:5439/db",
			tableName:      "orders",
			want:           "warehouse.orders",
		},
		{
			name:           "snowflake table gets schema from connection",
			connectionName: "sf",
			destURI:        "snowflake://user:pass@account/db",
			tableName:      "customers",
			want:           "RAW.customers",
		},
		{
			name:           "clickhouse table gets database from connection",
			connectionName: "ch",
			destURI:        "clickhouse://user:pass@localhost:9000",
			tableName:      "sessions",
			want:           "events.sessions",
		},
		{
			name:           "already qualified table name is unchanged",
			connectionName: "pg",
			destURI:        "postgresql://user:pass@localhost:5432/db",
			tableName:      "mart.users",
			want:           "mart.users",
		},
		{
			name:           "unsupported destination keeps table name unchanged",
			connectionName: "bq",
			destURI:        "bigquery://project",
			tableName:      "events",
			want:           "events",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := o.resolveSeedDestinationTableName(tt.connectionName, tt.destURI, tt.tableName)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestBasicOperator_CDCMode(t *testing.T) {
	t.Parallel()

	mockPg := new(mockConnection)
	mockPg.On("GetIngestrURI").Return("postgresql://user:pass@localhost:5432/db", nil)
	mockBq := new(mockConnection)
	mockBq.On("GetIngestrURI").Return("bigquery://uri-here", nil)

	fetcher := simpleConnectionFetcher{
		connections: map[string]*mockConnection{
			"pg": mockPg,
			"bq": mockBq,
		},
	}

	finder := new(mockFinder)

	tests := []struct {
		name  string
		asset *pipeline.Asset
		want  []string
	}{
		{
			name: "CDC mode transforms URI and auto-sets merge strategy",
			asset: &pipeline.Asset{
				Name:       "cdc-asset",
				Connection: "bq",
				Parameters: map[string]string{
					"source_connection": "pg",
					"source_table":      "public.users",
					"destination":       "bigquery",
					"cdc":               "true",
				},
			},
			want: []string{
				"ingest",
				"--source-uri", "postgres+cdc://user:pass@localhost:5432/db",
				"--source-table", "public.users",
				"--dest-uri", "bigquery://uri-here",
				"--dest-table", "cdc-asset",
				"--yes",
				"--progress", "log",
				"--incremental-strategy", "merge",
			},
		},
		{
			name: "CDC mode with publication and slot params",
			asset: &pipeline.Asset{
				Name:       "cdc-asset-with-params",
				Connection: "bq",
				Parameters: map[string]string{
					"source_connection": "pg",
					"source_table":      "public.users",
					"destination":       "bigquery",
					"cdc":               "true",
					"cdc_publication":   "my_publication",
					"cdc_slot":          "my_slot",
				},
			},
			want: []string{
				"ingest",
				"--source-uri", "postgres+cdc://user:pass@localhost:5432/db?publication=my_publication&slot=my_slot",
				"--source-table", "public.users",
				"--dest-uri", "bigquery://uri-here",
				"--dest-table", "cdc-asset-with-params",
				"--yes",
				"--progress", "log",
				"--incremental-strategy", "merge",
			},
		},
		{
			name: "CDC wildcard source_table omits --source-table flag",
			asset: &pipeline.Asset{
				Name:       "cdc-wildcard-asset",
				Connection: "bq",
				Parameters: map[string]string{
					"source_connection": "pg",
					"source_table":      "*",
					"destination":       "bigquery",
					"cdc":               "true",
				},
			},
			want: []string{
				"ingest",
				"--source-uri", "postgres+cdc://user:pass@localhost:5432/db",
				"--dest-uri", "bigquery://uri-here",
				"--dest-table", "cdc-wildcard-asset",
				"--yes",
				"--progress", "log",
				"--incremental-strategy", "merge",
			},
		},
		{
			name: "CDC mode with cdc_mode stream",
			asset: &pipeline.Asset{
				Name:       "cdc-asset-stream-mode",
				Connection: "bq",
				Parameters: map[string]string{
					"source_connection": "pg",
					"source_table":      "public.users",
					"destination":       "bigquery",
					"cdc":               "true",
					"cdc_mode":          "stream",
				},
			},
			want: []string{
				"ingest",
				"--source-uri", "postgres+cdc://user:pass@localhost:5432/db?mode=stream",
				"--source-table", "public.users",
				"--dest-uri", "bigquery://uri-here",
				"--dest-table", "cdc-asset-stream-mode",
				"--yes",
				"--progress", "log",
				"--incremental-strategy", "merge",
			},
		},
		{
			name: "CDC mode with explicit incremental strategy",
			asset: &pipeline.Asset{
				Name:       "cdc-asset-explicit-strategy",
				Connection: "bq",
				Parameters: map[string]string{
					"source_connection":    "pg",
					"source_table":         "public.users",
					"destination":          "bigquery",
					"cdc":                  "true",
					"incremental_strategy": "append",
				},
			},
			want: []string{
				"ingest",
				"--source-uri", "postgres+cdc://user:pass@localhost:5432/db",
				"--source-table", "public.users",
				"--dest-uri", "bigquery://uri-here",
				"--dest-table", "cdc-asset-explicit-strategy",
				"--yes",
				"--progress", "log",
				"--incremental-strategy", "append",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := new(mockRunner)
			runner.On("RunIngestr", mock.Anything, tt.want, []string(nil), repo).Return(nil)

			startDate := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
			executionDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

			o := &BasicOperator{
				conn:          &fetcher,
				finder:        finder,
				runner:        runner,
				jinjaRenderer: jinja.NewRendererWithStartEndDates(&startDate, &endDate, &executionDate, "ingestr-test", "ingestr-test", nil),
			}

			ti := scheduler.AssetInstance{
				Pipeline: &pipeline.Pipeline{},
				Asset:    tt.asset,
			}

			ctx := context.WithValue(t.Context(), pipeline.RunConfigFullRefresh, false)

			err := o.Run(ctx, &ti)
			require.NoError(t, err)
		})
	}
}

func TestBasicOperator_Run_MissingConnectionErrorIsActionable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		connections      map[string]string
		assetConnection  string
		sourceConnection string
		secretsBackend   string
		expectedParts    []string
		notExpectedParts []string
	}{
		{
			name: "missing source connection uses .bruin.yml guidance",
			connections: map[string]string{
				"bq": "bigquery://uri-here",
			},
			assetConnection:  "bq",
			sourceConnection: "missing-source",
			expectedParts: []string{
				"source connection 'missing-source' not found",
				"Configure it under the correct environment in '.bruin.yml' at the repository root",
				"--config-file",
			},
		},
		{
			name: "missing destination connection uses .bruin.yml guidance",
			connections: map[string]string{
				"sf": "snowflake://uri-here",
			},
			assetConnection:  "missing-destination",
			sourceConnection: "sf",
			expectedParts: []string{
				"destination connection 'missing-destination' not found",
				"Configure it under the correct environment in '.bruin.yml' at the repository root",
				"--config-file",
			},
		},
		{
			name: "missing source connection uses secrets backend guidance",
			connections: map[string]string{
				"bq": "bigquery://uri-here",
			},
			assetConnection:  "bq",
			sourceConnection: "missing-source",
			secretsBackend:   "vault",
			expectedParts: []string{
				"source connection 'missing-source' not found",
				"Configure it in the 'vault' secrets backend",
				"--secrets-backend",
			},
			notExpectedParts: []string{
				".bruin.yml",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			connectionMap := make(map[string]*mockConnection, len(tt.connections))
			for name, uri := range tt.connections {
				conn := new(mockConnection)
				conn.On("GetIngestrURI").Return(uri, nil)
				connectionMap[name] = conn
			}

			o := &BasicOperator{
				conn: &simpleConnectionFetcher{
					connections: connectionMap,
				},
				finder:        &mockFinder{},
				runner:        &mockRunner{},
				jinjaRenderer: jinja.NewRendererWithYesterday("ingestr-test", "ingestr-test"),
			}

			ti := scheduler.AssetInstance{
				Pipeline: &pipeline.Pipeline{},
				Asset: &pipeline.Asset{
					Name:       "asset-name",
					Connection: tt.assetConnection,
					Parameters: map[string]string{
						"source_connection": tt.sourceConnection,
						"source_table":      "source-table",
						"destination":       "bigquery",
					},
				},
			}

			ctx := t.Context()
			if tt.secretsBackend != "" {
				ctx = context.WithValue(ctx, config.SecretsBackendContextKey, tt.secretsBackend)
			}

			err := o.Run(ctx, &ti)
			require.Error(t, err)

			for _, expected := range tt.expectedParts {
				require.Contains(t, err.Error(), expected)
			}

			for _, notExpected := range tt.notExpectedParts {
				require.NotContains(t, err.Error(), notExpected)
			}
		})
	}
}
