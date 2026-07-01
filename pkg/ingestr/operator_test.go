package ingestr

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/assert"
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

// contextCapturingRunner captures the context passed to RunIngestr for assertion.
type contextCapturingRunner struct {
	mock.Mock
	capturedCtx context.Context //nolint
}

func (m *contextCapturingRunner) RunIngestr(ctx context.Context, args, extraPackages []string, repo *git.Repo) error {
	m.capturedCtx = ctx
	return m.Called(ctx, args, extraPackages, repo).Error(0)
}

func TestApplyClickHouseEngineParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		uri    string
		params pipeline.ParameterMap
		want   string
	}{
		{
			name:   "no engine params",
			uri:    "clickhouse://user:pass@localhost:9000",
			params: pipeline.ParameterMap{},
			want:   "clickhouse://user:pass@localhost:9000",
		},
		{
			name: "engine only",
			uri:  "clickhouse://user:pass@localhost:9000",
			params: pipeline.ParameterMap{
				"engine": "merge_tree",
			},
			want: "clickhouse://user:pass@localhost:9000?engine=merge_tree",
		},
		{
			name: "engine with settings",
			uri:  "clickhouse://user:pass@localhost:9000",
			params: pipeline.ParameterMap{
				"engine":                   "merge_tree",
				"engine.index_granularity": "8125",
			},
			want: "clickhouse://user:pass@localhost:9000?engine=merge_tree&engine.index_granularity=8125",
		},
		{
			name: "engine settings without engine",
			uri:  "clickhouse://user:pass@localhost:9000",
			params: pipeline.ParameterMap{
				"engine.index_granularity": "8125",
			},
			want: "clickhouse://user:pass@localhost:9000?engine.index_granularity=8125",
		},
		{
			name: "preserves existing query params",
			uri:  "clickhouse://user:pass@localhost:9000?http_port=8123",
			params: pipeline.ParameterMap{
				"engine": "merge_tree",
			},
			want: "clickhouse://user:pass@localhost:9000?engine=merge_tree&http_port=8123",
		},
		{
			name: "empty engine value is ignored",
			uri:  "clickhouse://user:pass@localhost:9000",
			params: pipeline.ParameterMap{
				"engine": "",
			},
			want: "clickhouse://user:pass@localhost:9000",
		},
		{
			name: "empty engine setting value is ignored",
			uri:  "clickhouse://user:pass@localhost:9000",
			params: pipeline.ParameterMap{
				"engine.index_granularity": "",
			},
			want: "clickhouse://user:pass@localhost:9000",
		},
		{
			name: "non-engine params are not added",
			uri:  "clickhouse://user:pass@localhost:9000",
			params: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
					"source_connection": "sf",
					"source_table":      "source-table",
					"destination":       "bigquery",
				},
			},
			want: []string{"ingest", "--source-uri", "snowflake://uri-here", "--source-table", "source-table", "--dest-uri", "bigquery://uri-here", "--dest-table", "asset-name", "--yes", "--progress", "log"},
		},
		{
			name: "trim whitespace",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "bq",
				Parameters: pipeline.ParameterMap{
					"source_connection": "sf",
					"source_table":      "source-table",
					"destination":       "bigquery",
					"trim_whitespace":   "true",
				},
			},
			want: []string{"ingest", "--source-uri", "snowflake://uri-here", "--source-table", "source-table", "--dest-uri", "bigquery://uri-here", "--dest-table", "asset-name", "--yes", "--progress", "log", "--trim-whitespace"},
		},
		{
			name: "duck db source, basic scenario",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "bq",
				Parameters: pipeline.ParameterMap{
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
			want: []string{"ingest", "--source-uri", "duckdb:////some/path", "--source-table", "source-table", "--dest-uri", "bigquery://uri-here", "--dest-table", "asset-name", "--yes", "--progress", "log", "--columns", "id:int,name:text,DateOfBirth:int"},
		},
		{
			name: "duck db dest, basic scenario",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "duck",
				Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
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
			Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
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
				"id:int,load_date:timestamp,percent:double",
			},
		},
		{
			name: "athena seed infers database from connection details",
			asset: &pipeline.Asset{
				Name:       "events",
				Connection: "athena",
				Type:       pipeline.AssetTypeAthenaSeed,
				Parameters: pipeline.ParameterMap{
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
		{
			name: "parquet seed uses parquet:// scheme",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "duck",
				Type:       pipeline.AssetTypeDuckDBSeed,
				Parameters: pipeline.ParameterMap{
					"path": "seed.parquet",
				},
			},
			want: []string{"ingest", "--source-uri", "parquet://seed.parquet", "--source-table", "seed.raw", "--dest-uri", "duckdb:////some/path", "--dest-table", "asset-name", "--yes", "--progress", "log"},
		},
		{
			name: "jsonl seed uses jsonl:// scheme",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "duck",
				Type:       pipeline.AssetTypeDuckDBSeed,
				Parameters: pipeline.ParameterMap{
					"path": "seed.jsonl",
				},
			},
			want: []string{"ingest", "--source-uri", "jsonl://seed.jsonl", "--source-table", "seed.raw", "--dest-uri", "duckdb:////some/path", "--dest-table", "asset-name", "--yes", "--progress", "log"},
		},
		{
			name: "ndjson extension maps to ndjson:// scheme",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "duck",
				Type:       pipeline.AssetTypeDuckDBSeed,
				Parameters: pipeline.ParameterMap{
					"path": "seed.ndjson",
				},
			},
			want: []string{"ingest", "--source-uri", "ndjson://seed.ndjson", "--source-table", "seed.raw", "--dest-uri", "duckdb:////some/path", "--dest-table", "asset-name", "--yes", "--progress", "log"},
		},
		{
			name: "json seed uses json:// scheme",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "duck",
				Type:       pipeline.AssetTypeDuckDBSeed,
				Parameters: pipeline.ParameterMap{
					"path": "seed.json",
				},
			},
			want: []string{"ingest", "--source-uri", "json://seed.json", "--source-table", "seed.raw", "--dest-uri", "duckdb:////some/path", "--dest-table", "asset-name", "--yes", "--progress", "log"},
		},
		{
			name: "avro seed uses avro:// scheme",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "duck",
				Type:       pipeline.AssetTypeDuckDBSeed,
				Parameters: pipeline.ParameterMap{
					"path": "seed.avro",
				},
			},
			want: []string{"ingest", "--source-uri", "avro://seed.avro", "--source-table", "seed.raw", "--dest-uri", "duckdb:////some/path", "--dest-table", "asset-name", "--yes", "--progress", "log"},
		},
		{
			name: "explicit file_type parameter overrides extension",
			asset: &pipeline.Asset{
				Name:       "asset-name",
				Connection: "duck",
				Type:       pipeline.AssetTypeDuckDBSeed,
				Parameters: pipeline.ParameterMap{
					"path":      "seed.dat",
					"file_type": "parquet",
				},
			},
			want: []string{"ingest", "--source-uri", "parquet://seed.dat", "--source-table", "seed.raw", "--dest-uri", "duckdb:////some/path", "--dest-table", "asset-name", "--yes", "--progress", "log"},
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

func TestResolveSeedSourceURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seedPath  string
		fileType  string
		assetDir  string
		want      string
		expectErr string
	}{
		{name: "csv extension", seedPath: "data.csv", want: "csv://data.csv"},
		{name: "parquet extension", seedPath: "data.parquet", want: "parquet://data.parquet"},
		{name: "pq extension maps to parquet", seedPath: "data.pq", want: "parquet://data.pq"},
		{name: "jsonl extension", seedPath: "data.jsonl", want: "jsonl://data.jsonl"},
		{name: "ndjson extension", seedPath: "data.ndjson", want: "ndjson://data.ndjson"},
		{name: "json extension", seedPath: "data.json", want: "json://data.json"},
		{name: "avro extension", seedPath: "data.avro", want: "avro://data.avro"},
		{name: "unknown extension falls back to csv", seedPath: "data.dat", want: "csv://data.dat"},
		{name: "no extension falls back to csv", seedPath: "data", want: "csv://data"},
		{
			name:     "explicit file_type overrides extension",
			seedPath: "data.csv", fileType: "parquet", want: "parquet://data.csv",
		},
		{
			name:     "file_type is case insensitive",
			seedPath: "data", fileType: "Parquet", want: "parquet://data",
		},
		{
			name:     "asset dir is joined with relative path",
			seedPath: "seed.parquet", assetDir: filepath.Join("/repo", "assets"),
			want: "parquet://" + filepath.Join("/repo", "assets", "seed.parquet"),
		},
		{name: "http URL is passed through unchanged", seedPath: "http://example.com/data.parquet", want: "http://example.com/data.parquet"},
		{name: "https URL is passed through unchanged", seedPath: "https://example.com/data.parquet", want: "https://example.com/data.parquet"},
		{
			name:      "invalid file_type returns error",
			seedPath:  "data.txt",
			fileType:  "xml",
			expectErr: "unsupported seed file_type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := resolveSeedSourceURI(tt.seedPath, tt.fileType, tt.assetDir)
			if tt.expectErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
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
				Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
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

func TestBasicOperator_MySQLCDCMode(t *testing.T) {
	t.Parallel()

	// A bruin MySQL connection emits a mysql+pymysql:// URI. These cases also exercise the
	// operator's generic CDC parameter plumbing (grpc_* / cdc_backend) on the MySQL family;
	// the dedicated vitess:// / planetscale:// schemes are covered by
	// TestBasicOperator_VitessPlanetScaleCDCMode.
	mockMy := new(mockConnection)
	mockMy.On("GetIngestrURI").Return("mysql+pymysql://user:pass@localhost:3306/db", nil)
	mockBq := new(mockConnection)
	mockBq.On("GetIngestrURI").Return("bigquery://uri-here", nil)

	fetcher := simpleConnectionFetcher{
		connections: map[string]*mockConnection{
			"my": mockMy,
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
			name: "MySQL CDC transforms URI and auto-sets merge strategy",
			asset: &pipeline.Asset{
				Name:       "cdc-mysql-asset",
				Connection: "bq",
				Parameters: pipeline.ParameterMap{
					"source_connection": "my",
					"source_table":      "orders",
					"destination":       "bigquery",
					"cdc":               "true",
				},
			},
			want: []string{
				"ingest",
				"--source-uri", "mysql+pymysql+cdc://user:pass@localhost:3306/db",
				"--source-table", "orders",
				"--dest-uri", "bigquery://uri-here",
				"--dest-table", "cdc-mysql-asset",
				"--yes",
				"--progress", "log",
				"--incremental-strategy", "merge",
			},
		},
		{
			name: "Vitess CDC adds VStream grpc parameters",
			asset: &pipeline.Asset{
				Name:       "cdc-vitess-asset",
				Connection: "bq",
				Parameters: pipeline.ParameterMap{
					"source_connection": "my",
					"source_table":      "orders",
					"destination":       "bigquery",
					"cdc":               "true",
					"cdc_grpc_port":     "15991",
					"cdc_grpc_host":     "vtgate.internal",
					"cdc_grpc_tls":      "true",
				},
			},
			want: []string{
				"ingest",
				"--source-uri", "mysql+pymysql+cdc://user:pass@localhost:3306/db?grpc_host=vtgate.internal&grpc_port=15991&grpc_tls=true",
				"--source-table", "orders",
				"--dest-uri", "bigquery://uri-here",
				"--dest-table", "cdc-vitess-asset",
				"--yes",
				"--progress", "log",
				"--incremental-strategy", "merge",
			},
		},
		{
			name: "PlanetScale CDC forces the psdbconnect backend",
			asset: &pipeline.Asset{
				Name:       "cdc-planetscale-asset",
				Connection: "bq",
				Parameters: pipeline.ParameterMap{
					"source_connection": "my",
					"source_table":      "orders",
					"destination":       "bigquery",
					"cdc":               "true",
					"cdc_backend":       "planetscale",
				},
			},
			want: []string{
				"ingest",
				"--source-uri", "mysql+pymysql+cdc://user:pass@localhost:3306/db?cdc_backend=planetscale",
				"--source-table", "orders",
				"--dest-uri", "bigquery://uri-here",
				"--dest-table", "cdc-planetscale-asset",
				"--yes",
				"--progress", "log",
				"--incremental-strategy", "merge",
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

func TestBasicOperator_VitessPlanetScaleCDCMode(t *testing.T) {
	t.Parallel()

	// Dedicated Vitess/PlanetScale connections emit the vitess:// / planetscale:// schemes.
	// The vitess connection carries the vtgate gRPC port in the URI (from its config builder),
	// so it survives into the derived vitess+cdc:// scheme without any asset-level parameters.
	mockVitess := new(mockConnection)
	mockVitess.On("GetIngestrURI").Return("vitess://user:pass@vtgate.internal:15306/commerce?grpc_port=15991", nil)
	mockPS := new(mockConnection)
	mockPS.On("GetIngestrURI").Return("planetscale://user:pass@aws.connect.psdb.cloud:3306/my_database", nil)
	mockBq := new(mockConnection)
	mockBq.On("GetIngestrURI").Return("bigquery://uri-here", nil)

	fetcher := simpleConnectionFetcher{
		connections: map[string]*mockConnection{
			"vitess":      mockVitess,
			"planetscale": mockPS,
			"bq":          mockBq,
		},
	}

	finder := new(mockFinder)

	tests := []struct {
		name  string
		asset *pipeline.Asset
		want  []string
	}{
		{
			name: "Vitess source is left untouched when CDC is disabled",
			asset: &pipeline.Asset{
				Name:       "vitess-asset",
				Connection: "bq",
				Parameters: pipeline.ParameterMap{
					"source_connection": "vitess",
					"source_table":      "orders",
					"destination":       "bigquery",
				},
			},
			want: []string{
				"ingest",
				"--source-uri", "vitess://user:pass@vtgate.internal:15306/commerce?grpc_port=15991",
				"--source-table", "orders",
				"--dest-uri", "bigquery://uri-here",
				"--dest-table", "vitess-asset",
				"--yes",
				"--progress", "log",
			},
		},
		{
			name: "Vitess CDC derives the vitess+cdc scheme and keeps grpc_port",
			asset: &pipeline.Asset{
				Name:       "cdc-vitess-asset",
				Connection: "bq",
				Parameters: pipeline.ParameterMap{
					"source_connection": "vitess",
					"source_table":      "orders",
					"destination":       "bigquery",
					"cdc":               "true",
				},
			},
			want: []string{
				"ingest",
				"--source-uri", "vitess+cdc://user:pass@vtgate.internal:15306/commerce?grpc_port=15991",
				"--source-table", "orders",
				"--dest-uri", "bigquery://uri-here",
				"--dest-table", "cdc-vitess-asset",
				"--yes",
				"--progress", "log",
				"--incremental-strategy", "merge",
			},
		},
		{
			name: "PlanetScale CDC derives the planetscale+cdc scheme",
			asset: &pipeline.Asset{
				Name:       "cdc-planetscale-asset",
				Connection: "bq",
				Parameters: pipeline.ParameterMap{
					"source_connection": "planetscale",
					"source_table":      "orders",
					"destination":       "bigquery",
					"cdc":               "true",
				},
			},
			want: []string{
				"ingest",
				"--source-uri", "planetscale+cdc://user:pass@aws.connect.psdb.cloud:3306/my_database",
				"--source-table", "orders",
				"--dest-uri", "bigquery://uri-here",
				"--dest-table", "cdc-planetscale-asset",
				"--yes",
				"--progress", "log",
				"--incremental-strategy", "merge",
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

func TestBasicOperator_Run_MissingConnectionError(t *testing.T) {
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
			name: "missing source connection reports config path and environment",
			connections: map[string]string{
				"bq": "bigquery://uri-here",
			},
			assetConnection:  "bq",
			sourceConnection: "missing-source",
			expectedParts: []string{
				"source connection 'missing-source' not found in config file '.bruin.yml' under environment 'default'",
			},
		},
		{
			name: "missing destination connection reports config path and environment",
			connections: map[string]string{
				"sf": "snowflake://uri-here",
			},
			assetConnection:  "missing-destination",
			sourceConnection: "sf",
			expectedParts: []string{
				"destination connection 'missing-destination' not found in config file '.bruin.yml' under environment 'default'",
			},
		},
		{
			name: "missing source connection reports secrets backend",
			connections: map[string]string{
				"bq": "bigquery://uri-here",
			},
			assetConnection:  "bq",
			sourceConnection: "missing-source",
			secretsBackend:   "vault",
			expectedParts: []string{
				"source connection 'missing-source' not found in secrets backend 'vault'",
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
					Parameters: pipeline.ParameterMap{
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

func TestResolveIngestrEngine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		params    pipeline.ParameterMap
		want      resolvedEngine
		expectErr string
	}{
		{
			name:   "unset defaults to v1 (latest)",
			params: pipeline.ParameterMap{},
			want:   resolvedEngine{family: versionFamilyV1, ingestrVersion: python.IngestrVersionV1},
		},
		{
			name:   "bare v0 pins to legacy version",
			params: pipeline.ParameterMap{"version": "v0"},
			want:   resolvedEngine{family: versionFamilyV0, ingestrVersion: python.IngestrVersionV0},
		},
		{
			name:   "bare v1 pins to latest version",
			params: pipeline.ParameterMap{"version": "v1"},
			want:   resolvedEngine{family: versionFamilyV1, ingestrVersion: python.IngestrVersionV1},
		},
		{
			name:   "v0.14.2 selects exact version in v0 family",
			params: pipeline.ParameterMap{"version": "v0.14.2"},
			want:   resolvedEngine{family: versionFamilyV0, ingestrVersion: "0.14.2"},
		},
		{
			name:   "v1.0.5 selects exact version in v1 family",
			params: pipeline.ParameterMap{"version": "v1.0.5"},
			want:   resolvedEngine{family: versionFamilyV1, ingestrVersion: "1.0.5"},
		},
		{
			name:   "future major v2 maps to v1 family with v1 pin",
			params: pipeline.ParameterMap{"version": "v2"},
			want:   resolvedEngine{family: versionFamilyV1, ingestrVersion: python.IngestrVersionV1},
		},
		{
			name:   "future major v2.0.0 keeps exact pin in v1 family",
			params: pipeline.ParameterMap{"version": "v2.0.0"},
			want:   resolvedEngine{family: versionFamilyV1, ingestrVersion: "2.0.0"},
		},
		{
			name:      "v0.14 partial is rejected",
			params:    pipeline.ParameterMap{"version": "v0.14"},
			expectErr: "invalid parameters.version",
		},
		{
			name:      "leading-zero major is rejected",
			params:    pipeline.ParameterMap{"version": "v01"},
			expectErr: "invalid parameters.version",
		},
		{
			name:      "non-prefixed version is rejected",
			params:    pipeline.ParameterMap{"version": "0.14.2"},
			expectErr: "invalid parameters.version",
		},
		{
			name:      "latest is rejected",
			params:    pipeline.ParameterMap{"version": "latest"},
			expectErr: "invalid parameters.version",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := resolveIngestrEngine(&pipeline.Asset{Parameters: tt.params})
			if tt.expectErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBasicOperator_Version(t *testing.T) {
	t.Parallel()

	mockSf := new(mockConnection)
	mockSf.On("GetIngestrURI").Return("snowflake://uri-here", nil)
	mockBq := new(mockConnection)
	mockBq.On("GetIngestrURI").Return("bigquery://uri-here", nil)

	fetcher := simpleConnectionFetcher{
		connections: map[string]*mockConnection{
			"sf": mockSf,
			"bq": mockBq,
		},
	}
	finder := new(mockFinder)

	makeOperator := func(runner ingestrRunner) *BasicOperator {
		startDate := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		endDate := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
		executionDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		return &BasicOperator{
			conn:          &fetcher,
			finder:        finder,
			runner:        runner,
			jinjaRenderer: jinja.NewRendererWithStartEndDatesAndMacros(&startDate, &endDate, &executionDate, "ingestr-test", "ingestr-test", nil, ""),
		}
	}

	makeAsset := func(extra pipeline.ParameterMap) *pipeline.Asset {
		params := pipeline.ParameterMap{
			"source_connection": "sf",
			"source_table":      "source-table",
		}
		for k, v := range extra {
			params[k] = v
		}
		return &pipeline.Asset{
			Name:       "asset-name",
			Connection: "bq",
			Parameters: params,
		}
	}

	tests := []struct {
		name        string
		params      pipeline.ParameterMap
		wantVersion string
	}{
		{name: "unset defaults to v1", params: nil, wantVersion: python.IngestrVersionV1},
		{name: "bare v0 pins legacy", params: pipeline.ParameterMap{"version": "v0"}, wantVersion: python.IngestrVersionV0},
		{name: "bare v1 pins latest", params: pipeline.ParameterMap{"version": "v1"}, wantVersion: python.IngestrVersionV1},
		{name: "exact v0.14.2 honors pin", params: pipeline.ParameterMap{"version": "v0.14.2"}, wantVersion: "0.14.2"},
		{name: "exact v1.0.5 honors pin", params: pipeline.ParameterMap{"version": "v1.0.5"}, wantVersion: "1.0.5"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := new(contextCapturingRunner)
			runner.On("RunIngestr", mock.Anything, mock.Anything, mock.Anything, repo).Return(nil)

			o := makeOperator(runner)
			ti := scheduler.AssetInstance{Pipeline: &pipeline.Pipeline{}, Asset: makeAsset(tt.params)}
			ctx := context.WithValue(t.Context(), pipeline.RunConfigFullRefresh, false)

			require.NoError(t, o.Run(ctx, &ti))
			assert.Equal(t, tt.wantVersion, runner.capturedCtx.Value(python.CtxIngestrVersion))
		})
	}

	t.Run("malformed version returns an error before runner is called", func(t *testing.T) {
		t.Parallel()

		runner := new(mockRunner)
		o := makeOperator(runner)
		ti := scheduler.AssetInstance{Pipeline: &pipeline.Pipeline{}, Asset: makeAsset(pipeline.ParameterMap{"version": "latest"})}
		ctx := context.WithValue(t.Context(), pipeline.RunConfigFullRefresh, false)

		err := o.Run(ctx, &ti)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid parameters.version")
		runner.AssertNotCalled(t, "RunIngestr", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})
}

func TestEnsureFabricEngineSupport(t *testing.T) {
	t.Parallel()

	const fabricURI = "fabric://client-id:secret@host:1433/warehouse?tenant_id=tid"

	tests := []struct {
		name    string
		engine  resolvedEngine
		uris    []string
		wantErr bool
	}{
		{name: "non-fabric uris are always allowed", engine: resolvedEngine{family: versionFamilyV0, ingestrVersion: python.IngestrVersionV0}, uris: []string{"postgres://x", "bigquery://y"}},
		{name: "fabric on default v1 is allowed", engine: resolvedEngine{family: versionFamilyV1, ingestrVersion: python.IngestrVersionV1}, uris: []string{fabricURI}},
		{name: "fabric on exact 1.0.5 is allowed", engine: resolvedEngine{family: versionFamilyV1, ingestrVersion: "1.0.5"}, uris: []string{fabricURI}},
		{name: "fabric on newer 1.1.0 is allowed", engine: resolvedEngine{family: versionFamilyV1, ingestrVersion: "1.1.0"}, uris: []string{fabricURI}},
		{name: "fabric on v0 is rejected", engine: resolvedEngine{family: versionFamilyV0, ingestrVersion: python.IngestrVersionV0}, uris: []string{fabricURI}, wantErr: true},
		{name: "fabric on 1.0.0 is rejected", engine: resolvedEngine{family: versionFamilyV1, ingestrVersion: "1.0.0"}, uris: []string{fabricURI}, wantErr: true},
		{name: "fabric as destination only is checked", engine: resolvedEngine{family: versionFamilyV1, ingestrVersion: "1.0.0"}, uris: []string{"csv:///tmp/x.csv", fabricURI}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ensureFabricEngineSupport(tt.engine, tt.uris...)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), fabricMinIngestrVersion)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
