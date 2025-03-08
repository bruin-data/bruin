package ingestr

import (
	"context"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/git"
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
	connections map[string]*mockConnection
}

var repo = &git.Repo{
	Path: "/the/repo",
}

type mockFinder struct{}

func (m *mockFinder) Repo(path string) (*git.Repo, error) {
	return repo, nil
}

func (s simpleConnectionFetcher) GetConnection(name string) (interface{}, error) {
	return s.connections[name], nil
}

type mockRunner struct {
	mock.Mock
}

func (m *mockRunner) RunIngestr(ctx context.Context, args, extraPackages []string, repo *git.Repo) error {
	return m.Called(ctx, args, extraPackages, repo).Error(0)
}

func TestBasicOperator_ConvertTaskInstanceToIngestrCommand(t *testing.T) {
	t.Parallel()

	mockBq := new(mockConnection)
	mockBq.On("GetIngestrURI").Return("bigquery://uri-here", nil)
	mockSf := new(mockConnection)
	mockSf.On("GetIngestrURI").Return("snowflake://uri-here", nil)
	mockDuck := new(mockConnection)
	mockDuck.On("GetIngestrURI").Return("duckdb:////some/path", nil)

	fetcher := simpleConnectionFetcher{
		connections: map[string]*mockConnection{
			"bq":   mockBq,
			"sf":   mockSf,
			"duck": mockDuck,
		},
	}

	finder := new(mockFinder)

	tests := []struct {
		name          string
		asset         *pipeline.Asset
		fullRefresh   bool
		startTime     *time.Time
		endTime       *time.Time
		want          []string
		extraPackages []string
	}{
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
				},
			},
			want: []string{"ingest", "--source-uri", "duckdb:////some/path", "--source-table", "source-table", "--dest-uri", "bigquery://uri-here", "--dest-table", "asset-name", "--yes", "--progress", "log"},
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

			o := &BasicOperator{
				conn:   &fetcher,
				finder: finder,
				runner: runner,
			}

			ti := scheduler.AssetInstance{
				Pipeline: &pipeline.Pipeline{},
				Asset:    tt.asset,
			}

			ctx := context.WithValue(context.Background(), pipeline.RunConfigFullRefresh, tt.fullRefresh)

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
		conn:   &fetcher,
		finder: finder,
		runner: runner,
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

	ctx := context.WithValue(context.Background(), pipeline.RunConfigFullRefresh, false)
	ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC))

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

	fetcher := simpleConnectionFetcher{
		connections: map[string]*mockConnection{
			"bq":   mockBq,
			"sf":   mockSf,
			"duck": mockDuck,
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

			ctx := context.WithValue(context.Background(), pipeline.RunConfigFullRefresh, tt.fullRefresh)

			err := o.Run(ctx, &ti)
			require.NoError(t, err)
		})
	}
}
