package ingestr

import (
	"context"
	"testing"

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

func (m *mockRunner) RunIngestr(ctx context.Context, args []string, repo *git.Repo) error {
	return m.Called(ctx, args, repo).Error(0)
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
		name        string
		asset       *pipeline.Asset
		fullRefresh bool
		want        []string
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
				"--incremental-strategy", "merge",
				"--incremental-key", "updated_at",
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
				"--incremental-strategy", "merge",
				"--incremental-key", "updated_at",
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
				"--incremental-strategy", "merge",
				"--incremental-key", "updated_at",
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
				"--incremental-strategy", "merge",
				"--incremental-key", "updated_at",
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
			runner.On("RunIngestr", mock.Anything, tt.want, repo).Return(nil)

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
