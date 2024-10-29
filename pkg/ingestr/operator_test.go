package ingestr

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
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
	connections map[string]*mockConnection
}

func (s simpleConnectionFetcher) GetConnection(name string) (interface{}, error) {
	return s.connections[name], nil
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

	tests := []struct {
		name        string
		asset       *pipeline.Asset
		fullRefresh bool
		want        []string
		wantMounts  []mount.Mount
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
			want: []string{"ingest", "--source-uri", "duckdb:////tmp/source.db", "--source-table", "source-table", "--dest-uri", "bigquery://uri-here", "--dest-table", "asset-name", "--yes", "--progress", "log"},
			wantMounts: []mount.Mount{
				{
					Type:   mount.TypeBind,
					Source: "/some/path",
					Target: "/tmp/source.db",
				},
			},
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
			want: []string{"ingest", "--source-uri", "snowflake://uri-here", "--source-table", "source-table", "--dest-uri", "duckdb:////tmp/dest.db", "--dest-table", "asset-name", "--yes", "--progress", "log"},
			wantMounts: []mount.Mount{
				{
					Type:   mount.TypeBind,
					Source: "/some/path",
					Target: "/tmp/dest.db",
				},
			},
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
			baseConfig := container.Config{
				Image:        DockerImage,
				AttachStdout: false,
				AttachStderr: true,
				Tty:          true,
				Env:          []string{},
			}
			baseHostConfig := container.HostConfig{
				NetworkMode: "host",
			}

			t.Parallel()

			o := &BasicOperator{
				conn: &fetcher,
			}

			ti := scheduler.AssetInstance{
				Pipeline: &pipeline.Pipeline{},
				Asset:    tt.asset,
			}

			ctx := context.WithValue(context.Background(), pipeline.RunConfigFullRefresh, tt.fullRefresh)

			got, gotHost, err := o.ConvertTaskInstanceToContainerConfig(ctx, &ti)
			baseConfig.Cmd = tt.want
			baseHostConfig.Mounts = tt.wantMounts

			require.NoError(t, err)
			assert.Equal(t, baseConfig, *got)
			assert.Equal(t, baseHostConfig, *gotHost)
		})
	}
}
