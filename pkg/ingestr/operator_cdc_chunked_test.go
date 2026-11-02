package ingestr

import (
	"context"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestBasicOperator_CDCChunkedSnapshotParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		params      pipeline.ParameterMap
		sourceURI   string
		wantURI     string
		table       string
		wantFlags   []string
		stream      bool
		fullRefresh bool
		wantErr     string
	}{
		{name: "omitted supports older engine", params: pipeline.ParameterMap{"version": "v1.1.80"}},
		{name: "opt in uses engine defaults", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true}, wantFlags: []string{"--cdc-snapshot-chunked=true"}},
		{name: "quoted boolean", params: pipeline.ParameterMap{"cdc_snapshot_chunked": "true"}, wantFlags: []string{"--cdc-snapshot-chunked=true"}},
		{name: "explicitly disabled", params: pipeline.ParameterMap{"cdc_snapshot_chunked": false}, wantFlags: []string{"--cdc-snapshot-chunked=false"}},
		{
			name: "serial tuning",
			params: pipeline.ParameterMap{
				"cdc_snapshot_chunked": true, "cdc_snapshot_execution": "serial",
				"cdc_snapshot_chunk_size": 5000, "cdc_snapshot_chunk_timeout": "45s",
				"cdc_wal_staging": "inmemory", "cdc_wal_buffer_bytes": int64(33554432),
			},
			wantFlags: []string{
				"--cdc-snapshot-chunked=true", "--cdc-snapshot-execution", "serial",
				"--cdc-snapshot-chunk-size", "5000", "--cdc-snapshot-chunk-timeout", "45s",
				"--cdc-wal-staging", "inmemory", "--cdc-wal-buffer-bytes", "33554432",
			},
		},
		{
			name: "parallel destination staging with multiple tables",
			params: pipeline.ParameterMap{
				"cdc_snapshot_chunked": true, "cdc_snapshot_execution": "parallel", "cdc_wal_staging": "destination",
			},
			table: "public.users,public.orders",
			wantFlags: []string{
				"--cdc-snapshot-chunked=true", "--cdc-snapshot-execution", "parallel", "--cdc-wal-staging", "destination",
			},
		},
		{name: "streaming", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "stream": true}, stream: true, wantFlags: []string{"--cdc-snapshot-chunked=true"}},
		{name: "deprecated stream alias", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc_mode": "stream"}, stream: true, wantFlags: []string{"--cdc-snapshot-chunked=true"}},
		{name: "finite full refresh", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true}, fullRefresh: true, wantFlags: []string{"--cdc-snapshot-chunked=true"}},
		{name: "streaming full refresh", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "stream": true}, stream: true, fullRefresh: true, wantFlags: []string{"--cdc-snapshot-chunked=true"}},
		{name: "PostgreSQL CDC URI alias", sourceURI: "postgresql+cdc://user:pass@localhost:5432/db", wantURI: "postgresql+cdc://user:pass@localhost:5432/db?slot=bruin_slot", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true}, wantFlags: []string{"--cdc-snapshot-chunked=true"}},
		{name: "first supported release", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "version": "v1.1.81"}, wantFlags: []string{"--cdc-snapshot-chunked=true"}},
		{name: "older engine", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "version": "v1.1.80"}, wantErr: "require ingestr 1.1.81 or newer"},
		{name: "legacy engine", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "version": "v0"}, wantErr: "require ingestr 1.1.81 or newer"},
		{name: "non CDC", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc": false}, wantErr: "cdc: true"},
		{name: "non PostgreSQL CDC", sourceURI: "mysql://user:pass@localhost:3306/db", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true}, wantErr: "only supported for PostgreSQL CDC"},
		{name: "destination requires explicit parallel", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc_wal_staging": "destination"}, wantErr: "parallel"},
		{name: "runtime validates numeric tuning", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc_snapshot_chunk_size": 0}, wantErr: "cdc_snapshot_chunk_size"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sourceURI := tt.sourceURI
			if sourceURI == "" {
				sourceURI = "postgresql://user:pass@localhost:5432/db"
			}
			source := new(mockConnection)
			source.On("GetIngestrURI").Return(sourceURI, nil)
			dest := new(mockConnection)
			dest.On("GetIngestrURI").Return("bigquery://project", nil)
			table := tt.table
			if table == "" {
				table = "public.users"
			}
			params := pipeline.ParameterMap{
				"source_connection": "source", "source_table": table, "cdc": true, "cdc_slot": "bruin_slot",
			}
			for key, value := range tt.params {
				params[key] = value
			}
			asset := &pipeline.Asset{Name: "public.users", Type: pipeline.AssetTypeIngestr, Connection: "dest", Parameters: params}
			runner := new(mockRunner)
			if tt.wantErr == "" {
				wantURI := tt.wantURI
				if wantURI == "" {
					wantURI = "postgres+cdc://user:pass@localhost:5432/db?slot=bruin_slot"
				}
				want := []string{
					"ingest", "--source-uri", wantURI, "--source-table", table,
					"--dest-uri", "bigquery://project", "--dest-table", "public.users", "--yes", "--progress", "log",
				}
				want = append(want, tt.wantFlags...)
				want = append(want, "--incremental-strategy", "merge")
				if tt.stream {
					want = append(want, "--stream")
				}
				chunked, _ := params.GetString("cdc_snapshot_chunked")
				if chunked != "true" {
					want = append(want, "--interval-start", "2026-11-01T00:00:00Z", "--interval-end", "2026-11-02T00:00:00Z")
				}
				if tt.fullRefresh {
					want = append(want, "--full-refresh")
				}
				runner.On("RunIngestr", mock.Anything, want, []string(nil), repo).Return(nil).Once()
			}
			op := &BasicOperator{
				conn:   &simpleConnectionFetcher{connections: map[string]*mockConnection{"source": source, "dest": dest}},
				finder: &mockFinder{}, runner: runner,
			}
			ti := &scheduler.AssetInstance{Pipeline: &pipeline.Pipeline{}, Asset: asset}
			ctx := context.WithValue(t.Context(), pipeline.RunConfigFullRefresh, tt.fullRefresh)
			ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
			ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC))
			err := op.Run(ctx, ti)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				runner.AssertNotCalled(t, "RunIngestr", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
				return
			}
			require.NoError(t, err)
			runner.AssertExpectations(t)
		})
	}
}
