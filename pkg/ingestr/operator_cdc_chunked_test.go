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
		{name: "spool uses engine staging default", params: pipeline.ParameterMap{"cdc_snapshot_spool": true}, wantFlags: []string{"--cdc-snapshot-spool=true"}},
		{name: "quoted spool boolean", params: pipeline.ParameterMap{"cdc_snapshot_spool": "true"}, wantFlags: []string{"--cdc-snapshot-spool=true"}},
		{name: "disabled spool", params: pipeline.ParameterMap{"cdc_snapshot_spool": false}, wantFlags: []string{"--cdc-snapshot-spool=false"}},
		{name: "spool memory", params: pipeline.ParameterMap{"cdc_snapshot_spool": true, "cdc_wal_staging": "memory"}, wantFlags: []string{"--cdc-snapshot-spool=true", "--cdc-wal-staging", "memory"}},
		{name: "spool inmemory", params: pipeline.ParameterMap{"cdc_snapshot_spool": true, "cdc_wal_staging": "inmemory"}, wantFlags: []string{"--cdc-snapshot-spool=true", "--cdc-wal-staging", "inmemory"}},
		{name: "spool destination", params: pipeline.ParameterMap{"cdc_snapshot_spool": true, "cdc_wal_staging": "destination"}, wantFlags: []string{"--cdc-snapshot-spool=true", "--cdc-wal-staging", "destination"}},
		{
			name: "spool persistent disk and buffer",
			params: pipeline.ParameterMap{
				"cdc_snapshot_spool": true, "cdc_wal_staging": "disk", "cdc_wal_spool_dir": "/persistent/spool with spaces", "cdc_wal_buffer_bytes": int64(1 << 32),
			},
			wantFlags: []string{"--cdc-snapshot-spool=true", "--cdc-wal-staging", "disk", "--cdc-wal-spool-dir", "/persistent/spool with spaces", "--cdc-wal-buffer-bytes", "4294967296"},
		},
		{
			name: "spool preserves inactive chunk tuning",
			params: pipeline.ParameterMap{
				"cdc_snapshot_spool": true, "cdc_snapshot_chunked": false, "cdc_snapshot_chunk_size": 0, "cdc_snapshot_chunk_timeout": "-1s",
			},
			wantFlags: []string{"--cdc-snapshot-chunked=false", "--cdc-snapshot-spool=true", "--cdc-snapshot-chunk-size", "0", "--cdc-snapshot-chunk-timeout", "-1s"},
		},
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
		{name: "spool streaming", params: pipeline.ParameterMap{"cdc_snapshot_spool": true, "stream": true}, stream: true, wantFlags: []string{"--cdc-snapshot-spool=true"}},
		{name: "spool deprecated stream alias", params: pipeline.ParameterMap{"cdc_snapshot_spool": true, "cdc_mode": "stream"}, stream: true, wantFlags: []string{"--cdc-snapshot-spool=true"}},
		{name: "spool finite full refresh", params: pipeline.ParameterMap{"cdc_snapshot_spool": true}, fullRefresh: true, wantFlags: []string{"--cdc-snapshot-spool=true"}},
		{name: "spool streaming full refresh", params: pipeline.ParameterMap{"cdc_snapshot_spool": true, "stream": true}, stream: true, fullRefresh: true, wantFlags: []string{"--cdc-snapshot-spool=true"}},
		{name: "PostgreSQL CDC URI alias", sourceURI: "postgresql+cdc://user:pass@localhost:5432/db", wantURI: "postgresql+cdc://user:pass@localhost:5432/db?slot=bruin_slot", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true}, wantFlags: []string{"--cdc-snapshot-chunked=true"}},
		{name: "first supported release", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "version": "v1.1.81"}, wantFlags: []string{"--cdc-snapshot-chunked=true"}},
		{name: "first spool release", params: pipeline.ParameterMap{"cdc_snapshot_spool": true, "version": "v1.1.86"}, wantFlags: []string{"--cdc-snapshot-spool=true"}},
		{name: "older spool engine", params: pipeline.ParameterMap{"cdc_snapshot_spool": true, "version": "v1.1.85"}, wantErr: "require ingestr 1.1.86 or newer"},
		{name: "disabled spool on old engine", params: pipeline.ParameterMap{"cdc_snapshot_spool": false, "version": "v1.1.85"}, wantErr: "require ingestr 1.1.86 or newer"},
		{name: "memory alias on old engine", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc_wal_staging": "memory", "version": "v1.1.85"}, wantErr: "require ingestr 1.1.86 or newer"},
		{name: "spool legacy engine", params: pipeline.ParameterMap{"cdc_snapshot_spool": true, "version": "v0"}, wantErr: "require ingestr 1.1.86 or newer"},
		{name: "older engine", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "version": "v1.1.80"}, wantErr: "require ingestr 1.1.81 or newer"},
		{name: "legacy engine", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "version": "v0"}, wantErr: "require ingestr 1.1.81 or newer"},
		{name: "non CDC", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc": false}, wantErr: "cdc: true"},
		{name: "non PostgreSQL CDC", sourceURI: "mysql://user:pass@localhost:3306/db", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true}, wantErr: "only supported for PostgreSQL CDC"},
		{name: "destination requires explicit parallel", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc_wal_staging": "destination"}, wantErr: "parallel"},
		{name: "runtime validates numeric tuning", params: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc_snapshot_chunk_size": 0}, wantErr: "cdc_snapshot_chunk_size"},
		{name: "spool non PostgreSQL CDC", sourceURI: "mysql://user:pass@localhost:3306/db", params: pipeline.ParameterMap{"cdc_snapshot_spool": true}, wantErr: "only supported for PostgreSQL CDC"},
		{name: "spool chunked conflict", params: pipeline.ParameterMap{"cdc_snapshot_spool": true, "cdc_snapshot_chunked": true}, wantErr: "cdc_snapshot_spool cannot be combined with cdc_snapshot_chunked"},
		{name: "spool zero buffer", params: pipeline.ParameterMap{"cdc_snapshot_spool": true, "cdc_wal_buffer_bytes": 0}, wantErr: "cdc_wal_buffer_bytes"},
		{name: "spool disk without directory", params: pipeline.ParameterMap{"cdc_snapshot_spool": true, "cdc_wal_staging": "disk"}, wantErr: "cdc_wal_spool_dir is required"},
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
				spool, _ := params.GetString("cdc_snapshot_spool")
				if chunked != "true" && spool != "true" {
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
