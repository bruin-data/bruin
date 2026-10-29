package ingestr

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestBasicOperator_CDCSnapshotParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		params      pipeline.ParameterMap
		sourceURI   string
		wantURI     string
		wantFlags   []string
		stream      bool
		fullRefresh bool
		wantErr     string
	}{
		{name: "omitted inherits saved policy"},
		{name: "omitted supports older engine", params: pipeline.ParameterMap{"version": "v1.1.77"}},
		{name: "auto batch", params: pipeline.ParameterMap{"cdc_snapshot_mode": "auto"}, wantFlags: []string{"--cdc-snapshot-mode", "auto"}},
		{name: "auto streaming", params: pipeline.ParameterMap{"cdc_snapshot_mode": "auto", "stream": true}, wantFlags: []string{"--cdc-snapshot-mode", "auto"}, stream: true},
		{name: "never resumes without bootstrap LSN", params: pipeline.ParameterMap{"cdc_snapshot_mode": "never"}, wantFlags: []string{"--cdc-snapshot-mode", "never"}},
		{name: "bootstrap batch", params: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "cdc_start_lsn": "0/16B6C50"}, wantFlags: []string{"--cdc-snapshot-mode", "never", "--cdc-start-lsn", "0/16B6C50"}},
		{name: "bootstrap streaming with hard deletes", params: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "cdc_start_lsn": "0/16B6C50", "cdc_delete_behavior": "hard", "stream": true}, wantFlags: []string{"--cdc-delete-behavior", "hard", "--cdc-snapshot-mode", "never", "--cdc-start-lsn", "0/16B6C50"}, stream: true},
		{name: "deprecated stream alias", params: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "cdc_mode": "stream"}, wantFlags: []string{"--cdc-snapshot-mode", "never"}, stream: true},
		{name: "PostgreSQL CDC URI alias", sourceURI: "postgresql+cdc://user:pass@localhost:5432/db", wantURI: "postgresql+cdc://user:pass@localhost:5432/db?slot=bruin_slot", params: pipeline.ParameterMap{"cdc_snapshot_mode": "never"}, wantFlags: []string{"--cdc-snapshot-mode", "never"}},
		{name: "explicit supported version", params: pipeline.ParameterMap{"cdc_snapshot_mode": "auto", "version": "v1.1.78"}, wantFlags: []string{"--cdc-snapshot-mode", "auto"}},
		{name: "auto full refresh", params: pipeline.ParameterMap{"cdc_snapshot_mode": "auto"}, wantFlags: []string{"--cdc-snapshot-mode", "auto"}, fullRefresh: true},
		{name: "never rejects run full refresh", params: pipeline.ParameterMap{"cdc_snapshot_mode": "never"}, fullRefresh: true, wantErr: "full refresh"},
		{name: "invalid mode", params: pipeline.ParameterMap{"cdc_snapshot_mode": "initial"}, wantErr: "cdc_snapshot_mode"},
		{name: "non CDC", params: pipeline.ParameterMap{"cdc_snapshot_mode": "auto", "cdc": false}, wantErr: "cdc: true"},
		{name: "non PostgreSQL CDC", sourceURI: "mysql://user:pass@localhost:3306/db", params: pipeline.ParameterMap{"cdc_snapshot_mode": "auto"}, wantErr: "only supported for PostgreSQL CDC"},
		{name: "older engine", params: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "version": "v1.1.77"}, wantErr: "require ingestr 1.1.78 or newer"},
		{name: "legacy engine", params: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "version": "v0"}, wantErr: "require ingestr 1.1.78 or newer"},
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
			params := pipeline.ParameterMap{
				"source_connection": "source",
				"source_table":      "public.users",
				"cdc":               true,
				"cdc_slot":          "bruin_slot",
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
					"ingest", "--source-uri", wantURI, "--source-table", "public.users",
					"--dest-uri", "bigquery://project", "--dest-table", "public.users",
					"--yes", "--progress", "log",
				}
				want = append(want, tt.wantFlags...)
				want = append(want, "--incremental-strategy", "merge")
				if tt.stream {
					want = append(want, "--stream")
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
