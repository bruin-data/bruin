package python

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestValidateIngestrCDCParameters(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		params pipeline.ParameterMap
		want   string
	}{
		{"unknown", pipeline.ParameterMap{"cdc_snapshott_spool": true}, "unknown ingestr CDC parameter(s): cdc_snapshott_spool"},
		{"unknown sorted", pipeline.ParameterMap{"cdc_z": true, "cdc_a": true}, "cdc_a, cdc_z"},
		{"other sources", pipeline.ParameterMap{"cdc_sql_capture": "change_tracking", "cdc_server_id": "10", "cdc_grpc_tls": true, "cdc_max_await_time": "2s"}, ""},
		{"unrelated parameter", pipeline.ParameterMap{"custom": true}, ""},
		{"naming", pipeline.ParameterMap{"cdc": true, "cdc_table_naming": "table"}, ""},
		{"naming missing CDC", pipeline.ParameterMap{"cdc_table_naming": "table"}, "requires cdc: true"},
		{"invalid naming", pipeline.ParameterMap{"cdc": true, "cdc_table_naming": "direct"}, "schema_table or table"},
		{"structured naming", pipeline.ParameterMap{"cdc": true, "cdc_table_naming": []string{"table"}}, "schema_table or table"},
		{"discovery disabled", pipeline.ParameterMap{"cdc": true, "cdc_discover_interval": 0}, ""},
		{"discovery off", pipeline.ParameterMap{"cdc": true, "cdc_discover_interval": "off"}, ""},
		{"discovery duration", pipeline.ParameterMap{"cdc": true, "cdc_discover_interval": "2m"}, ""},
		{"negative discovery", pipeline.ParameterMap{"cdc": true, "cdc_discover_interval": "-1s"}, "nonnegative duration"},
		{"null discovery", pipeline.ParameterMap{"cdc": true, "cdc_discover_interval": nil}, "nonnegative duration"},
		{"binary disabled", pipeline.ParameterMap{"cdc": true, "cdc_binary": false}, ""},
		{"binary invalid", pipeline.ParameterMap{"cdc": true, "cdc_binary": 1}, "true or false"},
		{"exported snapshot", pipeline.ParameterMap{"postgres_snapshot": "00000003-0000001B-1"}, ""},
		{"expired-looking ID is syntactically valid", pipeline.ParameterMap{"postgres_snapshot": "00000003-0000001B-999"}, ""},
		{"invalid snapshot", pipeline.ParameterMap{"postgres_snapshot": "0/16B6C50"}, "exported PostgreSQL snapshot ID"},
		{"snapshot SQL injection", pipeline.ParameterMap{"postgres_snapshot": "00000003-0000001B-1'; SELECT 1"}, "exported PostgreSQL snapshot ID"},
		{"snapshot with CDC", pipeline.ParameterMap{"cdc": true, "postgres_snapshot": "00000003-0000001B-1"}, "ordinary PostgreSQL ingestion"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateIngestrCDCParameters(&pipeline.Asset{Parameters: tt.params})
			if tt.want != "" {
				require.ErrorContains(t, err, tt.want)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConsolidatedParameters_LoadControls(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		params  pipeline.ParameterMap
		version string
		want    []string
		wantErr string
	}{
		{name: "defaults omitted"},
		{name: "independent parallelism", params: pipeline.ParameterMap{"extract_parallelism": 2, "destination_parallelism": "8", "batch_size": 16}, want: []string{"--batch-size", "16", "--extract-parallelism", "2", "--destination-parallelism", "8"}},
		{name: "boolean false preserved", params: pipeline.ParameterMap{"no_run_id": false, "no_load_timestamp": "true"}, want: []string{"--no-load-timestamp=true", "--no-run-id=false"}},
		{name: "zero batch", params: pipeline.ParameterMap{"batch_size": 0}, wantErr: "positive integer"},
		{name: "overflow MiB", params: pipeline.ParameterMap{"batch_size": math.MaxInt64}, wantErr: "byte range"},
		{name: "fractional workers", params: pipeline.ParameterMap{"destination_parallelism": 1.5}, wantErr: "positive integer"},
		{name: "null metadata boolean", params: pipeline.ParameterMap{"no_run_id": nil}, wantErr: "true or false"},
		{name: "first write parallelism version", version: "1.1.85", params: pipeline.ParameterMap{"destination_parallelism": 1}, want: []string{"--destination-parallelism", "1"}},
		{name: "older write parallelism version", version: "1.1.84", params: pipeline.ParameterMap{"destination_parallelism": 1}, wantErr: "requires ingestr 1.1.85"},
		{name: "legacy batch size", version: IngestrVersionV0, params: pipeline.ParameterMap{"batch_size": 16}, wantErr: "requires ingestr 1.1.30"},
		{name: "metadata version", version: "1.1.30", params: pipeline.ParameterMap{"no_run_id": false}, wantErr: "requires ingestr 1.1.31"},
		{name: "batch metrics", params: pipeline.ParameterMap{"metrics_addr": "127.0.0.1:6060"}, want: []string{"--metrics-addr", "127.0.0.1:6060"}},
		{name: "canonical metrics wins", params: pipeline.ParameterMap{"metrics_addr": ":6060", "cdc_stream_metrics_addr": ":6061"}, want: []string{"--metrics-addr", ":6060"}},
		{name: "batch alias", params: pipeline.ParameterMap{"cdc": true, "cdc_stream_metrics_addr": ":6061"}, want: []string{"--metrics-addr", ":6061"}},
		{name: "old batch metrics", version: "1.1.86", params: pipeline.ParameterMap{"metrics_addr": ":6060"}, wantErr: "batch metrics require ingestr 1.1.87"},
		{name: "stream metrics", version: "1.1.86", params: pipeline.ParameterMap{"metrics_addr": ":6060", "stream": true}, want: []string{"--metrics-addr", ":6060", "--stream"}},
		{name: "structured metrics", params: pipeline.ParameterMap{"metrics_addr": []string{":6060"}}, wantErr: "nonempty listen address"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			if tt.version != "" {
				ctx = context.WithValue(ctx, CtxIngestrVersion, tt.version)
			}
			args, err := ConsolidatedParameters(ctx, &pipeline.Asset{Parameters: tt.params}, []string{"ingest"}, nil)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, append([]string{"ingest"}, tt.want...), args)
		})
	}
}

func TestConsolidatedParameters_SpoolIntervals(t *testing.T) {
	t.Parallel()
	for _, stream := range []bool{false, true} {
		asset := &pipeline.Asset{Parameters: pipeline.ParameterMap{"cdc": true, "cdc_snapshot_spool": true, "stream": stream}}
		ctx := context.WithValue(t.Context(), pipeline.RunConfigStartDate, time.Now())
		ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, time.Now().Add(time.Hour))
		args, err := ConsolidatedParameters(ctx, asset, nil, nil)
		require.NoError(t, err)
		require.NotContains(t, args, "--interval-start")
		require.NotContains(t, args, "--interval-end")
	}
}

func TestConsolidatedParameters_GenericCallers(t *testing.T) {
	t.Parallel()
	for _, assetType := range []pipeline.AssetType{pipeline.AssetTypeIngestr, pipeline.AssetTypePython, "pg.seed"} {
		t.Run(string(assetType), func(t *testing.T) {
			t.Parallel()
			asset := &pipeline.Asset{Type: assetType, Parameters: pipeline.ParameterMap{"destination_parallelism": 3, "batch_size": 8, "no_run_id": true}}
			args, err := ConsolidatedParameters(t.Context(), asset, nil, &ColumnHintOptions{})
			require.NoError(t, err)
			require.Equal(t, []string{"--batch-size", "8", "--destination-parallelism", "3", "--no-run-id=true"}, args)
		})
	}
}
