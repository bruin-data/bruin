package lint

import (
	"maps"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestEnsureIngestrCDCParityParameters(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		params pipeline.ParameterMap
		want   string
	}{
		{"unknown option", pipeline.ParameterMap{"cdc_spool": true}, "unknown ingestr CDC parameter(s): cdc_spool"},
		{"disk spooling", pipeline.ParameterMap{"cdc_snapshot_spool": true, "cdc_wal_staging": "disk", "cdc_wal_spool_dir": "/persistent/spool"}, ""},
		{"disk missing directory", pipeline.ParameterMap{"cdc_snapshot_spool": true, "cdc_wal_staging": "disk"}, "cdc_wal_spool_dir is required"},
		{"spooling old engine", pipeline.ParameterMap{"cdc_snapshot_spool": true, "version": "v1.1.85"}, "require ingestr 1.1.86"},
		{"batch metrics old engine", pipeline.ParameterMap{"metrics_addr": ":6060", "version": "v1.1.86"}, "batch metrics require ingestr 1.1.87"},
		{"stream metrics old engine", pipeline.ParameterMap{"metrics_addr": ":6060", "version": "v1.1.86", "cdc_mode": "stream"}, ""},
		{"zero write workers", pipeline.ParameterMap{"destination_parallelism": 0}, "positive integer"},
		{"quoted discovery zero", pipeline.ParameterMap{"cdc_discover_interval": "0"}, ""},
		{"invalid discovery", pipeline.ParameterMap{"cdc_discover_interval": "yesterday"}, "nonnegative duration"},
		{"binary old engine", pipeline.ParameterMap{"cdc_binary": true, "version": "v1.0.71"}, "requires ingestr 1.0.72"},
		{"table naming", pipeline.ParameterMap{"cdc_table_naming": "table"}, ""},
		{"snapshot mode conflict", pipeline.ParameterMap{"cdc_snapshot_spool": true, "cdc_snapshot_chunked": true}, "cannot be combined"},
		{"other source controls", pipeline.ParameterMap{"cdc_sql_capture": "change_tracking", "cdc_server_id": "42", "cdc_grpc_tls": "true", "cdc_max_await_time": "1s"}, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			params := pipeline.ParameterMap{"source_connection": "pg", "source_table": "public.users", "destination": "postgres", "cdc": true}
			maps.Copy(params, tt.params)
			asset := &pipeline.Asset{Type: pipeline.AssetTypeIngestr, Parameters: params}
			issues, err := EnsureIngestrAssetIsValidForASingleAsset(t.Context(), &pipeline.Pipeline{}, asset)
			require.NoError(t, err)
			if tt.want == "" {
				require.Empty(t, issues)
				return
			}
			var descriptions []string
			for _, issue := range issues {
				descriptions = append(descriptions, issue.Description)
			}
			require.Contains(t, strings.Join(descriptions, "\n"), tt.want)
		})
	}
}
