package lint

import (
	"maps"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestEnsureIngestrAssetCDCChunkedSnapshotParameters(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		parameters pipeline.ParameterMap
		material   pipeline.Materialization
		wantIssue  string
	}{
		{name: "existing CDC asset"},
		{name: "default serial", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true}},
		{name: "finite parallel destination", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc_snapshot_execution": "parallel", "cdc_wal_staging": "destination"}},
		{name: "streaming serial", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "stream": true}},
		{name: "requires CDC", parameters: pipeline.ParameterMap{"cdc": false, "cdc_snapshot_chunked": true}, wantIssue: "CDC snapshot parameters require cdc: true"},
		{name: "invalid boolean", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": "yes"}, wantIssue: "invalid cdc_snapshot_chunked: must be true or false"},
		{name: "invalid execution while disabled", parameters: pipeline.ParameterMap{"cdc_snapshot_execution": nil}, wantIssue: "invalid cdc_snapshot_execution: must be \"serial\" or \"parallel\""},
		{name: "zero size", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc_snapshot_chunk_size": 0}, wantIssue: "invalid cdc_snapshot_chunk_size: must be positive when cdc_snapshot_chunked is true"},
		{name: "default serial destination", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc_wal_staging": "destination"}, wantIssue: "cdc_wal_staging: destination requires cdc_snapshot_execution: parallel; serial execution retains pending changes in source WAL"},
		{name: "never policy", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc_snapshot_mode": "never", "cdc_slot": "existing_slot"}, wantIssue: "cdc_snapshot_chunked cannot be combined with cdc_snapshot_mode: never"},
		{name: "incremental key", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "incremental_key": "id"}, wantIssue: "cdc_snapshot_chunked cannot be combined with incremental_key"},
		{name: "materialization key", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true}, material: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyMerge, IncrementalKey: "id"}, wantIssue: "cdc_snapshot_chunked cannot be combined with materialization incremental_key or incremental_predicate"},
		{name: "replace strategy", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "incremental_strategy": "replace"}, wantIssue: "cdc_snapshot_chunked requires incremental_strategy: merge"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			params := pipeline.ParameterMap{
				"cdc": true, "source_connection": "postgres", "source_table": "public.users", "destination": "bigquery",
			}
			maps.Copy(params, tt.parameters)
			asset := &pipeline.Asset{Type: pipeline.AssetTypeIngestr, Parameters: params, Materialization: tt.material}
			issues, err := EnsureIngestrAssetIsValidForASingleAsset(t.Context(), &pipeline.Pipeline{Assets: []*pipeline.Asset{asset}}, asset)
			require.NoError(t, err)
			if tt.wantIssue == "" {
				require.Empty(t, issues)
				return
			}
			descriptions := make([]string, len(issues))
			for i, issue := range issues {
				require.Same(t, asset, issue.Task)
				descriptions[i] = issue.Description
			}
			require.Contains(t, descriptions, tt.wantIssue)
		})
	}
}
