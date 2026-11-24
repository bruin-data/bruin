package python

import (
	"maps"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestValidateCDCChunkedSnapshotParameters(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		parameters pipeline.ParameterMap
		material   pipeline.Materialization
		wantErr    string
	}{
		{name: "omitted options preserve non CDC assets", parameters: pipeline.ParameterMap{"cdc": false}},
		{name: "defaults", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true}},
		{name: "quoted boolean", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": "true"}},
		{name: "explicit merge", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "incremental_strategy": "merge"}},
		{name: "materialization merge", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true}, material: pipeline.Materialization{Strategy: pipeline.MaterializationStrategyMerge}},
		{name: "parallel destination", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc_snapshot_execution": "parallel", "cdc_wal_staging": "destination"}},
		{name: "parallel memory", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc_snapshot_execution": "parallel"}},
		{name: "defaults are serial", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc_wal_staging": "destination"}, wantErr: "destination requires cdc_snapshot_execution: parallel"},
		{name: "explicit serial destination", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc_snapshot_execution": "serial", "cdc_wal_staging": "destination"}, wantErr: "destination requires cdc_snapshot_execution: parallel"},
		{name: "valid tuning does not enable chunking", parameters: pipeline.ParameterMap{"cdc_wal_staging": "destination"}},
		{name: "disabled incompatible settings", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": false, "cdc_snapshot_execution": "serial", "cdc_wal_staging": "destination", "cdc_snapshot_mode": "never", "incremental_key": "id"}},
		{name: "disabled limits follow ingestr", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": "false", "cdc_snapshot_chunk_size": 0, "cdc_snapshot_chunk_timeout": "-1s", "cdc_wal_buffer_bytes": -1}},
		{name: "enabled requires CDC", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc": false}, wantErr: "require cdc: true"},
		{name: "disabled flag requires CDC", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": false, "cdc": nil}, wantErr: "require cdc: true"},
		{name: "tuning requires CDC", parameters: pipeline.ParameterMap{"cdc_snapshot_chunk_size": 100, "cdc": "false"}, wantErr: "require cdc: true"},
		{name: "never", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc_snapshot_mode": "never"}, wantErr: "cannot be combined with cdc_snapshot_mode: never"},
		{name: "external baseline", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "cdc_start_lsn": "0/1"}, wantErr: "cannot be combined with cdc_start_lsn"},
		{name: "replace", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "incremental_strategy": "replace"}, wantErr: "requires incremental_strategy: merge"},
		{name: "append", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "incremental_strategy": "append"}, wantErr: "requires incremental_strategy: merge"},
		{name: "materialization append", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true}, material: pipeline.Materialization{Strategy: pipeline.MaterializationStrategyAppend}, wantErr: "requires materialization.strategy: merge"},
		{name: "materialization key", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true}, material: pipeline.Materialization{IncrementalKey: "updated_at"}, wantErr: "materialization incremental_key or incremental_predicate"},
		{name: "materialization predicate", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true}, material: pipeline.Materialization{IncrementalPredicate: "id > 0"}, wantErr: "materialization incremental_key or incremental_predicate"},
		{name: "empty extraction settings", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true, "sql_limit": 0, "sql_exclude_columns": "", "incremental_key": "", "incremental_predicate": " \t", "interval_start": "", "extract_partition_by": ""}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			params := pipeline.ParameterMap{"cdc": true}
			maps.Copy(params, tt.parameters)
			original := maps.Clone(params)
			err := ValidateCDCChunkedSnapshotParameters(&pipeline.Asset{Parameters: params, Materialization: tt.material})
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
			require.Equal(t, original, params, "validation must not enable options or insert defaults")
		})
	}
}

func TestCDCChunkedSnapshotParameterTypes(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		key   string
		value any
		valid bool
	}{
		{name: "native true", key: "cdc_snapshot_chunked", value: true, valid: true},
		{name: "quoted false", key: "cdc_snapshot_chunked", value: "false", valid: true},
		{name: "numeric boolean", key: "cdc_snapshot_chunked", value: 1},
		{name: "noncanonical boolean", key: "cdc_snapshot_chunked", value: "TRUE"},
		{name: "serial", key: "cdc_snapshot_execution", value: "serial", valid: true},
		{name: "unknown execution", key: "cdc_snapshot_execution", value: "concurrent"},
		{name: "integer execution", key: "cdc_snapshot_execution", value: 1},
		{name: "memory staging", key: "cdc_wal_staging", value: "inmemory", valid: true},
		{name: "memory staging alias", key: "cdc_wal_staging", value: "memory", valid: true},
		{name: "unknown staging", key: "cdc_wal_staging", value: "remote"},
		{name: "boolean staging", key: "cdc_wal_staging", value: false},
		{name: "integer size", key: "cdc_snapshot_chunk_size", value: 10000, valid: true},
		{name: "quoted size", key: "cdc_snapshot_chunk_size", value: "123", valid: true},
		{name: "JSON size", key: "cdc_snapshot_chunk_size", value: float64(10000), valid: true},
		{name: "zero size", key: "cdc_snapshot_chunk_size", value: 0},
		{name: "negative size", key: "cdc_snapshot_chunk_size", value: -1},
		{name: "fractional size", key: "cdc_snapshot_chunk_size", value: 1.5},
		{name: "boolean size", key: "cdc_snapshot_chunk_size", value: true},
		{name: "decimal size string", key: "cdc_snapshot_chunk_size", value: "1.0"},
		{name: "overflow size", key: "cdc_snapshot_chunk_size", value: "9223372036854775808"},
		{name: "duration", key: "cdc_snapshot_chunk_timeout", value: "30s", valid: true},
		{name: "fractional duration", key: "cdc_snapshot_chunk_timeout", value: "1.5m", valid: true},
		{name: "zero duration", key: "cdc_snapshot_chunk_timeout", value: "0s"},
		{name: "negative duration", key: "cdc_snapshot_chunk_timeout", value: "-1s"},
		{name: "duration without unit", key: "cdc_snapshot_chunk_timeout", value: 30},
		{name: "invalid duration", key: "cdc_snapshot_chunk_timeout", value: "1 day"},
		{name: "overflow duration", key: "cdc_snapshot_chunk_timeout", value: "9223372036854775808ns"},
		{name: "64 bit buffer", key: "cdc_wal_buffer_bytes", value: int64(1 << 32), valid: true},
		{name: "maximum buffer", key: "cdc_wal_buffer_bytes", value: "9223372036854775807", valid: true},
		{name: "zero buffer", key: "cdc_wal_buffer_bytes", value: "0"},
		{name: "negative buffer", key: "cdc_wal_buffer_bytes", value: -1},
		{name: "overflow buffer", key: "cdc_wal_buffer_bytes", value: uint64(1 << 63)},
		{name: "fractional buffer", key: "cdc_wal_buffer_bytes", value: 512.5},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateCDCChunkedSnapshotParameters(&pipeline.Asset{Parameters: pipeline.ParameterMap{
				"cdc": true, "cdc_snapshot_chunked": true, tt.key: tt.value,
			}})
			if tt.valid {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "invalid "+tt.key)
			}
		})
	}
}

func TestCDCChunkedSnapshotRejectsEmptyOrStructuredOptions(t *testing.T) {
	t.Parallel()
	for _, key := range cdcChunkedSnapshotParameterNames {
		t.Run(key, func(t *testing.T) {
			t.Parallel()
			for _, value := range []any{"", " \t", nil, []string{"serial"}, map[string]string{"value": "serial"}} {
				for _, enabled := range []bool{false, true} {
					params := pipeline.ParameterMap{"cdc": true, "cdc_snapshot_chunked": enabled}
					params[key] = value
					err := ValidateCDCChunkedSnapshotParameters(&pipeline.Asset{Parameters: params})
					require.ErrorContains(t, err, "invalid "+key)
				}
			}
		})
	}
}

func TestCDCChunkedSnapshotRejectsExtractionFilters(t *testing.T) {
	t.Parallel()
	for _, key := range []string{"sql_limit", "sql_exclude_columns", "incremental_key", "incremental_predicate", "interval_start", "interval_end", "extract_partition_by"} {
		t.Run(key, func(t *testing.T) {
			t.Parallel()
			for _, mode := range []string{"cdc_snapshot_chunked", "cdc_snapshot_spool"} {
				params := pipeline.ParameterMap{"cdc": true, mode: true, key: "1"}
				err := ValidateCDCChunkedSnapshotParameters(&pipeline.Asset{Parameters: params})
				require.ErrorContains(t, err, mode+" cannot be combined with "+key)
			}
		})
	}
}

func TestHasCDCChunkedSnapshotParameters(t *testing.T) {
	t.Parallel()
	require.False(t, HasCDCChunkedSnapshotParameters(nil))
	require.NoError(t, ValidateCDCChunkedSnapshotParameters(nil))
	require.False(t, HasCDCChunkedSnapshotParameters(&pipeline.Asset{}))
	require.False(t, HasCDCChunkedSnapshotParameters(&pipeline.Asset{Parameters: pipeline.ParameterMap{"cdc": true, "cdc_snapshot_mode": "auto"}}))
	for _, key := range cdcChunkedSnapshotParameterNames {
		require.True(t, HasCDCChunkedSnapshotParameters(&pipeline.Asset{Parameters: pipeline.ParameterMap{key: nil}}), key)
	}
}
