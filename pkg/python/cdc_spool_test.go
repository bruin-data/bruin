package python

import (
	"maps"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestValidateCDCSnapshotSpoolParameters(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		parameters pipeline.ParameterMap
		material   pipeline.Materialization
		wantErr    string
	}{
		{name: "defaults stay omitted"},
		{name: "quoted boolean", parameters: pipeline.ParameterMap{"cdc_snapshot_spool": "true"}},
		{name: "destination", parameters: pipeline.ParameterMap{"cdc_wal_staging": "destination"}},
		{name: "memory", parameters: pipeline.ParameterMap{"cdc_wal_staging": "memory"}},
		{name: "inmemory", parameters: pipeline.ParameterMap{"cdc_wal_staging": "inmemory"}},
		{name: "disk", parameters: pipeline.ParameterMap{"cdc_wal_staging": "disk", "cdc_wal_spool_dir": "/persistent/spool"}},
		{name: "buffer", parameters: pipeline.ParameterMap{"cdc_wal_buffer_bytes": int64(1 << 32)}},
		{name: "explicit merge", parameters: pipeline.ParameterMap{"incremental_strategy": "merge"}},
		{name: "materialization merge", material: pipeline.Materialization{Strategy: pipeline.MaterializationStrategyMerge}},
		{name: "inactive chunk tuning", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": false, "cdc_snapshot_chunk_size": 0, "cdc_snapshot_chunk_timeout": "-1s"}},
		{name: "inactive execution setting", parameters: pipeline.ParameterMap{"cdc_snapshot_execution": "serial", "cdc_wal_staging": "destination"}},
		{name: "disabled preserves tuning", parameters: pipeline.ParameterMap{"cdc_snapshot_spool": false, "cdc_wal_buffer_bytes": -1, "cdc_snapshot_mode": "never", "incremental_key": "id"}},
		{name: "disabled mode still requires CDC", parameters: pipeline.ParameterMap{"cdc": false, "cdc_snapshot_spool": false}, wantErr: "require cdc: true"},
		{name: "chunking conflict", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true}, wantErr: "cdc_snapshot_spool cannot be combined with cdc_snapshot_chunked"},
		{name: "disk requires directory", parameters: pipeline.ParameterMap{"cdc_wal_staging": "disk"}, wantErr: "cdc_wal_spool_dir is required"},
		{name: "disk requires spool", parameters: pipeline.ParameterMap{"cdc_snapshot_spool": false, "cdc_wal_staging": "disk"}, wantErr: "disk requires cdc_snapshot_spool: true"},
		{name: "chunked cannot use disk", parameters: pipeline.ParameterMap{"cdc_snapshot_spool": false, "cdc_snapshot_chunked": true, "cdc_wal_staging": "disk"}, wantErr: "disk requires cdc_snapshot_spool: true"},
		{name: "directory requires disk", parameters: pipeline.ParameterMap{"cdc_wal_spool_dir": "/persistent/spool"}, wantErr: "cdc_wal_spool_dir requires cdc_snapshot_spool: true and cdc_wal_staging: disk"},
		{name: "unused directory", parameters: pipeline.ParameterMap{"cdc_snapshot_spool": false, "cdc_wal_spool_dir": "/persistent/spool"}, wantErr: "cdc_wal_spool_dir requires cdc_snapshot_spool: true and cdc_wal_staging: disk"},
		{name: "never", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never"}, wantErr: "cdc_snapshot_spool cannot be combined with cdc_snapshot_mode: never"},
		{name: "external baseline", parameters: pipeline.ParameterMap{"cdc_start_lsn": "0/100"}, wantErr: "cdc_snapshot_spool cannot be combined with cdc_start_lsn"},
		{name: "replace", parameters: pipeline.ParameterMap{"incremental_strategy": "replace"}, wantErr: "cdc_snapshot_spool requires incremental_strategy: merge"},
		{name: "append", parameters: pipeline.ParameterMap{"incremental_strategy": "append"}, wantErr: "cdc_snapshot_spool requires incremental_strategy: merge"},
		{name: "materialization append", material: pipeline.Materialization{Strategy: pipeline.MaterializationStrategyAppend}, wantErr: "cdc_snapshot_spool requires materialization.strategy: merge"},
		{name: "materialization key", material: pipeline.Materialization{IncrementalKey: "updated_at"}, wantErr: "cdc_snapshot_spool cannot be combined with materialization incremental_key or incremental_predicate"},
		{name: "materialization predicate", material: pipeline.Materialization{IncrementalPredicate: "id > 0"}, wantErr: "cdc_snapshot_spool cannot be combined with materialization incremental_key or incremental_predicate"},
		{name: "empty extraction settings", parameters: pipeline.ParameterMap{"sql_limit": 0, "sql_exclude_columns": "", "incremental_key": "", "incremental_predicate": " \t", "interval_start": "", "interval_end": "", "extract_partition_by": ""}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			params := pipeline.ParameterMap{"cdc": true, "cdc_snapshot_spool": true}
			maps.Copy(params, tt.parameters)
			original := maps.Clone(params)
			err := ValidateCDCChunkedSnapshotParameters(&pipeline.Asset{Parameters: params, Materialization: tt.material})
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
			require.Equal(t, original, params, "validation must not insert defaults or alter disabled options")
		})
	}
}

func TestCDCSnapshotSpoolParameterTypes(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		key   string
		value any
	}{
		{name: "numeric boolean", key: "cdc_snapshot_spool", value: 1},
		{name: "noncanonical boolean", key: "cdc_snapshot_spool", value: "TRUE"},
		{name: "null boolean", key: "cdc_snapshot_spool", value: nil},
		{name: "zero buffer", key: "cdc_wal_buffer_bytes", value: 0},
		{name: "negative buffer", key: "cdc_wal_buffer_bytes", value: -1},
		{name: "fractional buffer", key: "cdc_wal_buffer_bytes", value: 1.5},
		{name: "overflow buffer", key: "cdc_wal_buffer_bytes", value: "9223372036854775808"},
		{name: "empty directory", key: "cdc_wal_spool_dir", value: ""},
		{name: "blank directory", key: "cdc_wal_spool_dir", value: " \t"},
		{name: "numeric directory", key: "cdc_wal_spool_dir", value: 1},
		{name: "boolean directory", key: "cdc_wal_spool_dir", value: true},
		{name: "null directory", key: "cdc_wal_spool_dir", value: nil},
		{name: "list directory", key: "cdc_wal_spool_dir", value: []string{"/persistent/spool"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			params := pipeline.ParameterMap{"cdc": true, "cdc_snapshot_spool": true, tt.key: tt.value}
			if tt.key == "cdc_wal_spool_dir" {
				params["cdc_wal_staging"] = "disk"
			}
			require.ErrorContains(t, ValidateCDCChunkedSnapshotParameters(&pipeline.Asset{Parameters: params}), "invalid "+tt.key)
		})
	}
}

func TestIsCDCFullTableSnapshot(t *testing.T) {
	t.Parallel()
	require.False(t, IsCDCFullTableSnapshot(nil))
	require.False(t, IsCDCFullTableSnapshot(&pipeline.Asset{}))
	for _, mode := range []string{"cdc_snapshot_chunked", "cdc_snapshot_spool"} {
		for _, value := range []any{true, "true"} {
			require.True(t, IsCDCFullTableSnapshot(&pipeline.Asset{Parameters: pipeline.ParameterMap{"cdc": true, mode: value}}))
		}
		for _, value := range []any{false, "false", nil} {
			require.False(t, IsCDCFullTableSnapshot(&pipeline.Asset{Parameters: pipeline.ParameterMap{"cdc": true, mode: value}}))
		}
		require.False(t, IsCDCFullTableSnapshot(&pipeline.Asset{Parameters: pipeline.ParameterMap{"cdc": false, mode: true}}))
	}
	require.False(t, IsCDCFullTableSnapshot(&pipeline.Asset{Parameters: pipeline.ParameterMap{"cdc": true, "cdc_wal_staging": "destination"}}))
}

func TestValidateCDCSnapshotEngineVersion(t *testing.T) {
	t.Parallel()
	require.NoError(t, ValidateCDCSnapshotEngineVersion(nil, "0.15.1"))
	for _, tt := range []struct {
		name       string
		parameters pipeline.ParameterMap
		version    string
		minimum    string
	}{
		{name: "absent", version: "0.15.1"},
		{name: "existing chunk flag", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true}, version: "1.1.81"},
		{name: "unsupported chunk flag", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": false}, version: "1.1.80", minimum: "1.1.81"},
		{name: "existing inmemory staging", parameters: pipeline.ParameterMap{"cdc_wal_staging": "inmemory"}, version: "1.1.81"},
		{name: "existing destination staging", parameters: pipeline.ParameterMap{"cdc_wal_staging": "destination"}, version: "1.1.81"},
		{name: "spool flag", parameters: pipeline.ParameterMap{"cdc_snapshot_spool": true}, version: "1.1.86"},
		{name: "unsupported spool flag", parameters: pipeline.ParameterMap{"cdc_snapshot_spool": true}, version: "1.1.85", minimum: "1.1.86"},
		{name: "unsupported disabled spool flag", parameters: pipeline.ParameterMap{"cdc_snapshot_spool": false}, version: "1.1.85", minimum: "1.1.86"},
		{name: "memory alias", parameters: pipeline.ParameterMap{"cdc_wal_staging": "memory"}, version: "1.1.86"},
		{name: "unsupported memory alias", parameters: pipeline.ParameterMap{"cdc_wal_staging": "memory"}, version: "1.1.85", minimum: "1.1.86"},
		{name: "disk staging", parameters: pipeline.ParameterMap{"cdc_wal_staging": "disk"}, version: "1.1.86"},
		{name: "unsupported disk staging", parameters: pipeline.ParameterMap{"cdc_wal_staging": "disk"}, version: "1.1.85", minimum: "1.1.86"},
		{name: "spool directory", parameters: pipeline.ParameterMap{"cdc_wal_spool_dir": "/persistent/spool"}, version: "1.1.86"},
		{name: "unsupported spool directory", parameters: pipeline.ParameterMap{"cdc_wal_spool_dir": "/persistent/spool"}, version: "1.1.85", minimum: "1.1.86"},
		{name: "legacy engine", parameters: pipeline.ParameterMap{"cdc_snapshot_spool": true}, version: "0.15.1", minimum: "1.1.86"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateCDCSnapshotEngineVersion(&pipeline.Asset{Parameters: tt.parameters}, tt.version)
			if tt.minimum == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, "require ingestr "+tt.minimum+" or newer")
			require.ErrorContains(t, err, "parameters.version resolved to "+tt.version)
		})
	}
}
