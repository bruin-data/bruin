package python

import (
	"maps"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestValidateCDCSnapshotParameters(t *testing.T) {
	t.Parallel()

	const (
		requiresCDC   = "cdc_snapshot_mode and cdc_start_lsn require cdc: true"
		invalidMode   = "invalid cdc_snapshot_mode: must be \"auto\" or \"never\""
		requiresNever = "cdc_start_lsn requires cdc_snapshot_mode: never"
		requiresSlot  = "cdc_snapshot_mode: never requires a nonempty cdc_slot"
		requiresTable = "cdc_snapshot_mode: never requires explicit, nonempty source_table names without wildcards"
		fullRefresh   = "cdc_snapshot_mode: never cannot be used with full refresh"
	)
	tests := []struct {
		name              string
		parameters        pipeline.ParameterMap
		runFullRefresh    bool
		refreshRestricted bool
		wantErr           string
	}{
		{name: "omitted options preserve saved policy", runFullRefresh: true},
		{name: "omitted options without CDC", parameters: pipeline.ParameterMap{"cdc": false}},
		{name: "automatic snapshots", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "auto"}},
		{name: "automatic snapshots allow refresh", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "auto"}, runFullRefresh: true},
		{name: "never snapshots without start LSN", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never"}},
		{name: "never snapshots in stream", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "stream": true}},
		{name: "never snapshots with multiple tables", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "source_table": "public.users, public.orders"}},
		{name: "bootstrap boundary", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "cdc_start_lsn": "0/16B6C50"}},
		{name: "mode requires CDC", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "auto", "cdc": false}, wantErr: requiresCDC},
		{name: "LSN requires CDC", parameters: pipeline.ParameterMap{"cdc_start_lsn": "0/16B6C50", "cdc": nil}, wantErr: requiresCDC},
		{name: "invalid mode", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "always"}, wantErr: invalidMode},
		{name: "case sensitive mode", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "NEVER"}, wantErr: invalidMode},
		{name: "empty mode", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": ""}, wantErr: invalidMode},
		{name: "null mode", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": nil}, wantErr: invalidMode},
		{name: "boolean mode", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": true}, wantErr: invalidMode},
		{name: "numeric mode", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": 1}, wantErr: invalidMode},
		{name: "list mode", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": []string{"never"}}, wantErr: invalidMode},
		{name: "map mode", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": map[string]string{"mode": "never"}}, wantErr: invalidMode},
		{name: "LSN without mode", parameters: pipeline.ParameterMap{"cdc_start_lsn": "0/16B6C50"}, wantErr: requiresNever},
		{name: "LSN with automatic snapshots", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "auto", "cdc_start_lsn": "0/16B6C50"}, wantErr: requiresNever},
		{name: "empty slot", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "cdc_slot": ""}, wantErr: requiresSlot},
		{name: "null slot", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "cdc_slot": nil}, wantErr: requiresSlot},
		{name: "blank slot", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "cdc_slot": " \t"}, wantErr: requiresSlot},
		{name: "empty table", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "source_table": ""}, wantErr: requiresTable},
		{name: "null table", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "source_table": nil}, wantErr: requiresTable},
		{name: "blank table", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "source_table": " \t"}, wantErr: requiresTable},
		{name: "wildcard table", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "source_table": "*"}, wantErr: requiresTable},
		{name: "schema wildcard table", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "source_table": "public.*"}, wantErr: requiresTable},
		{name: "wildcard in table list", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "source_table": "public.users,*"}, wantErr: requiresTable},
		{name: "empty entry in table list", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "source_table": "public.users, ,public.orders"}, wantErr: requiresTable},
		{name: "trailing empty table", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "source_table": "public.users,"}, wantErr: requiresTable},
		{name: "leading empty table", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "source_table": ",public.users"}, wantErr: requiresTable},
		{name: "global full refresh", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never"}, runFullRefresh: true, wantErr: fullRefresh},
		{name: "asset full refresh", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "full_refresh": true}, wantErr: fullRefresh},
		{name: "global refresh overrides asset false", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "full_refresh": false}, runFullRefresh: true, wantErr: fullRefresh},
		{name: "restricted global refresh", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never"}, runFullRefresh: true, refreshRestricted: true},
		{name: "restricted asset refresh", parameters: pipeline.ParameterMap{"cdc_snapshot_mode": "never", "full_refresh": true}, refreshRestricted: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parameters := pipeline.ParameterMap{
				"cdc":          true,
				"cdc_slot":     "bruin_slot",
				"source_table": "public.users",
			}
			maps.Copy(parameters, tt.parameters)
			asset := &pipeline.Asset{Parameters: parameters, RefreshRestricted: &tt.refreshRestricted}
			err := ValidateCDCSnapshotParameters(asset, tt.runFullRefresh)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestValidateCDCSnapshotParametersStartLSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
		valid bool
	}{
		{name: "typical LSN", value: "0/16B6C50", valid: true},
		{name: "lowercase", value: "ab/cdef", valid: true},
		{name: "maximum LSN", value: "FFFFFFFF/FFFFFFFF", valid: true},
		{name: "nonzero high part", value: "1/0", valid: true},
		{name: "leading zeros", value: "00000000/00000001", valid: true},
		{name: "zero", value: "0/0"},
		{name: "zero padded", value: "00000000/00000000"},
		{name: "empty", value: ""},
		{name: "null", value: nil},
		{name: "boolean", value: true},
		{name: "numeric", value: 123},
		{name: "list", value: []string{"0/1"}},
		{name: "map", value: map[string]string{"lsn": "0/1"}},
		{name: "missing separator", value: "16B6C50"},
		{name: "extra separator", value: "0/1/2"},
		{name: "empty high part", value: "/1"},
		{name: "empty low part", value: "1/"},
		{name: "high part overflow", value: "100000000/1"},
		{name: "low part overflow", value: "0/100000000"},
		{name: "non hexadecimal", value: "0/G"},
		{name: "hexadecimal prefix", value: "0/0x1"},
		{name: "negative", value: "0/-1"},
		{name: "positive sign", value: "+1/0"},
		{name: "underscore", value: "0/1_0"},
		{name: "surrounding whitespace", value: " 0/1 "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			asset := &pipeline.Asset{Parameters: pipeline.ParameterMap{
				"cdc":               true,
				"cdc_snapshot_mode": "never",
				"cdc_start_lsn":     tt.value,
				"cdc_slot":          "bruin_slot",
				"source_table":      "public.users",
			}}
			err := ValidateCDCSnapshotParameters(asset, false)
			if tt.valid {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, "invalid cdc_start_lsn: must be a nonzero PostgreSQL LSN in X/Y hexadecimal format with at most 8 digits per part")
		})
	}
}
