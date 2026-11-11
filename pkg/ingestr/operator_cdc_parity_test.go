package ingestr

import (
	"maps"
	"net/url"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func runCDCParityOperator(t *testing.T, params pipeline.ParameterMap, sourceURI, wantErr string) []string {
	t.Helper()
	if sourceURI == "" {
		sourceURI = "postgresql://source:secret@localhost:5432/source?sslmode=require&application_name=bruin+CDC"
	}
	source := new(mockConnection)
	source.On("GetIngestrURI").Return(sourceURI, nil)
	destination := new(mockConnection)
	destination.On("GetIngestrURI").Return("postgres://sink:secret@localhost:5432/destination?sslmode=require", nil)
	parameters := pipeline.ParameterMap{"source_connection": "source", "source_table": "public.orders"}
	maps.Copy(parameters, params)
	asset := &pipeline.Asset{Name: "public.orders", Type: pipeline.AssetTypeIngestr, Connection: "destination", Parameters: parameters}
	runner := new(mockRunner)
	var captured []string
	if wantErr == "" {
		runner.On("RunIngestr", mock.Anything, mock.Anything, []string(nil), repo).
			Run(func(arguments mock.Arguments) {
				captured = append([]string(nil), arguments.Get(1).([]string)...)
			}).Return(nil).Once()
	}
	op := &BasicOperator{
		conn:   &simpleConnectionFetcher{connections: map[string]*mockConnection{"source": source, "destination": destination}},
		finder: &mockFinder{}, runner: runner,
	}
	err := op.Run(t.Context(), &scheduler.AssetInstance{Pipeline: &pipeline.Pipeline{}, Asset: asset})
	if wantErr != "" {
		require.ErrorContains(t, err, wantErr)
		runner.AssertNotCalled(t, "RunIngestr", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		return nil
	}
	require.NoError(t, err)
	runner.AssertExpectations(t)
	return captured
}

func cdcParityFlagValues(args []string, flag string) []string {
	var values []string
	for index, arg := range args {
		if value, present := strings.CutPrefix(arg, flag+"="); present {
			values = append(values, value)
		} else if arg == flag && index+1 < len(args) {
			values = append(values, args[index+1])
		}
	}
	return values
}

func cdcParityURI(t *testing.T, args []string, flag string) *url.URL {
	t.Helper()
	values := cdcParityFlagValues(args, flag)
	require.Len(t, values, 1)
	parsed, err := url.Parse(values[0])
	require.NoError(t, err)
	return parsed
}

func TestBasicOperator_PostgresCDCURIControls(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		params    pipeline.ParameterMap
		sourceURI string
		wantQuery url.Values
		wantErr   string
	}{
		{name: "omitted defaults", params: pipeline.ParameterMap{"cdc": true}},
		{name: "native false and duration", params: pipeline.ParameterMap{"cdc": true, "cdc_binary": false, "cdc_discover_interval": "1m30s"}, wantQuery: url.Values{"binary": {"false"}, "discover_interval": {"1m30s"}}},
		{name: "native true and numeric zero", params: pipeline.ParameterMap{"cdc": true, "cdc_binary": true, "cdc_discover_interval": 0}, wantQuery: url.Values{"binary": {"true"}, "discover_interval": {"0"}}},
		{name: "quoted boolean and off", params: pipeline.ParameterMap{"cdc": true, "cdc_binary": "false", "cdc_discover_interval": "off"}, wantQuery: url.Values{"binary": {"false"}, "discover_interval": {"off"}}},
		{name: "discovery version boundary", params: pipeline.ParameterMap{"cdc": true, "cdc_discover_interval": "30s", "version": "v1.0.71"}, wantQuery: url.Values{"discover_interval": {"30s"}}},
		{name: "binary version boundary", params: pipeline.ParameterMap{"cdc": true, "cdc_binary": true, "version": "v1.0.72"}, wantQuery: url.Values{"binary": {"true"}}},
		{
			name:      "parameters override existing query once",
			params:    pipeline.ParameterMap{"cdc": true, "cdc_binary": false, "cdc_discover_interval": "off", "cdc_state_id": "tenant & team=A"},
			sourceURI: "postgresql+cdc://source:secret@localhost:5432/source?sslmode=require&application_name=bruin+CDC&binary=true&discover_interval=1s",
			wantQuery: url.Values{"binary": {"false"}, "discover_interval": {"off"}, "state_id": {"tenant & team=A"}},
		},
		{name: "discovery on wrong source", params: pipeline.ParameterMap{"cdc": true, "cdc_discover_interval": "30s"}, sourceURI: "mysql://localhost/db", wantErr: "cdc_discover_interval is only supported for PostgreSQL CDC sources"},
		{name: "binary on wrong source", params: pipeline.ParameterMap{"cdc": true, "cdc_binary": false}, sourceURI: "mysql://localhost/db", wantErr: "cdc_binary is only supported for PostgreSQL CDC sources"},
		{name: "discovery requires CDC", params: pipeline.ParameterMap{"cdc_discover_interval": "30s"}, wantErr: "cdc_discover_interval requires cdc: true"},
		{name: "binary requires CDC", params: pipeline.ParameterMap{"cdc_binary": false}, wantErr: "cdc_binary requires cdc: true"},
		{name: "invalid discovery unit", params: pipeline.ParameterMap{"cdc": true, "cdc_discover_interval": "30"}, wantErr: "invalid cdc_discover_interval"},
		{name: "negative discovery interval", params: pipeline.ParameterMap{"cdc": true, "cdc_discover_interval": "-1s"}, wantErr: "invalid cdc_discover_interval"},
		{name: "structured discovery interval", params: pipeline.ParameterMap{"cdc": true, "cdc_discover_interval": []string{"30s"}}, wantErr: "invalid cdc_discover_interval"},
		{name: "invalid binary", params: pipeline.ParameterMap{"cdc": true, "cdc_binary": "TRUE"}, wantErr: "invalid cdc_binary"},
		{name: "numeric binary", params: pipeline.ParameterMap{"cdc": true, "cdc_binary": 1}, wantErr: "invalid cdc_binary"},
		{name: "old discovery engine", params: pipeline.ParameterMap{"cdc": true, "cdc_discover_interval": "30s", "version": "v1.0.70"}, wantErr: "cdc_discover_interval requires ingestr 1.0.71"},
		{name: "old binary engine", params: pipeline.ParameterMap{"cdc": true, "cdc_binary": false, "version": "v1.0.71"}, wantErr: "cdc_binary requires ingestr 1.0.72"},
		{name: "legacy binary engine", params: pipeline.ParameterMap{"cdc": true, "cdc_binary": false, "version": "v0"}, wantErr: "cdc_binary requires ingestr 1.0.72"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			args := runCDCParityOperator(t, tt.params, tt.sourceURI, tt.wantErr)
			if tt.wantErr != "" {
				return
			}
			source := cdcParityURI(t, args, "--source-uri")
			require.Contains(t, []string{"postgres+cdc", "postgresql+cdc"}, source.Scheme)
			want := url.Values{"sslmode": {"require"}, "application_name": {"bruin CDC"}}
			maps.Copy(want, tt.wantQuery)
			require.Equal(t, want, source.Query())
			require.Equal(t, url.Values{"sslmode": {"require"}}, cdcParityURI(t, args, "--dest-uri").Query())
			require.Empty(t, cdcParityFlagValues(args, "--cdc-discover-interval"))
			require.Empty(t, cdcParityFlagValues(args, "--cdc-binary"))
		})
	}
}

func TestBasicOperator_CDCTableNaming(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		params    pipeline.ParameterMap
		sourceURI string
		want      []string
		wantErr   string
	}{
		{name: "omitted", params: pipeline.ParameterMap{"cdc": true}},
		{name: "table at version boundary", params: pipeline.ParameterMap{"cdc": true, "cdc_table_naming": "table", "version": "v1.1.49"}, want: []string{"table"}},
		{name: "schema table", params: pipeline.ParameterMap{"cdc": true, "cdc_table_naming": "schema_table"}, want: []string{"schema_table"}},
		{name: "other CDC source", sourceURI: "mysql://localhost/db", params: pipeline.ParameterMap{"cdc": true, "cdc_table_naming": "table"}, want: []string{"table"}},
		{name: "ordinary source", params: pipeline.ParameterMap{"cdc_table_naming": "table"}, wantErr: "cdc_table_naming requires cdc: true"},
		{name: "invalid value", params: pipeline.ParameterMap{"cdc": true, "cdc_table_naming": "flat"}, wantErr: "invalid cdc_table_naming"},
		{name: "invalid type", params: pipeline.ParameterMap{"cdc": true, "cdc_table_naming": true}, wantErr: "invalid cdc_table_naming"},
		{name: "older engine", params: pipeline.ParameterMap{"cdc": true, "cdc_table_naming": "table", "version": "v1.1.48"}, wantErr: "cdc_table_naming requires ingestr 1.1.49"},
		{name: "legacy engine", params: pipeline.ParameterMap{"cdc": true, "cdc_table_naming": "table", "version": "v0"}, wantErr: "cdc_table_naming requires ingestr 1.1.49"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			params := maps.Clone(tt.params)
			params["source_table"] = "*"
			args := runCDCParityOperator(t, params, tt.sourceURI, tt.wantErr)
			if tt.wantErr == "" {
				require.Equal(t, tt.want, cdcParityFlagValues(args, "--cdc-table-naming"))
				require.Empty(t, cdcParityFlagValues(args, "--source-table"))
				require.False(t, cdcParityURI(t, args, "--source-uri").Query().Has("table_naming"))
			}
		})
	}
}

func TestBasicOperator_PostgresExportedSnapshot(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		params    pipeline.ParameterMap
		sourceURI string
		wantErr   string
	}{
		{name: "ordinary PostgreSQL"},
		{name: "version boundary", params: pipeline.ParameterMap{"version": "v1.1.83"}},
		{name: "replace existing snapshot", sourceURI: "postgres://localhost/source?sslmode=require&application_name=bruin+CDC&snapshot=00000001-00000002-1"},
		{name: "CDC enabled", params: pipeline.ParameterMap{"cdc": true}, wantErr: "postgres_snapshot requires ordinary PostgreSQL ingestion"},
		{name: "CDC source URI", sourceURI: "postgres+cdc://localhost/source", wantErr: "postgres_snapshot is only supported for ordinary PostgreSQL sources"},
		{name: "wrong source", sourceURI: "mysql://localhost/source", wantErr: "postgres_snapshot is only supported for ordinary PostgreSQL sources"},
		{name: "invalid format", params: pipeline.ParameterMap{"postgres_snapshot": "snapshot'"}, wantErr: "invalid postgres_snapshot"},
		{name: "invalid type", params: pipeline.ParameterMap{"postgres_snapshot": 123}, wantErr: "invalid postgres_snapshot"},
		{name: "old engine", params: pipeline.ParameterMap{"version": "v1.1.82"}, wantErr: "postgres_snapshot requires ingestr 1.1.83"},
		{name: "legacy engine", params: pipeline.ParameterMap{"version": "v0"}, wantErr: "postgres_snapshot requires ingestr 1.1.83"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			params := pipeline.ParameterMap{"postgres_snapshot": "00000003-0000001B-1"}
			maps.Copy(params, tt.params)
			args := runCDCParityOperator(t, params, tt.sourceURI, tt.wantErr)
			if tt.wantErr == "" {
				require.Equal(t, url.Values{"snapshot": {"00000003-0000001B-1"}, "sslmode": {"require"}, "application_name": {"bruin CDC"}}, cdcParityURI(t, args, "--source-uri").Query())
				require.False(t, cdcParityURI(t, args, "--dest-uri").Query().Has("snapshot"))
				require.Empty(t, cdcParityFlagValues(args, "--postgres-snapshot"))
			}
		})
	}
}

func TestBasicOperator_IngestrMetricsCompatibility(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		params  pipeline.ParameterMap
		want    []string
		wantErr string
	}{
		{name: "omitted metrics", params: pipeline.ParameterMap{"cdc": true, "version": "v1.0.72"}},
		{name: "batch canonical boundary", params: pipeline.ParameterMap{"cdc": true, "metrics_addr": "127.0.0.1:6060", "version": "v1.1.87"}, want: []string{"127.0.0.1:6060"}},
		{name: "batch alias boundary", params: pipeline.ParameterMap{"cdc": true, "cdc_stream_metrics_addr": "127.0.0.1:6060", "version": "v1.1.87"}, want: []string{"127.0.0.1:6060"}},
		{name: "batch canonical wins", params: pipeline.ParameterMap{"cdc": true, "metrics_addr": "127.0.0.1:6061", "cdc_stream_metrics_addr": "127.0.0.1:6060"}, want: []string{"127.0.0.1:6061"}},
		{name: "ordinary batch metrics", params: pipeline.ParameterMap{"metrics_addr": "127.0.0.1:6060"}, want: []string{"127.0.0.1:6060"}},
		{name: "ordinary batch ignores CDC alias", params: pipeline.ParameterMap{"cdc_stream_metrics_addr": "127.0.0.1:6060", "version": "v1.1.86"}},
		{name: "ordinary stream preserves CDC alias", params: pipeline.ParameterMap{"stream": true, "cdc_stream_metrics_addr": "127.0.0.1:6060", "version": "v1.0.73"}, want: []string{"127.0.0.1:6060"}},
		{name: "stream canonical boundary", params: pipeline.ParameterMap{"cdc": true, "stream": true, "metrics_addr": "127.0.0.1:6060", "version": "v1.0.73"}, want: []string{"127.0.0.1:6060"}},
		{name: "stream alias boundary", params: pipeline.ParameterMap{"cdc": true, "stream": true, "cdc_stream_metrics_addr": "127.0.0.1:6060", "version": "v1.0.73"}, want: []string{"127.0.0.1:6060"}},
		{name: "stream canonical wins", params: pipeline.ParameterMap{"cdc": true, "stream": true, "metrics_addr": "127.0.0.1:6061", "cdc_stream_metrics_addr": "127.0.0.1:6060", "version": "v1.1.86"}, want: []string{"127.0.0.1:6061"}},
		{name: "deprecated streaming selector", params: pipeline.ParameterMap{"cdc": true, "cdc_mode": "stream", "metrics_addr": "127.0.0.1:6060", "version": "v1.0.73"}, want: []string{"127.0.0.1:6060"}},
		{name: "old batch canonical", params: pipeline.ParameterMap{"cdc": true, "metrics_addr": "127.0.0.1:6060", "version": "v1.1.86"}, wantErr: "batch metrics require ingestr 1.1.87"},
		{name: "old batch alias", params: pipeline.ParameterMap{"cdc": true, "cdc_stream_metrics_addr": "127.0.0.1:6060", "version": "v1.1.86"}, wantErr: "batch metrics require ingestr 1.1.87"},
		{name: "old stream canonical", params: pipeline.ParameterMap{"cdc": true, "stream": true, "metrics_addr": "127.0.0.1:6060", "version": "v1.0.72"}, wantErr: "1.0.73"},
		{name: "old stream alias", params: pipeline.ParameterMap{"cdc": true, "stream": true, "cdc_stream_metrics_addr": "127.0.0.1:6060", "version": "v1.0.72"}, wantErr: "1.0.73"},
		{name: "legacy stream engine", params: pipeline.ParameterMap{"cdc": true, "stream": true, "metrics_addr": "127.0.0.1:6060", "version": "v0"}, wantErr: "1.0.73"},
		{name: "invalid address type", params: pipeline.ParameterMap{"metrics_addr": true}, wantErr: "invalid metrics_addr"},
		{name: "blank canonical address", params: pipeline.ParameterMap{"metrics_addr": " \t"}, wantErr: "invalid metrics_addr"},
		{name: "invalid alias type", params: pipeline.ParameterMap{"cdc": true, "cdc_stream_metrics_addr": 6060}, wantErr: "invalid cdc_stream_metrics_addr"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			args := runCDCParityOperator(t, tt.params, "", tt.wantErr)
			if tt.wantErr == "" {
				require.Equal(t, tt.want, cdcParityFlagValues(args, "--metrics-addr"))
				require.Empty(t, cdcParityFlagValues(args, "--cdc-stream-metrics-addr"))
			}
		})
	}
}

func TestBasicOperator_IngestrLoadControls(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		params  pipeline.ParameterMap
		want    map[string][]string
		wantErr string
	}{
		{name: "omitted controls"},
		{
			name:   "independent source and destination settings",
			params: pipeline.ParameterMap{"extract_parallelism": 2, "destination_parallelism": 8, "batch_size": 64, "no_load_timestamp": true, "no_run_id": false},
			want:   map[string][]string{"--extract-parallelism": {"2"}, "--destination-parallelism": {"8"}, "--batch-size": {"64"}, "--no-load-timestamp": {"true"}, "--no-run-id": {"false"}},
		},
		{name: "source concurrency does not insert destination override", params: pipeline.ParameterMap{"extract_parallelism": 2}, want: map[string][]string{"--extract-parallelism": {"2"}}},
		{name: "destination version boundary", params: pipeline.ParameterMap{"destination_parallelism": "4", "version": "v1.1.85"}, want: map[string][]string{"--destination-parallelism": {"4"}}},
		{name: "batch size version boundary", params: pipeline.ParameterMap{"batch_size": "32", "version": "v1.1.30"}, want: map[string][]string{"--batch-size": {"32"}}},
		{name: "timestamp version boundary", params: pipeline.ParameterMap{"no_load_timestamp": "false", "version": "v1.0.39"}, want: map[string][]string{"--no-load-timestamp": {"false"}}},
		{name: "run ID version boundary", params: pipeline.ParameterMap{"no_run_id": "true", "version": "v1.1.31"}, want: map[string][]string{"--no-run-id": {"true"}}},
		{name: "old destination engine", params: pipeline.ParameterMap{"destination_parallelism": 4, "version": "v1.1.84"}, wantErr: "destination_parallelism requires ingestr 1.1.85"},
		{name: "old batch engine", params: pipeline.ParameterMap{"batch_size": 32, "version": "v1.1.29"}, wantErr: "batch_size requires ingestr 1.1.30"},
		{name: "old timestamp engine", params: pipeline.ParameterMap{"no_load_timestamp": false, "version": "v1.0.38"}, wantErr: "no_load_timestamp requires ingestr 1.0.39"},
		{name: "old run ID engine", params: pipeline.ParameterMap{"no_run_id": false, "version": "v1.1.30"}, wantErr: "no_run_id requires ingestr 1.1.31"},
		{name: "legacy destination engine", params: pipeline.ParameterMap{"destination_parallelism": 4, "version": "v0"}, wantErr: "destination_parallelism requires ingestr 1.1.85"},
		{name: "zero batch size", params: pipeline.ParameterMap{"batch_size": 0}, wantErr: "invalid batch_size"},
		{name: "negative destination concurrency", params: pipeline.ParameterMap{"destination_parallelism": -1}, wantErr: "invalid destination_parallelism"},
		{name: "fractional batch size", params: pipeline.ParameterMap{"batch_size": 1.5}, wantErr: "invalid batch_size"},
		{name: "batch byte overflow", params: pipeline.ParameterMap{"batch_size": int64(1 << 43)}, wantErr: "invalid batch_size"},
		{name: "invalid timestamp type", params: pipeline.ParameterMap{"no_load_timestamp": 1}, wantErr: "invalid no_load_timestamp"},
		{name: "invalid run ID type", params: pipeline.ParameterMap{"no_run_id": nil}, wantErr: "invalid no_run_id"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			args := runCDCParityOperator(t, tt.params, "", tt.wantErr)
			if tt.wantErr != "" {
				return
			}
			for _, flag := range []string{"--extract-parallelism", "--destination-parallelism", "--batch-size", "--no-load-timestamp", "--no-run-id"} {
				require.Equal(t, tt.want[flag], cdcParityFlagValues(args, flag), flag)
			}
		})
	}
}
