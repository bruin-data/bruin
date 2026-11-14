package ingestr

import (
	"maps"
	"net/url"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func runBigQueryReadAPIOperator(t *testing.T, params pipeline.ParameterMap, sourceURI, destURI, wantErr string) []string {
	t.Helper()
	if destURI == "" {
		destURI = "postgres://sink:secret@localhost:5432/destination"
	}
	source := new(mockConnection)
	source.On("GetIngestrURI").Return(sourceURI, nil)
	destination := new(mockConnection)
	destination.On("GetIngestrURI").Return(destURI, nil)
	parameters := pipeline.ParameterMap{"source_connection": "source", "source_table": "dataset.orders"}
	maps.Copy(parameters, params)
	asset := &pipeline.Asset{Name: "public.orders", Type: pipeline.AssetTypeIngestr, Connection: "destination", Parameters: parameters}
	runner := new(mockRunner)
	var captured []string
	if wantErr == "" {
		runner.On("RunIngestr", mock.Anything, mock.Anything, mock.Anything, repo).
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

func TestBasicOperator_BigQueryReadAPI(t *testing.T) {
	t.Parallel()
	const (
		bigQuery         = "bigquery://my-project?credentials_base64=eyJhIjoiYiJ9&location=EU"
		bigQueryWithREST = bigQuery + "&read_api=rest"
	)
	for _, tt := range []struct {
		name      string
		sourceURI string
		destURI   string
		params    pipeline.ParameterMap
		// want is the source URI's read_api values; nil means the URI must be passed through unchanged.
		want    []string
		wantErr string
	}{
		{name: "unset", sourceURI: bigQuery},
		{name: "connection only", sourceURI: bigQueryWithREST, want: []string{"rest"}},
		{name: "asset only", sourceURI: bigQuery, params: pipeline.ParameterMap{"read_api": "rest"}, want: []string{"rest"}},
		{name: "asset overrides connection", sourceURI: bigQueryWithREST, params: pipeline.ParameterMap{"read_api": "storage"}, want: []string{"storage"}},
		{name: "version boundary", sourceURI: bigQuery, params: pipeline.ParameterMap{"read_api": "rest", "version": "v1.1.92"}, want: []string{"rest"}},
		{name: "invalid value", sourceURI: bigQuery, params: pipeline.ParameterMap{"read_api": "REST"}, wantErr: `invalid read_api "REST": must be "storage" or "rest"`},
		{name: "empty value", sourceURI: bigQuery, params: pipeline.ParameterMap{"read_api": ""}, wantErr: `invalid read_api ""`},
		{name: "invalid type", sourceURI: bigQuery, params: pipeline.ParameterMap{"read_api": true}, wantErr: `invalid read_api: must be "storage" or "rest"`},
		{name: "non-BigQuery source", sourceURI: "postgresql://source:secret@localhost:5432/source", params: pipeline.ParameterMap{"read_api": "rest"}, wantErr: "read_api is only supported for BigQuery sources"},
		{name: "Google Sheets source", sourceURI: bigQuery, params: pipeline.ParameterMap{"read_api": "rest", "source": "gsheets"}, wantErr: "read_api is only supported for BigQuery sources"},
		{name: "asset on old engine", sourceURI: bigQuery, params: pipeline.ParameterMap{"read_api": "rest", "version": "v1.1.91"}, wantErr: "bigquery read_api requires ingestr 1.1.92 or newer, but parameters.version resolved to 1.1.91"},
		{name: "connection on old engine", sourceURI: bigQueryWithREST, params: pipeline.ParameterMap{"version": "v1.1.91"}, wantErr: "bigquery read_api requires ingestr 1.1.92 or newer"},
		{name: "legacy engine", sourceURI: bigQuery, params: pipeline.ParameterMap{"read_api": "storage", "version": "v0"}, wantErr: "bigquery read_api requires ingestr 1.1.92 or newer"},
		{name: "unset on old engine", sourceURI: bigQuery, params: pipeline.ParameterMap{"version": "v1.1.91"}},
		{name: "BigQuery destination ignored by version gate", sourceURI: "postgresql://source:secret@localhost:5432/source", destURI: bigQueryWithREST, params: pipeline.ParameterMap{"version": "v1.1.91"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			args := runBigQueryReadAPIOperator(t, tt.params, tt.sourceURI, tt.destURI, tt.wantErr)
			if tt.wantErr != "" {
				return
			}
			if tt.want == nil {
				require.Equal(t, []string{tt.sourceURI}, cdcParityFlagValues(args, "--source-uri"))
				return
			}
			got := cdcParityURI(t, args, "--source-uri")
			require.Equal(t, "bigquery", got.Scheme)
			require.Equal(t, "my-project", got.Host)
			require.Equal(t, url.Values{
				"credentials_base64": {"eyJhIjoiYiJ9"},
				"location":           {"EU"},
				"read_api":           tt.want,
			}, got.Query())
		})
	}
}

func TestEnsureBigQueryReadAPISupport(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		version   string
		sourceURI string
		wantErr   bool
	}{
		{name: "default release supported", version: python.IngestrVersionV1, sourceURI: "bigquery://p?read_api=rest"},
		{name: "minimum release supported", version: "1.1.92", sourceURI: "bigquery://p?read_api=rest"},
		{name: "older release rejected", version: "1.1.91", sourceURI: "bigquery://p?read_api=storage", wantErr: true},
		{name: "legacy engine rejected", version: python.IngestrVersionV0, sourceURI: "bigquery://p?read_api=rest", wantErr: true},
		{name: "unset on older release", version: "1.1.91", sourceURI: "bigquery://p?location=EU"},
		{name: "Google Sheets source not checked", version: "1.1.91", sourceURI: "gsheets://p?read_api=rest"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ensureBigQueryReadAPISupport(resolvedEngine{ingestrVersion: tt.version}, tt.sourceURI)
			if tt.wantErr {
				require.ErrorContains(t, err, "bigquery read_api requires ingestr 1.1.92 or newer")
				return
			}
			require.NoError(t, err)
		})
	}
}
