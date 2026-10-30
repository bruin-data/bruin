package ingestr

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestEnsureStarRocksEngineSupport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		uris    []string
		wantErr bool
	}{
		{name: "HTTPS supported", version: "1.1.78", uris: []string{"starrocks://host/db?http_scheme=https"}},
		{name: "default release supported", version: python.IngestrVersionV1, uris: []string{"starrocks://host/db?http_scheme=https"}},
		{name: "HTTPS requires new engine", version: "1.1.77", uris: []string{"starrocks://host/db?http_scheme=https"}, wantErr: true},
		{name: "legacy engine rejected", version: python.IngestrVersionV0, uris: []string{"starrocks://host/db?http_scheme=https"}, wantErr: true},
		{name: "explicit HTTP requires support", version: "1.1.77", uris: []string{"starrocks://host/db?http_scheme=http"}, wantErr: true},
		{name: "omitted scheme preserves compatibility", version: "1.1.77", uris: []string{"starrocks://host/db?http_port=8030&ssl=true"}},
		{name: "empty scheme preserves compatibility", version: "1.1.77", uris: []string{"starrocks://host/db?http_scheme="}},
		{name: "unrelated destination", version: "1.1.77", uris: []string{"postgres://host/db?http_scheme=https"}},
		{name: "source checked", version: "1.1.77", uris: []string{"starrocks://host/db?http_scheme=https", "bigquery://project"}, wantErr: true},
		{name: "destination checked", version: "1.1.77", uris: []string{"postgres://host/db", "starrocks://host/db?http_scheme=https"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ensureStarRocksEngineSupport(resolvedEngine{ingestrVersion: tt.version}, tt.uris...)
			if tt.wantErr {
				require.ErrorContains(t, err, "starrocks http_scheme requires ingestr 1.1.78 or newer")
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestOperators_RejectStarRocksHTTPSWithOlderEngine(t *testing.T) {
	t.Parallel()

	for _, seed := range []bool{false, true} {
		name := "ingestr"
		if seed {
			name = "seed"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			source := new(mockConnection)
			source.On("GetIngestrURI").Return("postgresql://host/db", nil)
			dest := new(mockConnection)
			dest.On("GetIngestrURI").Return("starrocks://host/db?http_scheme=https", nil)
			fetcher := &simpleConnectionFetcher{connections: map[string]*mockConnection{"source": source, "dest": dest}}
			asset := &pipeline.Asset{
				Name: "db.users", Type: pipeline.AssetTypeIngestr, Connection: "dest",
				Parameters: pipeline.ParameterMap{"source_connection": "source", "source_table": "public.users", "path": "users.csv", "version": "v1.1.77"},
			}
			ti := &scheduler.AssetInstance{Pipeline: &pipeline.Pipeline{}, Asset: asset}
			runner := new(mockRunner)
			var err error
			if seed {
				op := &SeedOperator{conn: fetcher, finder: &mockFinder{}, runner: runner}
				err = op.Run(t.Context(), ti)
			} else {
				op := &BasicOperator{conn: fetcher, finder: &mockFinder{}, runner: runner}
				err = op.Run(t.Context(), ti)
			}
			require.ErrorContains(t, err, "starrocks http_scheme requires ingestr 1.1.78 or newer")
			runner.AssertNotCalled(t, "RunIngestr", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		})
	}
}
