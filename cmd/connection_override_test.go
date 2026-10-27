package cmd

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeConnectionTypes map[string]string

func (f fakeConnectionTypes) GetConnectionType(name string) string {
	return f[name]
}

func TestApplyConnectionOverride(t *testing.T) {
	t.Parallel()

	connections := fakeConnectionTypes{
		"gcp-default": "google_cloud_platform",
		"gcp-other":   "google_cloud_platform",
		"pg-other":    "postgres",
	}
	newPipeline := func() *pipeline.Pipeline {
		return &pipeline.Pipeline{
			DefaultConnections: map[string]string{"google_cloud_platform": "gcp-default"},
			Assets: []*pipeline.Asset{
				{Name: "sql", Type: pipeline.AssetTypeBigqueryQuery},
				{Name: "other", Type: pipeline.AssetTypeBigqueryQuery},
				{
					Name:       "py",
					Type:       pipeline.AssetTypePython,
					Connection: "gcp-default",
					Secrets: []pipeline.SecretMapping{
						{SecretKey: "gcp-default", InjectedKey: "gcp-default"},
						{SecretKey: "gcp-default", InjectedKey: "WAREHOUSE"},
						{SecretKey: "slack", InjectedKey: "slack"},
					},
				},
			},
		}
	}

	t.Run("runs the asset on the connection, leaving the others", func(t *testing.T) {
		t.Parallel()
		p := newPipeline()
		target := &pipeline.Asset{Name: "sql", Type: pipeline.AssetTypeBigqueryQuery}

		require.NoError(t, applyConnectionOverride(p, target, "gcp-other", connections))

		assert.Equal(t, "gcp-other", p.GetAssetByName("sql").Connection)
		assert.Equal(t, "gcp-other", target.Connection)
		assert.Empty(t, p.GetAssetByName("other").Connection)
	})

	t.Run("reads the secrets of the asset's connection from the new one, under the same names and its own", func(t *testing.T) {
		t.Parallel()
		p := newPipeline()

		require.NoError(t, applyConnectionOverride(p, p.GetAssetByName("py"), "gcp-other", connections))

		// BRUIN_CONNECTION names gcp-other now, so its credentials are injected under that name too.
		assert.Equal(t, []pipeline.SecretMapping{
			{SecretKey: "gcp-other", InjectedKey: "gcp-default"},
			{SecretKey: "gcp-other", InjectedKey: "WAREHOUSE"},
			{SecretKey: "slack", InjectedKey: "slack"},
			{SecretKey: "gcp-other", InjectedKey: "gcp-other"},
		}, p.GetAssetByName("py").Secrets)
	})

	t.Run("injects the new connection once when the asset and the target are the same", func(t *testing.T) {
		t.Parallel()
		p := newPipeline()
		py := p.GetAssetByName("py")

		require.NoError(t, applyConnectionOverride(p, py, "gcp-other", connections))

		injected := 0
		for _, secret := range py.Secrets {
			if secret.InjectedKey == "gcp-other" {
				injected++
			}
		}
		assert.Equal(t, 1, injected)
	})

	t.Run("points an ingestr asset's destination_connection at the new connection", func(t *testing.T) {
		t.Parallel()
		p := newPipeline()
		ingest := &pipeline.Asset{
			Name:       "ingest",
			Type:       pipeline.AssetTypeIngestr,
			Parameters: pipeline.ParameterMap{"destination": "bigquery", "destination_connection": "gcp-default", "source_connection": "pg-other"},
		}
		p.Assets = append(p.Assets, ingest)

		require.NoError(t, applyConnectionOverride(p, ingest, "gcp-other", connections))

		assert.Equal(t, "gcp-other", ingest.Connection)
		assert.Equal(t, "gcp-other", ingest.Parameters["destination_connection"])
		assert.Equal(t, "pg-other", ingest.Parameters["source_connection"])
	})

	t.Run("rejects a connection of another type", func(t *testing.T) {
		t.Parallel()
		p := newPipeline()
		err := applyConnectionOverride(p, p.GetAssetByName("sql"), "pg-other", connections)
		require.ErrorContains(t, err, "is a postgres connection")
		assert.Empty(t, p.GetAssetByName("sql").Connection)
	})

	t.Run("rejects an unknown connection", func(t *testing.T) {
		t.Parallel()
		p := newPipeline()
		require.ErrorContains(t, applyConnectionOverride(p, p.GetAssetByName("sql"), "missing", connections), "does not exist")
	})

	t.Run("requires a single asset", func(t *testing.T) {
		t.Parallel()
		require.ErrorContains(t, applyConnectionOverride(newPipeline(), nil, "gcp-other", connections), "requires running a single asset")
	})

	t.Run("skips the type check without the connections", func(t *testing.T) {
		t.Parallel()
		p := newPipeline()
		require.NoError(t, applyConnectionOverride(p, p.GetAssetByName("sql"), "anything", nil))
		assert.Equal(t, "anything", p.GetAssetByName("sql").Connection)
	})
}

func TestContinuedConnectionOverride(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		given      string
		continuing bool
		saved      string
		want       string
		wantErr    string
	}{
		{name: "a new run takes the given one", given: "gcp-other", want: "gcp-other"},
		{name: "a new run ignores a saved one", given: "", saved: "gcp-other", want: ""},
		{name: "continuing keeps the one the run started on", continuing: true, saved: "gcp-other", want: "gcp-other"},
		{name: "continuing with the same one", given: "gcp-other", continuing: true, saved: "gcp-other", want: "gcp-other"},
		{name: "continuing a run without one", continuing: true, want: ""},
		{name: "continuing on another one is rejected", given: "gcp-third", continuing: true, saved: "gcp-other", wantErr: "started with --connection-override gcp-other"},
		{name: "continuing a run without one on one is rejected", given: "gcp-other", continuing: true, wantErr: "started without --connection-override"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := continuedConnectionOverride(tc.given, tc.continuing, tc.saved)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
