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

	t.Run("reads the secrets of the asset's connection from the new one, under the same names", func(t *testing.T) {
		t.Parallel()
		p := newPipeline()

		require.NoError(t, applyConnectionOverride(p, p.GetAssetByName("py"), "gcp-other", connections))

		assert.Equal(t, []pipeline.SecretMapping{
			{SecretKey: "gcp-other", InjectedKey: "gcp-default"},
			{SecretKey: "gcp-other", InjectedKey: "WAREHOUSE"},
			{SecretKey: "slack", InjectedKey: "slack"},
		}, p.GetAssetByName("py").Secrets)
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
