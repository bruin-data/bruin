package cmd

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderAssetHooks(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "schema.asset",
		Hooks: pipeline.Hooks{
			Pre:  []pipeline.Hook{{Query: "select '{{ foo }}'"}},
			Post: []pipeline.Hook{{Query: "select '{{ foo }}_2'"}},
		},
	}

	err := renderAssetHooks(t.Context(), &pipeline.Pipeline{Name: "pipe"}, asset, jinja.NewRenderer(jinja.Context{
		"foo": "bar",
	}))
	require.NoError(t, err)
	assert.Equal(t, pipeline.Hooks{
		Pre:  []pipeline.Hook{{Query: "select 'bar'"}},
		Post: []pipeline.Hook{{Query: "select 'bar_2'"}},
	}, asset.Hooks)
}

func TestRenderPipelineHooks(t *testing.T) {
	t.Parallel()

	p := &pipeline.Pipeline{
		Name: "pipe",
		Assets: []*pipeline.Asset{
			{
				Name: "schema.one",
				Hooks: pipeline.Hooks{
					Pre: []pipeline.Hook{{Query: "select '{{ foo }}'"}},
				},
			},
			{
				Name: "schema.two",
				Hooks: pipeline.Hooks{
					Post: []pipeline.Hook{{Query: "select '{{ foo }}'"}},
				},
			},
		},
	}

	err := renderPipelineHooks(t.Context(), p, jinja.NewRenderer(jinja.Context{
		"foo": "bar",
	}))
	require.NoError(t, err)
	assert.Equal(t, "select 'bar'", p.Assets[0].Hooks.Pre[0].Query)
	assert.Equal(t, "select 'bar'", p.Assets[1].Hooks.Post[0].Query)
}

func TestRenderAssetHooks_Error(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "schema.asset",
		Hooks: pipeline.Hooks{
			Pre: []pipeline.Hook{{Query: "select '{{ missing }}'"}},
		},
	}

	err := renderAssetHooks(t.Context(), &pipeline.Pipeline{Name: "pipe"}, asset, jinja.NewRenderer(jinja.Context{}))
	require.Error(t, err)
	assert.ErrorContains(t, err, "error rendering hooks for asset schema.asset")
}
