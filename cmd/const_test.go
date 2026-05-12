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

func TestVariableOverridesMutator_VariantWinsOnOverlap(t *testing.T) {
	t.Parallel()

	newPipeline := func() *pipeline.Pipeline {
		return &pipeline.Pipeline{
			Variables: pipeline.Variables{
				"client":        {"type": "string", "default": "alpha"},
				"region":        {"type": "string", "default": "us"},
				"forecast_days": {"type": "integer", "default": int64(7)},
				"min_signups":   {"type": "integer", "default": int64(0)},
			},
			Variants: pipeline.VariantSet{
				"client_alpha": {
					"client":        "alpha",
					"region":        "us",
					"forecast_days": int64(7),
				},
			},
			SelectedVariant: "client_alpha",
		}
	}

	t.Run("overlapping --var key is dropped, variant value preserved", func(t *testing.T) {
		t.Parallel()
		p := newPipeline()

		mutator := variableOverridesMutator([]string{
			`{"forecast_days": 14}`, // overlaps with variant, must be ignored
			`{"min_signups": 5}`,    // no overlap, must apply
		})
		out, err := mutator(t.Context(), p)
		require.NoError(t, err)

		vals := out.Variables.Value()
		assert.Equal(t, int64(7), vals["forecast_days"], "variant value should win over --var")
		assert.Equal(t, int64(5), vals["min_signups"], "non-overlapping --var should still apply")
	})

	t.Run("no variant selected: all --var overrides apply", func(t *testing.T) {
		t.Parallel()
		p := newPipeline()
		p.SelectedVariant = ""

		mutator := variableOverridesMutator([]string{
			`{"forecast_days": 14, "min_signups": 5}`,
		})
		out, err := mutator(t.Context(), p)
		require.NoError(t, err)

		vals := out.Variables.Value()
		assert.Equal(t, int64(14), vals["forecast_days"])
		assert.Equal(t, int64(5), vals["min_signups"])
	})

	t.Run("--var overlapping every variant key is fully suppressed", func(t *testing.T) {
		t.Parallel()
		p := newPipeline()

		mutator := variableOverridesMutator([]string{
			`{"client": "manual", "region": "zz", "forecast_days": 99}`,
		})
		out, err := mutator(t.Context(), p)
		require.NoError(t, err)

		vals := out.Variables.Value()
		assert.Equal(t, "alpha", vals["client"])
		assert.Equal(t, "us", vals["region"])
		assert.Equal(t, int64(7), vals["forecast_days"])
	})

	t.Run("unknown --var key still errors", func(t *testing.T) {
		t.Parallel()
		p := newPipeline()

		mutator := variableOverridesMutator([]string{`{"nope": 1}`})
		_, err := mutator(t.Context(), p)
		require.Error(t, err)
		assert.ErrorContains(t, err, `no such variable "nope"`)
	})
}
