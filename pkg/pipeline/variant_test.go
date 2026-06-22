package pipeline_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// fakeRender is a minimal stand-in for jinja: replaces {{ var.<name> }} with
// the corresponding value from the supplied vars map. Good enough for unit
// tests that don't need the real Jinja engine.
func fakeRender(vars map[string]any) pipeline.RenderFunc {
	return func(in string) (string, error) {
		out := in
		for k, v := range vars {
			placeholder := fmt.Sprintf("{{ var.%s }}", k)
			out = strings.ReplaceAll(out, placeholder, fmt.Sprintf("%v", v))
		}
		if strings.Contains(out, "{{") {
			return "", fmt.Errorf("unresolved template in %q", in)
		}
		return out, nil
	}
}

func makeFakeRenderer(vars map[string]any, _ string) pipeline.RenderFunc {
	return fakeRender(vars)
}

func TestVariantSet_Names(t *testing.T) {
	t.Parallel()
	vs := pipeline.VariantSet{
		"client_b": {"client": "b"},
		"client_a": {"client": "a"},
	}
	assert.Equal(t, []string{"client_a", "client_b"}, vs.Names())
}

func TestVariantSet_Validate(t *testing.T) {
	t.Parallel()
	vars := pipeline.Variables{
		"client": map[string]any{"type": "string", "default": "a"},
	}

	t.Run("rejects unknown variable key", func(t *testing.T) {
		t.Parallel()
		vs := pipeline.VariantSet{"v1": {"region": "us"}}
		err := vs.Validate(vars)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown variable")
	})

	t.Run("rejects invalid variant name", func(t *testing.T) {
		t.Parallel()
		vs := pipeline.VariantSet{"bad name!": {"client": "a"}}
		err := vs.Validate(vars)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid variant name")
	})

	t.Run("accepts valid set", func(t *testing.T) {
		t.Parallel()
		vs := pipeline.VariantSet{"v1": {"client": "a"}, "v2": {"client": "b"}}
		require.NoError(t, vs.Validate(vars))
	})
}

func TestVariantSet_Validate_TypeChecking(t *testing.T) {
	t.Parallel()

	vars := pipeline.Variables{
		"client":   map[string]any{"type": "string", "default": "a"},
		"limit":    map[string]any{"type": "integer", "default": 10},
		"ratio":    map[string]any{"type": "number", "default": 1.5},
		"enabled":  map[string]any{"type": "boolean", "default": true},
		"tags":     map[string]any{"type": "array", "default": []any{"x"}},
		"meta":     map[string]any{"type": "object", "default": map[string]any{}},
		"nullable": map[string]any{"type": "null", "default": nil},
		"untyped":  map[string]any{"default": "anything"}, // no "type" → permissive
	}

	t.Run("rejects string variable overridden with int", func(t *testing.T) {
		t.Parallel()
		vs := pipeline.VariantSet{"v": {"client": 42}}
		err := vs.Validate(vars)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "type mismatch")
		assert.Contains(t, err.Error(), "expected string")
	})

	t.Run("rejects integer variable overridden with string", func(t *testing.T) {
		t.Parallel()
		vs := pipeline.VariantSet{"v": {"limit": "ten"}}
		err := vs.Validate(vars)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected integer")
	})

	t.Run("rejects boolean variable overridden with string", func(t *testing.T) {
		t.Parallel()
		vs := pipeline.VariantSet{"v": {"enabled": "true"}}
		err := vs.Validate(vars)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected boolean")
	})

	t.Run("rejects array variable overridden with string", func(t *testing.T) {
		t.Parallel()
		vs := pipeline.VariantSet{"v": {"tags": "x,y"}}
		err := vs.Validate(vars)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected array")
	})

	t.Run("rejects object variable overridden with array", func(t *testing.T) {
		t.Parallel()
		vs := pipeline.VariantSet{"v": {"meta": []any{"x"}}}
		err := vs.Validate(vars)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected object")
	})

	t.Run("accepts integer overridden with int", func(t *testing.T) {
		t.Parallel()
		vs := pipeline.VariantSet{"v": {"limit": 99}}
		require.NoError(t, vs.Validate(vars))
	})

	t.Run("accepts integer overridden with int64", func(t *testing.T) {
		t.Parallel()
		vs := pipeline.VariantSet{"v": {"limit": int64(99)}}
		require.NoError(t, vs.Validate(vars))
	})

	t.Run("accepts integer overridden with whole-number float", func(t *testing.T) {
		t.Parallel()
		// JSON / YAML loaders may decode "99" as float64 — accept it.
		vs := pipeline.VariantSet{"v": {"limit": float64(99)}}
		require.NoError(t, vs.Validate(vars))
	})

	t.Run("rejects integer overridden with fractional float", func(t *testing.T) {
		t.Parallel()
		vs := pipeline.VariantSet{"v": {"limit": 1.5}}
		err := vs.Validate(vars)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected integer")
	})

	t.Run("accepts number overridden with int", func(t *testing.T) {
		t.Parallel()
		vs := pipeline.VariantSet{"v": {"ratio": 2}}
		require.NoError(t, vs.Validate(vars))
	})

	t.Run("accepts number overridden with float", func(t *testing.T) {
		t.Parallel()
		vs := pipeline.VariantSet{"v": {"ratio": 3.14}}
		require.NoError(t, vs.Validate(vars))
	})

	t.Run("accepts null variable overridden with nil", func(t *testing.T) {
		t.Parallel()
		vs := pipeline.VariantSet{"v": {"nullable": nil}}
		require.NoError(t, vs.Validate(vars))
	})

	t.Run("rejects null variable overridden with non-nil", func(t *testing.T) {
		t.Parallel()
		vs := pipeline.VariantSet{"v": {"nullable": "not null"}}
		err := vs.Validate(vars)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected null")
	})

	t.Run("untyped variable accepts any value", func(t *testing.T) {
		t.Parallel()
		vs := pipeline.VariantSet{
			"v1": {"untyped": "string"},
			"v2": {"untyped": 42},
			"v3": {"untyped": true},
		}
		require.NoError(t, vs.Validate(vars))
	})

	t.Run("error message mentions variant and variable names", func(t *testing.T) {
		t.Parallel()
		vs := pipeline.VariantSet{"client_alpha": {"limit": "bad"}}
		err := vs.Validate(vars)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `variant "client_alpha"`)
		assert.Contains(t, err.Error(), `variable "limit"`)
	})
}

func TestPipeline_MaterializeVariant(t *testing.T) {
	t.Parallel()

	build := func() *pipeline.Pipeline {
		return &pipeline.Pipeline{
			Name:      "{{ var.client }}_pipe",
			Owner:     "team-{{ var.client }}",
			Schedule:  "{{ var.schedule }}",
			StartDate: "{{ var.start_date }}",
			Variables: pipeline.Variables{
				"asset_enabled": map[string]any{"type": "boolean", "default": false},
				"client":        map[string]any{"type": "string", "default": "default_client"},
				"region":        map[string]any{"type": "string", "default": "us"},
				"schedule":      map[string]any{"type": "string", "default": "@daily"},
				"start_date":    map[string]any{"type": "string", "default": "2024-01-01"},
			},
			Variants: pipeline.VariantSet{
				"client1": {"asset_enabled": true, "client": "alpha", "schedule": "@hourly"},
				"client2": {"client": "beta", "region": "eu", "start_date": "2024-06-01"},
			},
			Assets: []*pipeline.Asset{
				{
					Name:        "{{ var.client }}_users",
					Enabled:     &pipeline.TemplatedBool{Template: "{{ var.asset_enabled }}"},
					Description: "users for {{ var.client }} in {{ var.region }}",
					Connection:  "{{ var.client }}_db",
					ExecutableFile: pipeline.ExecutableFile{
						Content: "select '{{ var.client }}' as c", // body NOT pre-rendered
					},
					Upstreams: []pipeline.Upstream{
						{Type: "asset", Value: "{{ var.client }}_raw"},
					},
					CustomChecks: []pipeline.CustomCheck{
						{Name: "{{ var.client }}_check", Query: "select 1"},
					},
				},
				{
					Name:  "{{ var.client }}_raw",
					Image: "img/{{ var.client }}",
				},
			},
		}
	}

	t.Run("renders pipeline + asset string fields", func(t *testing.T) {
		t.Parallel()
		pl := build()
		err := pl.MaterializeVariant("client1", makeFakeRenderer)
		require.NoError(t, err)

		assert.Equal(t, "alpha_pipe", pl.Name)
		assert.Equal(t, "team-alpha", pl.Owner)
		assert.Equal(t, pipeline.Schedule("@hourly"), pl.Schedule)
		assert.Equal(t, "2024-01-01", pl.StartDate)
		assert.Equal(t, "alpha_users", pl.Assets[0].Name)
		assert.Equal(t, "alpha_raw", pl.Assets[1].Name)
		assert.Equal(t, "users for alpha in us", pl.Assets[0].Description)
		assert.Equal(t, "alpha_db", pl.Assets[0].Connection)
		assert.Equal(t, "alpha_raw", pl.Assets[0].Upstreams[0].Value)
		assert.Equal(t, "alpha_check", pl.Assets[0].CustomChecks[0].Name)
		assert.Equal(t, "img/alpha", pl.Assets[1].Image)
		assert.True(t, pl.Assets[0].IsEnabled())
		assert.Empty(t, pl.Assets[0].Enabled.Template)
	})

	t.Run("variant overrides apply to schedule and start_date", func(t *testing.T) {
		t.Parallel()
		pl := build()
		require.NoError(t, pl.MaterializeVariant("client2", makeFakeRenderer))
		assert.Equal(t, pipeline.Schedule("@daily"), pl.Schedule) // variant didn't override schedule, default kept
		assert.Equal(t, "2024-06-01", pl.StartDate)               // variant overrode start_date
		assert.False(t, pl.Assets[0].IsEnabled())                 // default enabled variable kept
	})

	t.Run("does NOT render asset bodies", func(t *testing.T) {
		t.Parallel()
		pl := build()
		require.NoError(t, pl.MaterializeVariant("client1", makeFakeRenderer))
		assert.Equal(t, "select '{{ var.client }}' as c", pl.Assets[0].ExecutableFile.Content)
	})

	t.Run("does NOT render runtime surfaces", func(t *testing.T) {
		t.Parallel()
		// Variant materialization runs with only `var` and `variant` in scope.
		// Anything that may reference runtime variables (start_date, end_date,
		// this, …) — parameter values, hook queries, custom check queries — must
		// be left untouched so the per-asset renderer can resolve them at
		// execution time with the full Jinja context. Variant variable values
		// still flow through because ApplyVariantVariables merges them into
		// pl.Variables, so `{{ var.client }}` inside a parameter resolves
		// correctly at run time.
		pl := build()
		pl.DefaultValues = &pipeline.DefaultValues{
			Parameters: pipeline.ParameterMap{"region": "{{ var.region }}"},
			Hooks: pipeline.Hooks{
				Pre:  []pipeline.Hook{{Query: "select '{{ start_date }}'"}},
				Post: []pipeline.Hook{{Query: "select '{{ end_date }}'"}},
			},
		}
		pl.Assets[0].Parameters = pipeline.ParameterMap{
			"database": "{{ var.client }}_db",
			"query":    "select '{{ start_date }}'",
		}
		pl.Assets[0].CustomChecks[0].Query = "select '{{ end_date }}'"
		pl.Assets[0].Hooks = pipeline.Hooks{
			Pre:  []pipeline.Hook{{Query: "select '{{ start_datetime }}'"}},
			Post: []pipeline.Hook{{Query: "select '{{ end_datetime }}'"}},
		}

		require.NoError(t, pl.MaterializeVariant("client1", makeFakeRenderer))

		assert.Equal(t, "{{ var.client }}_db", pl.Assets[0].Parameters["database"])
		assert.Equal(t, "select '{{ start_date }}'", pl.Assets[0].Parameters["query"])
		assert.Equal(t, "{{ var.region }}", pl.DefaultValues.Parameters["region"])
		assert.Equal(t, "select '{{ end_date }}'", pl.Assets[0].CustomChecks[0].Query)
		assert.Equal(t, "select '{{ start_datetime }}'", pl.Assets[0].Hooks.Pre[0].Query)
		assert.Equal(t, "select '{{ end_datetime }}'", pl.Assets[0].Hooks.Post[0].Query)
		assert.Equal(t, "select '{{ start_date }}'", pl.DefaultValues.Hooks.Pre[0].Query)
		assert.Equal(t, "select '{{ end_date }}'", pl.DefaultValues.Hooks.Post[0].Query)
	})

	t.Run("renders default secrets", func(t *testing.T) {
		t.Parallel()
		var pl pipeline.Pipeline
		require.NoError(t, yaml.Unmarshal([]byte(`
name: p
default:
  secrets:
    - key: "{{ var.secret_name }}"
      inject_as: "{{ var.inject_as }}"
variables:
  secret_name:
    type: string
    default: raw_secret
  inject_as:
    type: string
    default: RAW_SECRET
variants:
  only:
    secret_name: rendered_secret
    inject_as: RENDERED_SECRET
`), &pl))

		require.NoError(t, pl.MaterializeVariant("only", makeFakeRenderer))

		require.Len(t, pl.DefaultValues.Secrets, 1)
		assert.Equal(t, "rendered_secret", pl.DefaultValues.Secrets[0].SecretKey)
		assert.Equal(t, "RENDERED_SECRET", pl.DefaultValues.Secrets[0].InjectedKey)
	})

	t.Run("merges variant overrides into Variables", func(t *testing.T) {
		t.Parallel()
		pl := build()
		require.NoError(t, pl.MaterializeVariant("client2", makeFakeRenderer))
		assert.Equal(t, "beta_pipe", pl.Name)
		assert.Equal(t, "users for beta in eu", pl.Assets[0].Description)
	})

	t.Run("rebuilds tasksByName so GetAssetByName uses rendered names", func(t *testing.T) {
		t.Parallel()
		pl := build()
		require.NoError(t, pl.MaterializeVariant("client1", makeFakeRenderer))
		assert.NotNil(t, pl.GetAssetByName("alpha_users"))
		assert.NotNil(t, pl.GetAssetByName("alpha_raw"))
		assert.Nil(t, pl.GetAssetByName("{{ var.client }}_users"))
	})

	t.Run("errors on unknown variant", func(t *testing.T) {
		t.Parallel()
		pl := build()
		err := pl.MaterializeVariant("nope", makeFakeRenderer)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown variant")
		assert.Contains(t, err.Error(), "client1")
	})

	t.Run("errors when pipeline has no variants", func(t *testing.T) {
		t.Parallel()
		pl := &pipeline.Pipeline{Name: "x", Variables: pipeline.Variables{}}
		err := pl.MaterializeVariant("v1", makeFakeRenderer)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no variants")
	})

	t.Run("errors when variant name is empty", func(t *testing.T) {
		t.Parallel()
		pl := build()
		err := pl.MaterializeVariant("", makeFakeRenderer)
		require.Error(t, err)
	})

	t.Run("errors when enabled renders to non-boolean", func(t *testing.T) {
		t.Parallel()
		pl := build()
		pl.Variables["asset_enabled"] = map[string]any{"type": "string", "default": "not-a-bool"}
		pl.Variants["client1"]["asset_enabled"] = "not-a-bool"
		err := pl.MaterializeVariant("client1", makeFakeRenderer)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "asset[{{ var.client }}_users].enabled")
		assert.Contains(t, err.Error(), "expected boolean")
	})
}

// TestVariantVisitorCoversStringFields guards against silently regressing the
// hand-written visitor in pkg/pipeline/variant.go. The test populates every
// reachable string-typed field on a Pipeline + Asset with a sentinel template,
// runs MaterializeVariant, then walks the result and fails on any sentinel that
// survived rendering. Fields that are intentionally NOT rendered (asset bodies,
// internal IDs, enum-like values, file metadata, etc.) belong in skipFields.
//
// When this test fails after adding a new exported string field:
//   - if the field SHOULD be variant-templated, extend the visitor in variant.go
//   - if the field should NOT be templated, add its path to skipFields below
func TestVariantVisitorCoversStringFields(t *testing.T) {
	t.Parallel()

	// skipFields lists field paths that are intentionally not rendered by
	// MaterializeVariant. Paths are produced by the walker rooted at "Pipeline"
	// using "[]" for slice/map elements (e.g. "Pipeline.Assets[].Upstreams[].Type").
	skipFields := map[string]bool{
		// Pipeline metadata set internally or validated against an enum.
		"Pipeline.LegacyID":            true,
		"Pipeline.Catchup":             true,
		"Pipeline.MacrosPath":          true,
		"Pipeline.Commit":              true,
		"Pipeline.Snapshot":            true,
		"Pipeline.SelectedVariant":     true,
		"Pipeline.DefinitionFile.Name": true,
		"Pipeline.DefinitionFile.Path": true,
		"Pipeline.Macros[]":            true, // macro bodies are run-time Jinja
		// Variables / Variants are config inputs to the renderer, not data.
		"Pipeline.Variables[]": true,
		"Pipeline.Variants[]":  true,
		// Notifications: channel/connection strings are deployment-bound config.
		"Pipeline.Notifications.Slack[].Channel":      true,
		"Pipeline.Notifications.MSTeams[].Connection": true,
		"Pipeline.Notifications.Discord[].Connection": true,
		"Pipeline.Notifications.Webhook[].Connection": true,

		// Asset internals + file metadata — never user-templated.
		"Pipeline.Assets[].ID":                     true,
		"Pipeline.Assets[].Type":                   true,
		"Pipeline.Assets[].ExecutableFile.Name":    true,
		"Pipeline.Assets[].ExecutableFile.Path":    true,
		"Pipeline.Assets[].ExecutableFile.Content": true, // body Jinja runs at execution time
		"Pipeline.Assets[].DefinitionFile.Name":    true,
		"Pipeline.Assets[].DefinitionFile.Path":    true,
		"Pipeline.Assets[].DefinitionFile.Type":    true,
		"Pipeline.Assets[].Hooks.Pre[].Query":      true,
		"Pipeline.Assets[].Hooks.Post[].Query":     true,

		// Enum-typed strings on Asset.
		"Pipeline.Assets[].Materialization.Type":                 true,
		"Pipeline.Assets[].Materialization.Strategy":             true,
		"Pipeline.Assets[].Materialization.TimeGranularity":      true,
		"Pipeline.Assets[].Upstreams[].Type":                     true,
		"Pipeline.Assets[].Upstreams[].Mode":                     true,
		"Pipeline.DefaultValues.Materialization.Type":            true,
		"Pipeline.DefaultValues.Materialization.Strategy":        true,
		"Pipeline.DefaultValues.Materialization.TimeGranularity": true,
		"Pipeline.DefaultValues.Upstreams[].Type":                true,
		"Pipeline.DefaultValues.Upstreams[].Mode":                true,

		// Column structural fields (parse-time linkage, lineage tracking).
		"Pipeline.Assets[].Columns[].EntityAttribute.Entity":         true,
		"Pipeline.Assets[].Columns[].EntityAttribute.Attribute":      true,
		"Pipeline.Assets[].Columns[].Extends":                        true,
		"Pipeline.Assets[].Columns[].Upstreams[].Column":             true,
		"Pipeline.Assets[].Columns[].Upstreams[].Table":              true,
		"Pipeline.Assets[].Columns[].Checks[].ID":                    true,
		"Pipeline.Assets[].CustomChecks[].ID":                        true,
		"Pipeline.Assets[].CustomChecks[].Query":                     true,
		"Pipeline.DefaultValues.Columns[].EntityAttribute.Entity":    true,
		"Pipeline.DefaultValues.Columns[].EntityAttribute.Attribute": true,
		"Pipeline.DefaultValues.Columns[].Extends":                   true,
		"Pipeline.DefaultValues.Columns[].Upstreams[].Column":        true,
		"Pipeline.DefaultValues.Columns[].Upstreams[].Table":         true,
		"Pipeline.DefaultValues.Columns[].Checks[].ID":               true,
		"Pipeline.DefaultValues.CustomChecks[].ID":                   true,
		"Pipeline.DefaultValues.CustomChecks[].Query":                true,
		"Pipeline.DefaultValues.Hooks.Pre[].Query":                   true,
		"Pipeline.DefaultValues.Hooks.Post[].Query":                  true,

		// Parameter values frequently embed runtime variables (e.g. {{ start_date }})
		// and are resolved at execution time by the per-asset renderer.
		"Pipeline.Assets[].Parameters[]":      true,
		"Pipeline.DefaultValues.Parameters[]": true,

		// Interval modifier templates have a dedicated run-time resolver.
		"Pipeline.Assets[].IntervalModifiers.Start.Template":      true,
		"Pipeline.Assets[].IntervalModifiers.End.Template":        true,
		"Pipeline.DefaultValues.IntervalModifiers.Start.Template": true,
		"Pipeline.DefaultValues.IntervalModifiers.End.Template":   true,

		// Asset-level notifications mirror pipeline notifications.
		"Pipeline.Assets[].Notifications.Slack[].Channel":           true,
		"Pipeline.Assets[].Notifications.MSTeams[].Connection":      true,
		"Pipeline.Assets[].Notifications.Discord[].Connection":      true,
		"Pipeline.Assets[].Notifications.Webhook[].Connection":      true,
		"Pipeline.DefaultValues.Notifications.Slack[].Channel":      true,
		"Pipeline.DefaultValues.Notifications.MSTeams[].Connection": true,
		"Pipeline.DefaultValues.Notifications.Discord[].Connection": true,
		"Pipeline.DefaultValues.Notifications.Webhook[].Connection": true,
	}

	pl := buildFullyPopulatedPipelineForVisitorTest()

	// Phase 1: stamp the sentinel into every settable string field that isn't
	// in the skip list.
	stampSentinels(t, reflect.ValueOf(pl).Elem(), "Pipeline", skipFields, sentinelTemplate)

	// Phase 2: materialize. fakeRender resolves {{ var.sentinel }} → RENDERED.
	require.NoError(t, pl.MaterializeVariant("only", makeFakeRenderer))

	// Phase 3: walk again and collect any field whose value still carries the sentinel.
	var leftovers []string
	collectSentinels(reflect.ValueOf(pl).Elem(), "Pipeline", skipFields, sentinelMarker, &leftovers)
	if len(leftovers) > 0 {
		t.Errorf("variant visitor missed these string fields (extend renderPipelineStrings/renderAssetStrings in pkg/pipeline/variant.go, or add to skipFields if the field should never be templated):\n  %s", strings.Join(leftovers, "\n  "))
	}
}

const (
	sentinelTemplate = "{{ var.sentinel }}"
	sentinelMarker   = "{{ var.sentinel }}"
)

// buildFullyPopulatedPipelineForVisitorTest returns a Pipeline whose every
// slice and map has at least one element, so the reflection walker has
// something to recurse into. Field values themselves are arbitrary; the
// walker overwrites them with the sentinel.
func buildFullyPopulatedPipelineForVisitorTest() *pipeline.Pipeline {
	asset := &pipeline.Asset{
		Name:    "x",
		Tags:    []string{"a"},
		Domains: []string{"a"},
		Meta:    map[string]string{"k": "v"},
		Materialization: pipeline.Materialization{
			ClusterBy: []string{"a"},
		},
		Upstreams: []pipeline.Upstream{
			{Type: "asset", Value: "x", Metadata: map[string]string{"k": "v"}, Columns: []pipeline.DependsColumn{{Name: "n", Usage: "u"}}},
		},
		Parameters: pipeline.ParameterMap{"k": "v"},
		Secrets:    []pipeline.SecretMapping{{SecretKey: "k", InjectedKey: "i"}},
		Extends:    []string{"x"},
		Columns: []pipeline.Column{
			{
				Name:    "c",
				Tags:    []string{"a"},
				Domains: []string{"a"},
				Meta:    map[string]string{"k": "v"},
				Checks:  []pipeline.ColumnCheck{{Name: "ck"}},
			},
		},
		CustomChecks: []pipeline.CustomCheck{{Name: "cc", Query: "q"}},
		Hooks: pipeline.Hooks{
			Pre:  []pipeline.Hook{{Query: "q"}},
			Post: []pipeline.Hook{{Query: "q"}},
		},
		Metadata: map[string]string{"k": "v"},
		Routing:  &pipeline.RoutingConfig{EgressGateway: "gw"},
	}
	return &pipeline.Pipeline{
		Name:               "p",
		Tags:               []string{"a"},
		Domains:            []string{"a"},
		Meta:               map[string]string{"k": "v"},
		DefaultConnections: map[string]string{"k": "v"},
		DefaultValues: &pipeline.DefaultValues{
			Type:        "type",
			Description: "desc",
			StartDate:   "start",
			Connection:  "conn",
			Tags:        []string{"a"},
			Domains:     []string{"a"},
			Meta:        map[string]string{"k": "v"},
			Materialization: pipeline.Materialization{
				ClusterBy:      []string{"a"},
				PartitionBy:    "p",
				IncrementalKey: "i",
			},
			Upstreams: []pipeline.Upstream{
				{Type: "asset", Value: "x", Metadata: map[string]string{"k": "v"}, Columns: []pipeline.DependsColumn{{Name: "n", Usage: "u"}}},
			},
			Image:      "image",
			Instance:   "instance",
			Owner:      "owner",
			Parameters: pipeline.ParameterMap{"k": "v"},
			Extends:    []string{"x"},
			Columns: []pipeline.Column{
				{
					Name:    "c",
					Tags:    []string{"a"},
					Domains: []string{"a"},
					Meta:    map[string]string{"k": "v"},
					Checks:  []pipeline.ColumnCheck{{Name: "ck"}},
				},
			},
			CustomChecks: []pipeline.CustomCheck{{Name: "cc", Query: "q"}},
			Hooks: pipeline.Hooks{
				Pre:  []pipeline.Hook{{Query: "q"}},
				Post: []pipeline.Hook{{Query: "q"}},
			},
			Metadata:  map[string]string{"k": "v"},
			Snowflake: pipeline.SnowflakeConfig{Warehouse: "wh"},
			Athena:    pipeline.AthenaConfig{Location: "loc"},
			Routing:   &pipeline.RoutingConfig{EgressGateway: "gw"},
			Notifications: &pipeline.Notifications{
				Slack:   []pipeline.SlackNotification{{Channel: "c"}},
				MSTeams: []pipeline.MSTeamsNotification{{Connection: "c"}},
				Discord: []pipeline.DiscordNotification{{Connection: "c"}},
				Webhook: []pipeline.WebhookNotification{{Connection: "c"}},
			},
		},
		Variables: pipeline.Variables{
			"sentinel": map[string]any{"type": "string", "default": "RENDERED"},
		},
		Variants: pipeline.VariantSet{
			"only": {"sentinel": "RENDERED"},
		},
		Assets: []*pipeline.Asset{asset},
	}
}

// stampSentinels walks v and writes sentinel into every string field that is
// settable and whose path is not in skip.
func stampSentinels(t *testing.T, v reflect.Value, path string, skip map[string]bool, sentinel string) {
	t.Helper()
	switch v.Kind() { //nolint:exhaustive // only the listed kinds carry strings.
	case reflect.Pointer, reflect.Interface:
		if !v.IsNil() {
			stampSentinels(t, v.Elem(), path, skip, sentinel)
		}
	case reflect.Struct:
		typ := v.Type()
		for i := range v.NumField() {
			sf := typ.Field(i)
			if !sf.IsExported() {
				continue
			}
			stampSentinels(t, v.Field(i), path+"."+sf.Name, skip, sentinel)
		}
	case reflect.Slice, reflect.Array:
		for i := range v.Len() {
			stampSentinels(t, v.Index(i), path+"[]", skip, sentinel)
		}
	case reflect.Map:
		// We only stamp string-valued maps; anything more complex is config
		// (e.g. Variables / Variants) and lives in the skip list.
		if skip[path+"[]"] {
			return
		}
		if v.Type().Elem().Kind() != reflect.String {
			return
		}
		iter := v.MapRange()
		for iter.Next() {
			v.SetMapIndex(iter.Key(), reflect.ValueOf(sentinel).Convert(v.Type().Elem()))
		}
	case reflect.String:
		if skip[path] || !v.CanSet() {
			return
		}
		v.SetString(sentinel)
	}
}

func collectSentinels(v reflect.Value, path string, skip map[string]bool, marker string, out *[]string) {
	switch v.Kind() { //nolint:exhaustive // only the listed kinds carry strings.
	case reflect.Pointer, reflect.Interface:
		if !v.IsNil() {
			collectSentinels(v.Elem(), path, skip, marker, out)
		}
	case reflect.Struct:
		typ := v.Type()
		for i := range v.NumField() {
			sf := typ.Field(i)
			if !sf.IsExported() {
				continue
			}
			collectSentinels(v.Field(i), path+"."+sf.Name, skip, marker, out)
		}
	case reflect.Slice, reflect.Array:
		for i := range v.Len() {
			collectSentinels(v.Index(i), path+"[]", skip, marker, out)
		}
	case reflect.Map:
		if skip[path+"[]"] {
			return
		}
		if v.Type().Elem().Kind() != reflect.String {
			return
		}
		iter := v.MapRange()
		for iter.Next() {
			if strings.Contains(iter.Value().String(), marker) {
				*out = append(*out, fmt.Sprintf("%s[]=%s", path, iter.Value().String()))
			}
		}
	case reflect.String:
		if skip[path] {
			return
		}
		if strings.Contains(v.String(), marker) {
			*out = append(*out, fmt.Sprintf("%s=%s", path, v.String()))
		}
	}
}
