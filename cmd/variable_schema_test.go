package cmd

import (
	"sync/atomic"
	"testing"

	"github.com/bruin-data/bruin/pkg/lint"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestVariableSchemaWarningsDoNotFailValidation(t *testing.T) {
	t.Parallel()
	rules, err := lint.GetRules(afero.NewMemMapFs(), nil, false, nil, false)
	require.NoError(t, err)
	var schemaRule lint.Rule
	for _, rule := range rules {
		if rule.Name() == "valid-variable-schemas" {
			schemaRule = rule
			break
		}
	}
	require.NotNil(t, schemaRule)
	for name, definition := range map[string]map[string]any{
		"invalid schema":  {"type": "invalid", "default": 1},
		"invalid default": {"type": "integer", "default": "text"},
		"legacy schema":   {"type": "array", "items": map[string]any{"type": "int"}, "default": []int{1, 7}},
		"reference":       {"$ref": "#/properties/value", "default": 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := &pipeline.Pipeline{Name: "test", Variables: pipeline.Variables{"value": definition}}
			issues, err := schemaRule.Validate(t.Context(), p)
			require.NoError(t, err)
			require.NotEmpty(t, issues)
			result := &lint.PipelineAnalysisResult{Pipelines: []*lint.PipelineIssues{{
				Pipeline: p, Issues: map[lint.Rule][]*lint.Issue{schemaRule: issues},
			}}}
			assert.Zero(t, result.ErrorCount())
			assert.Positive(t, result.WarningCount())
			require.NoError(t, reportLintErrors(result, nil, lint.Printer{}, ""))
		})
	}
}

func TestCheckLintDoesNotEnforceVariableSchemas(t *testing.T) {
	t.Parallel()
	var schemaLoads atomic.Int32
	p := &pipeline.Pipeline{Name: "test", Concurrency: 1, Variables: pipeline.Variables{
		"legacy":  {"type": "array", "items": map[string]any{"type": "int"}, "default": []int{1, 7}},
		"invalid": {"type": "invalid", "default": 1},
		"count":   {"type": "integer", "default": "text"},
		"probe":   {"type": variableSchemaLoadProbe{calls: &schemaLoads}, "default": 1},
	}}
	require.NoError(t, p.Variables.Merge(map[string]any{"legacy": []int{3, 7}, "count": "override"}))
	require.NoError(t, CheckLint(t.Context(), p, "test", zap.NewNop().Sugar(), false))
	assert.Zero(t, schemaLoads.Load(), "live lint must not load schemas, even for warnings")
	assert.Equal(t, "override", p.Variables.Value()["count"])
}

type variableSchemaLoadProbe struct {
	calls *atomic.Int32
}

func (p variableSchemaLoadProbe) MarshalJSON() ([]byte, error) {
	p.calls.Add(1)
	return []byte(`"integer"`), nil
}
