package env_test

import (
	"context"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/env"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

func TestSetupVariables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupCtx    func() context.Context
		asset       *pipeline.Asset
		pipeline    *pipeline.Pipeline
		existingEnv map[string]string
		expectedEnv map[string]string
	}{
		{
			name: "with apply modifiers false",
			setupCtx: func() context.Context {
				ctx := context.Background()
				ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
				ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC))
				ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "test-run")
				ctx = context.WithValue(ctx, pipeline.RunConfigApplyIntervalModifiers, false)
				ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, true)
				return ctx
			},
			asset:       &pipeline.Asset{},
			existingEnv: map[string]string{"EXISTING": "value"},
			expectedEnv: map[string]string{
				"EXISTING":   "value",
				"BRUIN_VARS": "{}",
			},
		},
		{
			name: "with days modifier",
			setupCtx: func() context.Context {
				ctx := context.Background()
				ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
				ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC))
				ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "test-run")
				ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, true)
				return ctx
			},
			asset: &pipeline.Asset{
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{Days: 1},
					End:   pipeline.TimeModifier{Days: 0},
				},
			},
			expectedEnv: map[string]string{
				"BRUIN_START_DATE":      "2024-01-02",
				"BRUIN_START_DATETIME":  "2024-01-02T00:00:00",
				"BRUIN_START_TIMESTAMP": "2024-01-02T00:00:00.000000Z",
				"BRUIN_END_DATE":        "2024-01-02",
				"BRUIN_END_DATETIME":    "2024-01-02T00:00:00",
				"BRUIN_END_TIMESTAMP":   "2024-01-02T00:00:00.000000Z",
				"BRUIN_PIPELINE":        "test-pipeline",
				"BRUIN_RUN_ID":          "test-run",
				"BRUIN_FULL_REFRESH":    "1",
				"BRUIN_VARS":            "{}",
			},
		},
		{
			name: "with hours modifier",
			setupCtx: func() context.Context {
				ctx := context.Background()
				ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC))
				ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))
				ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "test-run")
				ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, true)
				return ctx
			},
			asset: &pipeline.Asset{
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{Hours: 2},
					End:   pipeline.TimeModifier{Hours: 0},
				},
			},
			expectedEnv: map[string]string{
				"BRUIN_START_DATE":      "2024-01-01",
				"BRUIN_START_DATETIME":  "2024-01-01T12:00:00",
				"BRUIN_START_TIMESTAMP": "2024-01-01T12:00:00.000000Z",
				"BRUIN_END_DATE":        "2024-01-01",
				"BRUIN_END_DATETIME":    "2024-01-01T12:00:00",
				"BRUIN_END_TIMESTAMP":   "2024-01-01T12:00:00.000000Z",
				"BRUIN_PIPELINE":        "test-pipeline",
				"BRUIN_RUN_ID":          "test-run",
				"BRUIN_FULL_REFRESH":    "1",
				"BRUIN_VARS":            "{}",
			},
		},
		{
			name: "with apply modifiers false 2",
			setupCtx: func() context.Context {
				ctx := context.Background()
				ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
				ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC))
				ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "test-run")
				ctx = context.WithValue(ctx, pipeline.RunConfigApplyIntervalModifiers, false)
				ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, true)
				return ctx
			},
			asset: &pipeline.Asset{
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{Days: 1},
					End:   pipeline.TimeModifier{Days: 1},
				},
			},
			existingEnv: map[string]string{
				"BRUIN_START_DATE":      "2024-01-01",
				"BRUIN_START_DATETIME":  "2024-01-01T00:00:00",
				"BRUIN_START_TIMESTAMP": "2024-01-01T00:00:00.000000Z",
				"BRUIN_END_DATE":        "2024-01-02",
				"BRUIN_END_DATETIME":    "2024-01-02T00:00:00",
				"BRUIN_END_TIMESTAMP":   "2024-01-02T00:00:00.000000Z",
				"BRUIN_PIPELINE":        "test-pipeline",
				"BRUIN_RUN_ID":          "test-run",
				"BRUIN_FULL_REFRESH":    "1",
			},
			expectedEnv: map[string]string{
				"BRUIN_START_DATE":      "2024-01-01",
				"BRUIN_START_DATETIME":  "2024-01-01T00:00:00",
				"BRUIN_START_TIMESTAMP": "2024-01-01T00:00:00.000000Z",
				"BRUIN_END_DATE":        "2024-01-02",
				"BRUIN_END_DATETIME":    "2024-01-02T00:00:00",
				"BRUIN_END_TIMESTAMP":   "2024-01-02T00:00:00.000000Z",
				"BRUIN_PIPELINE":        "test-pipeline",
				"BRUIN_RUN_ID":          "test-run",
				"BRUIN_FULL_REFRESH":    "1",
				"BRUIN_VARS":            "{}",
			},
		},
		{
			setupCtx: func() context.Context {
				ctx := context.Background()
				ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
				ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC))
				ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "test-run")
				ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, false)
				return ctx
			},
			name: "with variables",
			asset: &pipeline.Asset{
				Name: "test-asset",
			},
			pipeline: &pipeline.Pipeline{
				Name: "test-pipeline",
				Variables: pipeline.Variables{
					"env": map[string]any{
						"type":    "string",
						"default": "dev",
					},
					"users": map[string]any{
						"type":    "list",
						"default": []any{"alice", "bob", "charlie"},
					},
				},
			},
			existingEnv: map[string]string{
				"BRUIN_START_DATE":      "2024-01-01",
				"BRUIN_START_DATETIME":  "2024-01-01T00:00:00",
				"BRUIN_START_TIMESTAMP": "2024-01-01T00:00:00.000000Z",
				"BRUIN_END_DATE":        "2024-01-02",
				"BRUIN_END_DATETIME":    "2024-01-02T00:00:00",
				"BRUIN_END_TIMESTAMP":   "2024-01-02T00:00:00.000000Z",
				"BRUIN_PIPELINE":        "test-pipeline",
				"BRUIN_RUN_ID":          "test-run",
			},
			expectedEnv: map[string]string{
				"BRUIN_START_DATE":      "2024-01-01",
				"BRUIN_START_DATETIME":  "2024-01-01T00:00:00",
				"BRUIN_START_TIMESTAMP": "2024-01-01T00:00:00.000000Z",
				"BRUIN_END_DATE":        "2024-01-02",
				"BRUIN_END_DATETIME":    "2024-01-02T00:00:00",
				"BRUIN_END_TIMESTAMP":   "2024-01-02T00:00:00.000000Z",
				"BRUIN_PIPELINE":        "test-pipeline",
				"BRUIN_RUN_ID":          "test-run",
				"BRUIN_VARS":            `{"env":"dev","users":["alice","bob","charlie"]}`,
			},
		},
	}

	defaultPipeline := &pipeline.Pipeline{
		Name: "test-pipeline",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := tt.setupCtx()
			p := tt.pipeline
			if p == nil {
				p = defaultPipeline
			}
			result, err := env.SetupVariables(ctx, p, tt.asset, tt.existingEnv)
			if err != nil {
				t.Errorf("error: %v", err)
				return
			}

			t.Logf("Test case: %s", tt.name)
			t.Logf("Expected env: %+v", tt.expectedEnv)
			t.Logf("Actual result: %+v", result)

			// Check only the keys we care about
			for k, expected := range tt.expectedEnv {
				actual, exists := result[k]
				if !exists {
					t.Errorf("key %s missing from result", k)
					continue
				}
				if actual != expected {
					t.Errorf("key %s: expected '%s', got '%s'", k, expected, actual)
				}
			}
		})
	}
}
