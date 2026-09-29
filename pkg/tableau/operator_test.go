package tableau

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

type testConnectionGetter func(string) any

func (get testConnectionGetter) GetConnection(name string) any { return get(name) }

func TestRunTaskConnectionRequirements(t *testing.T) {
	t.Parallel()

	for _, assetType := range []pipeline.AssetType{
		pipeline.AssetTypeTableau, pipeline.AssetTypeTableauDatasource, pipeline.AssetTypeTableauWorkbook,
		pipeline.AssetTypeTableauWorksheet, pipeline.AssetTypeTableauDashboard,
	} {
		for _, fullRefresh := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/full-refresh=%t", assetType, fullRefresh), func(t *testing.T) {
				t.Parallel()
				ctx := context.WithValue(t.Context(), pipeline.RunConfigFullRefresh, fullRefresh)
				p := &pipeline.Pipeline{}
				asset := &pipeline.Asset{Name: "tableau", Type: assetType}
				// A nil getter proves no connection lookup occurs for a no-op.
				op := NewBasicOperator(nil)
				require.NoError(t, op.RunTask(ctx, p, asset))
				asset.Connection = "unavailable"
				asset.Parameters = pipeline.ParameterMap{"refresh": "false"}
				require.NoError(t, op.RunTask(ctx, p, asset))

				asset.Parameters["refresh"] = "true"
				if assetType == pipeline.AssetTypeTableauWorksheet || assetType == pipeline.AssetTypeTableauDashboard {
					require.NoError(t, op.RunTask(ctx, p, asset))
					return
				}
				asset.Connection = ""
				require.ErrorContains(t, op.RunTask(ctx, p, asset), "no connection mapping found")
				asset.Connection = "tableau-prod"
				op = NewBasicOperator(testConnectionGetter(func(name string) any {
					require.Equal(t, "tableau-prod", name)
					return nil
				}))
				require.Error(t, op.RunTask(ctx, p, asset))
				op = NewBasicOperator(testConnectionGetter(func(string) any { return "wrong connection type" }))
				require.ErrorContains(t, op.RunTask(ctx, p, asset), "is not a tableau connection")
				op = NewBasicOperator(testConnectionGetter(func(string) any { return &Client{} }))
				require.ErrorContains(t, op.RunTask(ctx, p, asset), "requires either")
			})
		}
	}
}

type incrementalTestCase struct {
	name       string
	ctxFunc    func(t *testing.T) context.Context
	parameters pipeline.ParameterMap
	want       bool
}

func TestResolveIncrementalRefresh(t *testing.T) {
	t.Parallel()

	tests := []incrementalTestCase{
		{
			name:       "defaults to incremental",
			parameters: pipeline.ParameterMap{},
			want:       true,
			ctxFunc:    func(t *testing.T) context.Context { return t.Context() },
		},
		{
			name: "uses explicit incremental true",
			parameters: pipeline.ParameterMap{
				"incremental": "true",
			},
			want:    true,
			ctxFunc: func(t *testing.T) context.Context { return t.Context() },
		},
		{
			name: "uses explicit incremental false",
			parameters: pipeline.ParameterMap{
				"incremental": "false",
			},
			want:    false,
			ctxFunc: func(t *testing.T) context.Context { return t.Context() },
		},
		{
			name: "run full refresh overrides incremental true",
			parameters: pipeline.ParameterMap{
				"incremental": "true",
			},
			want: false,
			ctxFunc: func(t *testing.T) context.Context {
				return context.WithValue(t.Context(), pipeline.RunConfigFullRefresh, true)
			},
		},
		{
			name: "invalid incremental falls back to default",
			parameters: pipeline.ParameterMap{
				"incremental": "maybe",
			},
			want:    true,
			ctxFunc: func(t *testing.T) context.Context { return t.Context() },
		},
		{
			name: "empty incremental falls back to default",
			parameters: pipeline.ParameterMap{
				"incremental": " ",
			},
			want:    true,
			ctxFunc: func(t *testing.T) context.Context { return t.Context() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, resolveIncrementalRefresh(tt.ctxFunc(t), tt.parameters))
		})
	}
}

func TestResolveRefreshTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		parameters pipeline.ParameterMap
		want       time.Duration
	}{
		{
			name:       "defaults to sixty minutes",
			parameters: pipeline.ParameterMap{},
			want:       60 * time.Minute,
		},
		{
			name: "uses explicit timeout value",
			parameters: pipeline.ParameterMap{
				"refresh_timeout_minutes": "90",
			},
			want: 90 * time.Minute,
		},
		{
			name: "invalid timeout falls back to default",
			parameters: pipeline.ParameterMap{
				"refresh_timeout_minutes": "abc",
			},
			want: 60 * time.Minute,
		},
		{
			name: "non-positive timeout falls back to default",
			parameters: pipeline.ParameterMap{
				"refresh_timeout_minutes": "0",
			},
			want: 60 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, resolveRefreshTimeout(tt.parameters))
		})
	}
}
