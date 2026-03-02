package tableau

import (
	"context"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

type incrementalTestCase struct {
	name       string
	ctxFunc    func(t *testing.T) context.Context
	parameters map[string]string
	want       bool
}

func TestResolveIncrementalRefresh(t *testing.T) {
	t.Parallel()

	tests := []incrementalTestCase{
		{
			name:       "defaults to incremental",
			parameters: map[string]string{},
			want:       true,
			ctxFunc:    func(t *testing.T) context.Context { return t.Context() },
		},
		{
			name: "uses explicit incremental true",
			parameters: map[string]string{
				"incremental": "true",
			},
			want:    true,
			ctxFunc: func(t *testing.T) context.Context { return t.Context() },
		},
		{
			name: "uses explicit incremental false",
			parameters: map[string]string{
				"incremental": "false",
			},
			want:    false,
			ctxFunc: func(t *testing.T) context.Context { return t.Context() },
		},
		{
			name: "run full refresh overrides incremental true",
			parameters: map[string]string{
				"incremental": "true",
			},
			want: false,
			ctxFunc: func(t *testing.T) context.Context {
				return context.WithValue(t.Context(), pipeline.RunConfigFullRefresh, true)
			},
		},
		{
			name: "invalid incremental falls back to default",
			parameters: map[string]string{
				"incremental": "maybe",
			},
			want:    true,
			ctxFunc: func(t *testing.T) context.Context { return t.Context() },
		},
		{
			name: "empty incremental falls back to default",
			parameters: map[string]string{
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
		parameters map[string]string
		want       time.Duration
	}{
		{
			name:       "defaults to sixty minutes",
			parameters: map[string]string{},
			want:       60 * time.Minute,
		},
		{
			name: "uses explicit timeout value",
			parameters: map[string]string{
				"refresh_timeout_minutes": "90",
			},
			want: 90 * time.Minute,
		},
		{
			name: "invalid timeout falls back to default",
			parameters: map[string]string{
				"refresh_timeout_minutes": "abc",
			},
			want: 60 * time.Minute,
		},
		{
			name: "non-positive timeout falls back to default",
			parameters: map[string]string{
				"refresh_timeout_minutes": "0",
			},
			want: 60 * time.Minute,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, resolveRefreshTimeout(tt.parameters))
		})
	}
}
