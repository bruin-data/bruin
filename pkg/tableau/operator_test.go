package tableau

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestResolveIncrementalRefresh(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ctx        context.Context
		parameters map[string]string
		want       bool
	}{
		{
			name:       "defaults to incremental",
			parameters: map[string]string{},
			want:       true,
			ctx:        context.Background(),
		},
		{
			name: "uses explicit incremental true",
			parameters: map[string]string{
				"incremental": "true",
			},
			want: true,
			ctx:  context.Background(),
		},
		{
			name: "uses explicit incremental false",
			parameters: map[string]string{
				"incremental": "false",
			},
			want: false,
			ctx:  context.Background(),
		},
		{
			name: "run full refresh overrides incremental true",
			parameters: map[string]string{
				"incremental": "true",
			},
			want: false,
			ctx:  context.WithValue(context.Background(), pipeline.RunConfigFullRefresh, true),
		},
		{
			name: "invalid incremental falls back to default",
			parameters: map[string]string{
				"incremental": "maybe",
			},
			want: true,
			ctx:  context.Background(),
		},
		{
			name: "empty incremental falls back to default",
			parameters: map[string]string{
				"incremental": " ",
			},
			want: true,
			ctx:  context.Background(),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, resolveIncrementalRefresh(tt.ctx, tt.parameters))
		})
	}
}
