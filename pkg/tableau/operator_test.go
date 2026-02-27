package tableau

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveIncrementalRefresh(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		parameters map[string]string
		want       bool
	}{
		{
			name:       "defaults to incremental",
			parameters: map[string]string{},
			want:       true,
		},
		{
			name: "uses explicit incremental true",
			parameters: map[string]string{
				"incremental": "true",
			},
			want: true,
		},
		{
			name: "uses explicit incremental false",
			parameters: map[string]string{
				"incremental": "false",
			},
			want: false,
		},
		{
			name: "full refresh overrides incremental true",
			parameters: map[string]string{
				"incremental": "true",
				"full_refresh": "true",
			},
			want: false,
		},
		{
			name: "invalid incremental falls back to default",
			parameters: map[string]string{
				"incremental": "maybe",
			},
			want: true,
		},
		{
			name: "empty incremental falls back to default",
			parameters: map[string]string{
				"incremental": " ",
			},
			want: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, resolveIncrementalRefresh(tt.parameters))
		})
	}
}
