package sumble

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		want    string
		wantErr bool
	}{
		{
			name:   "valid api key",
			config: Config{APIKey: "key-123"},
			want:   "sumble://?api_key=key-123",
		},
		{
			name:   "api key is trimmed",
			config: Config{APIKey: "  key-123  "},
			want:   "sumble://?api_key=key-123",
		},
		{
			name:   "special characters are escaped",
			config: Config{APIKey: "a b&c"},
			want:   "sumble://?api_key=a+b%26c",
		},
		{
			name:    "missing api key",
			config:  Config{},
			wantErr: true,
		},
		{
			name:    "blank api key",
			config:  Config{APIKey: "   "},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.config.GetIngestrURI()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
