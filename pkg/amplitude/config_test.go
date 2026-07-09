package amplitude

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
			name:   "all fields set",
			config: Config{APIKey: "key", SecretKey: "secret", Region: "eu"},
			want:   "amplitude://?api_key=key&region=eu&secret_key=secret",
		},
		{
			name:   "region defaults to us",
			config: Config{APIKey: "key", SecretKey: "secret"},
			want:   "amplitude://?api_key=key&region=us&secret_key=secret",
		},
		{
			name:    "missing api_key",
			config:  Config{SecretKey: "secret"},
			wantErr: true,
		},
		{
			name:    "missing secret_key",
			config:  Config{APIKey: "key"},
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

func TestNewClient(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{APIKey: "key", SecretKey: "secret"})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "amplitude://?api_key=key&region=us&secret_key=secret", uri)
}
