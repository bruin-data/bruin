package fastspring

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
			config: Config{Username: "user", Password: "pass"},
			want:   "fastspring://?password=pass&username=user",
		},
		{
			name:    "missing username",
			config:  Config{Password: "pass"},
			wantErr: true,
		},
		{
			name:    "missing password",
			config:  Config{Username: "user"},
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

	client, err := NewClient(Config{Username: "user", Password: "pass"})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "fastspring://?password=pass&username=user", uri)
}
