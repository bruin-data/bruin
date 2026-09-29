package exchangeratesapi

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
			name: "required fields only",
			config: Config{
				AccessKey: "secret-key",
			},
			want: "exchangeratesapi://?access_key=secret-key",
		},
		{
			name: "with base currency",
			config: Config{
				AccessKey: "secret-key",
				Base:      "czk",
			},
			want: "exchangeratesapi://?access_key=secret-key&base=CZK",
		},
		{
			name: "trims whitespace",
			config: Config{
				AccessKey: "  secret-key  ",
				Base:      "  usd  ",
			},
			want: "exchangeratesapi://?access_key=secret-key&base=USD",
		},
		{
			name:    "missing access key",
			config:  Config{},
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

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{AccessKey: "secret-key"})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "exchangeratesapi://?access_key=secret-key", uri)
}
