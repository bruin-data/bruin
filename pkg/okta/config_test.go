package okta

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
			config: Config{Domain: "dev-123456.okta.com", APIKey: "test-api-key"},
			want:   "okta://dev-123456.okta.com?api_key=test-api-key",
		},
		{
			name:    "missing domain",
			config:  Config{APIKey: "test-api-key"},
			wantErr: true,
		},
		{
			name:    "missing api key",
			config:  Config{Domain: "dev-123456.okta.com"},
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

	client, err := NewClient(Config{Domain: "dev-123456.okta.com", APIKey: "test-api-key"})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "okta://dev-123456.okta.com?api_key=test-api-key", uri)
}
