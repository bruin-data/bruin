package deel

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
			name:   "api key only",
			config: Config{APIKey: "deel_abc123"},
			want:   "deel://?api_key=deel_abc123",
		},
		{
			name:   "sandbox environment",
			config: Config{APIKey: "deel_abc123", Environment: "sandbox"},
			want:   "deel://?api_key=deel_abc123&environment=sandbox",
		},
		{
			name:   "production environment",
			config: Config{APIKey: "deel_abc123", Environment: "production"},
			want:   "deel://?api_key=deel_abc123&environment=production",
		},
		{
			name:   "values are trimmed",
			config: Config{APIKey: "  deel_abc123 ", Environment: " sandbox "},
			want:   "deel://?api_key=deel_abc123&environment=sandbox",
		},
		{
			name:   "api key with reserved characters is escaped",
			config: Config{APIKey: "a b&c"},
			want:   "deel://?api_key=a+b%26c",
		},
		{
			name:    "missing api key",
			config:  Config{Environment: "sandbox"},
			wantErr: true,
		},
		{
			name:    "invalid environment",
			config:  Config{APIKey: "deel_abc123", Environment: "staging"},
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

	client, err := NewClient(Config{APIKey: "deel_abc123"})
	require.NoError(t, err)
	require.NotNil(t, client)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "deel://?api_key=deel_abc123", uri)
}
