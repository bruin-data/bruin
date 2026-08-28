package twocheckout

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
			name:   "required fields set",
			config: Config{MerchantCode: "MERCHANT123", SecretKey: "secret"},
			want:   "twocheckout://?merchant_code=MERCHANT123&secret_key=secret",
		},
		{
			name:   "with base_url",
			config: Config{MerchantCode: "MERCHANT123", SecretKey: "secret", BaseURL: "https://api.example.com"},
			want:   "twocheckout://?base_url=https%3A%2F%2Fapi.example.com&merchant_code=MERCHANT123&secret_key=secret",
		},
		{
			name:    "missing merchant_code",
			config:  Config{SecretKey: "secret"},
			wantErr: true,
		},
		{
			name:    "missing secret_key",
			config:  Config{MerchantCode: "MERCHANT123"},
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

	client, err := NewClient(Config{MerchantCode: "MERCHANT123", SecretKey: "secret"})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "twocheckout://?merchant_code=MERCHANT123&secret_key=secret", uri)
}
