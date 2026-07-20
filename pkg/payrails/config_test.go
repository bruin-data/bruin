package payrails

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
			config: Config{ClientID: "cid", ClientSecret: "sec", CertPath: "/tmp/c.pem", KeyPath: "/tmp/c.key"},
			want:   "payrails://?cert_path=%2Ftmp%2Fc.pem&client_id=cid&client_secret=sec&key_path=%2Ftmp%2Fc.key",
		},
		{
			name:   "with optional environment and base_url",
			config: Config{ClientID: "cid", ClientSecret: "sec", CertPath: "/tmp/c.pem", KeyPath: "/tmp/c.key", Environment: "sandbox", BaseURL: "https://api.payrails.io"},
			want:   "payrails://?base_url=https%3A%2F%2Fapi.payrails.io&cert_path=%2Ftmp%2Fc.pem&client_id=cid&client_secret=sec&environment=sandbox&key_path=%2Ftmp%2Fc.key",
		},
		{
			name:   "cert and key content sent base64",
			config: Config{ClientID: "cid", ClientSecret: "sec", Cert: "cert", Key: "key"},
			want:   "payrails://?cert_base64=Y2VydA%3D%3D&client_id=cid&client_secret=sec&key_base64=a2V5",
		},
		{
			name:    "missing client_id",
			config:  Config{ClientSecret: "sec", CertPath: "/tmp/c.pem", KeyPath: "/tmp/c.key"},
			wantErr: true,
		},
		{
			name:    "missing client_secret",
			config:  Config{ClientID: "cid", CertPath: "/tmp/c.pem", KeyPath: "/tmp/c.key"},
			wantErr: true,
		},
		{
			name:    "missing cert_path",
			config:  Config{ClientID: "cid", ClientSecret: "sec", KeyPath: "/tmp/c.key"},
			wantErr: true,
		},
		{
			name:    "missing key_path",
			config:  Config{ClientID: "cid", ClientSecret: "sec", CertPath: "/tmp/c.pem"},
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

	client, err := NewClient(Config{ClientID: "cid", ClientSecret: "sec", CertPath: "/tmp/c.pem", KeyPath: "/tmp/c.key"})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "payrails://?cert_path=%2Ftmp%2Fc.pem&client_id=cid&client_secret=sec&key_path=%2Ftmp%2Fc.key", uri)
}
