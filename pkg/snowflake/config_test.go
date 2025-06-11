package snowflake

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/require"
)

func TestConfig_DSN(t *testing.T) {
	t.Parallel()

	type fields struct {
		Account  string
		Username string
		Password string
		Region   string
	}
	tests := []struct {
		name    string
		fields  fields
		want    *gosnowflake.Config
		wantErr bool
	}{
		{
			name: "some basic case with no region",
			fields: fields{
				Account:  "my-account",
				Username: "my-user",
				Password: "qwerty123",
				Region:   "us-east-1",
			},
			want: &gosnowflake.Config{
				Account:  "my-account",
				User:     "my-user",
				Password: "qwerty123",
				Region:   "us-east-1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := Config{
				Account:  tt.fields.Account,
				Username: tt.fields.Username,
				Password: tt.fields.Password,
				Region:   tt.fields.Region,
			}
			got, err := c.DSN()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			wantDsn, err := gosnowflake.DSN(tt.want)
			require.NoError(t, err)
			require.Equal(t, wantDsn, got)
		})
	}
}

func generatePrivateKeyPEM(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)

	keyBytes, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	var buf bytes.Buffer
	pem.Encode(&buf, &pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})
	return buf.String()
}

func TestConfig_DSN_WithPrivateKey(t *testing.T) {
	t.Parallel()

	pemKey := generatePrivateKeyPEM(t)

	c := Config{
		Account:    "my-account",
		Username:   "my-user",
		Region:     "us-east-1",
		PrivateKey: pemKey,
	}
	dsn, err := c.DSN()
	require.NoError(t, err)

	cfg, err := gosnowflake.ParseDSN(dsn)
	require.NoError(t, err)

	require.Equal(t, gosnowflake.AuthTypeJwt, cfg.Authenticator)
	require.NotNil(t, cfg.PrivateKey)
}
