package snowflake

import (
	"testing"

	"github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/require"
)

func TestConfig_DSNEndpoint(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		host     string
		port     int
		wantHost string
		wantPort int
	}{
		{name: "defaults", wantHost: "my-account.eu-west-1.snowflakecomputing.com", wantPort: 443},
		{name: "host only", host: "snowflake.example.com", wantHost: "snowflake.example.com", wantPort: 443},
		{name: "port only", port: 8443, wantHost: "my-account.eu-west-1.snowflakecomputing.com", wantPort: 8443},
		{name: "host and port", host: "127.0.0.1", port: 8443, wantHost: "127.0.0.1", wantPort: 8443},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := Config{
				Account:  "my-account",
				Username: "my-user",
				Password: "test-password",
				Region:   "eu-west-1",
				Host:     tt.host,
				Port:     tt.port,
			}
			dsn, err := c.DSN()
			require.NoError(t, err)
			parsed, err := gosnowflake.ParseDSN(dsn)
			require.NoError(t, err)
			require.Equal(t, tt.wantHost, parsed.Host)
			require.Equal(t, tt.wantPort, parsed.Port)
			require.Equal(t, "my-account", parsed.Account)
			require.Equal(t, "eu-west-1", parsed.Region)
			require.Equal(t, "https", parsed.Protocol)
		})
	}
}

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
