package snowflake

import (
	"net/url"
	"strings"
	"testing"

	"github.com/snowflakedb/gosnowflake/v2"
	"github.com/stretchr/testify/require"
)

func TestConfig_DSNEndpoint(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		account  string
		region   string
		host     string
		port     int
		wantHost string
		wantPort int

		wantOCSPDisabled bool
	}{
		{name: "defaults", wantHost: "my-account.eu-west-1.snowflakecomputing.com", wantPort: 443},
		{name: "host only", host: "snowflake.example.com", wantHost: "snowflake.example.com", wantPort: 443},
		{name: "port only", port: 8443, wantHost: "my-account.eu-west-1.snowflakecomputing.com", wantPort: 8443},
		{name: "host and port", host: "127.0.0.1", port: 8443, wantHost: "127.0.0.1", wantPort: 8443},
		{name: "privatelink host is used as given", host: "my-account.eu-west-1.privatelink.snowflakecomputing.com", wantHost: "my-account.eu-west-1.privatelink.snowflakecomputing.com", wantPort: 443},
		{name: "org account host is used as given", host: "my-account.snowflakecomputing.com", wantHost: "my-account.snowflakecomputing.com", wantPort: 443},
		{name: "locator host gets the region", account: "xy12345", host: "xy12345.snowflakecomputing.com", wantHost: "xy12345.eu-west-1.snowflakecomputing.com", wantPort: 443},
		{name: "locator host in another case gets the region", account: "xy12345", host: "XY12345.SnowflakeComputing.com", wantHost: "XY12345.eu-west-1.SnowflakeComputing.com", wantPort: 443},
		{name: "locator host with cloud region", account: "xy12345", region: "us-east-2.aws", host: "xy12345.snowflakecomputing.com", wantHost: "xy12345.us-east-2.aws.snowflakecomputing.com", wantPort: 443},
		{name: "locator host that has the region is used as given", account: "xy12345", host: "xy12345.eu-west-1.snowflakecomputing.com", wantHost: "xy12345.eu-west-1.snowflakecomputing.com", wantPort: 443},
		{name: "locator privatelink host is used as given", account: "xy12345", host: "xy12345.eu-west-1.privatelink.snowflakecomputing.com", wantHost: "xy12345.eu-west-1.privatelink.snowflakecomputing.com", wantPort: 443},
		{name: "regionless privatelink host is used as given", account: "xy12345", host: "xy12345.privatelink.snowflakecomputing.com", wantHost: "xy12345.privatelink.snowflakecomputing.com", wantPort: 443},
		{name: "host of another account is used as given", account: "xy12345", host: "ab67890.snowflakecomputing.com", wantHost: "ab67890.snowflakecomputing.com", wantPort: 443},
		{name: "locator host with port gets the region", account: "xy12345", host: "xy12345.snowflakecomputing.com", port: 8443, wantHost: "xy12345.eu-west-1.snowflakecomputing.com", wantPort: 8443},
		{name: "host merely containing the localstack domain", host: "snowflake.localhost.localstack.cloud.example.com", wantHost: "snowflake.localhost.localstack.cloud.example.com", wantPort: 443},
		{name: "host merely ending in the localstack name", host: "mylocalhost.localstack.cloud", wantHost: "mylocalhost.localstack.cloud", wantPort: 443},
		{name: "bare localstack host", host: "localhost.localstack.cloud", wantHost: "localhost.localstack.cloud", wantPort: 443, wantOCSPDisabled: true},
		{name: "localstack host", host: "snowflake.localhost.localstack.cloud", port: 4566, wantHost: "snowflake.localhost.localstack.cloud", wantPort: 4566, wantOCSPDisabled: true},
		{name: "localstack host in mixed case", host: "Snowflake.LocalHost.LocalStack.Cloud", wantHost: "Snowflake.LocalHost.LocalStack.Cloud", wantPort: 443, wantOCSPDisabled: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			account, region := tt.account, tt.region
			if account == "" {
				account = "my-account"
			}
			if region == "" {
				region = "eu-west-1"
			}
			c := Config{
				Account:  account,
				Username: "my-user",
				Password: "test-password",
				Region:   region,
				Host:     tt.host,
				Port:     tt.port,
			}
			dsn, err := c.DSN()
			require.NoError(t, err)
			parsed, err := gosnowflake.ParseDSN(dsn)
			require.NoError(t, err)
			require.Equal(t, tt.wantHost, parsed.Host)
			require.Equal(t, tt.wantPort, parsed.Port)
			require.Equal(t, account, parsed.Account)
			require.Equal(t, region, parsed.Region)
			require.Equal(t, "https", parsed.Protocol)
			require.Equal(t, tt.wantOCSPDisabled, parsed.DisableOCSPChecks)
			require.Equal(t, tt.wantOCSPDisabled, strings.Contains(dsn, "disableOCSPChecks"))
			// gosnowflake v2.3.0+ leaves OCSP off unless the DSN or SF_DISABLE_OCSP_CHECKS opts in.
			require.Zero(t, parsed.OCSPFailOpen)
			require.NotContains(t, dsn, "ocspFailOpen")
			require.Equal(t, gosnowflake.CertRevocationCheckDisabled, parsed.CertRevocationCheckMode)
		})
	}
}

func TestConfig_GetIngestrURIEndpoint(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		host     string
		port     int
		wantHost string
		wantPort string
	}{
		{name: "defaults"},
		{name: "host only", host: "snowflake.example.com", wantHost: "snowflake.example.com"},
		{name: "host and port", host: "127.0.0.1", port: 8443, wantHost: "127.0.0.1", wantPort: "8443"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uri, err := Config{Account: "my-account", Username: "my-user", Password: "test-password", Host: tt.host, Port: tt.port}.GetIngestrURI()
			require.NoError(t, err)
			parsed, err := url.Parse(uri)
			require.NoError(t, err)
			require.Equal(t, "my-account", parsed.Host)
			require.Equal(t, tt.wantHost, parsed.Query().Get("host"))
			require.Equal(t, tt.wantPort, parsed.Query().Get("port"))
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

func TestConfig_DSNRegionFromAccount(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		account  string
		host     string
		wantHost string
	}{
		{name: "locator host gets the region from the account", account: "xy12345.eu-west-1", host: "xy12345.snowflakecomputing.com", wantHost: "xy12345.eu-west-1.snowflakecomputing.com"},
		{name: "locator host gets the cloud region from the account", account: "xy12345.us-east-2.aws", host: "xy12345.snowflakecomputing.com", wantHost: "xy12345.us-east-2.aws.snowflakecomputing.com"},
		{name: "regionless locator account keeps the host", account: "xy12345", host: "xy12345.snowflakecomputing.com", wantHost: "xy12345.snowflakecomputing.com"},
		{name: "org account keeps the host", account: "myorg-myacct", host: "myorg-myacct.snowflakecomputing.com", wantHost: "myorg-myacct.snowflakecomputing.com"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := Config{Account: tt.account, Username: "my-user", Token: "test-token", Host: tt.host}
			dsn, err := c.DSN()
			require.NoError(t, err)
			parsed, err := gosnowflake.ParseDSN(dsn)
			require.NoError(t, err)
			require.Equal(t, tt.wantHost, parsed.Host)
		})
	}
}
