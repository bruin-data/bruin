package azureblob

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI_RequiresAccountNameOrConnectionString(t *testing.T) {
	t.Parallel()

	_, err := Config{AccountName: "  ", AccountKey: "key"}.GetIngestrURI()
	require.ErrorContains(t, err, "account_name or connection_string")
}

func TestConfig_GetIngestrURI_AccountNameOnly(t *testing.T) {
	t.Parallel()

	got, err := Config{AccountName: " myaccount "}.GetIngestrURI()

	require.NoError(t, err)
	require.Equal(t, "az://?account_name=myaccount", got)
}

func TestConfig_GetIngestrURI_EncodesCredentials(t *testing.T) {
	t.Parallel()

	connectionString := "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=a+b/c==;EndpointSuffix=core.windows.net"
	got, err := Config{
		AccountName:      "myaccount",
		ConnectionString: connectionString,
		AccountKey:       "a+b/c==",
		SASToken:         "?sv=2024-01-01&sp=rwdl&sig=x%2By",
		TenantID:         "tenant",
		ClientID:         "client",
		ClientSecret:     "p&ss=word",
		Layout:           "{table_name}/{load_id}.{file_id}.{ext}",
	}.GetIngestrURI()

	require.NoError(t, err)
	u, err := url.Parse(got)
	require.NoError(t, err)
	require.Equal(t, "az", u.Scheme)
	require.Empty(t, u.Host)
	require.Equal(t, url.Values{
		"account_name":      {"myaccount"},
		"connection_string": {connectionString},
		"account_key":       {"a+b/c=="},
		"sas_token":         {"?sv=2024-01-01&sp=rwdl&sig=x%2By"},
		"tenant_id":         {"tenant"},
		"client_id":         {"client"},
		"client_secret":     {"p&ss=word"},
		"layout":            {"{table_name}/{load_id}.{file_id}.{ext}"},
	}, u.Query())
}

func TestConfig_GetIngestrURI_ConnectionStringWithoutAccountName(t *testing.T) {
	t.Parallel()

	got, err := Config{ConnectionString: "UseDevelopmentStorage=true"}.GetIngestrURI()

	require.NoError(t, err)
	require.Equal(t, "az://?connection_string=UseDevelopmentStorage%3Dtrue", got)
}

func TestConfig_GetIngestrURI_ContainerAndPath(t *testing.T) {
	t.Parallel()

	got, err := Config{AccountName: "myaccount", ContainerName: " lake ", PathToFile: "/exports/daily/"}.GetIngestrURI()
	require.NoError(t, err)
	require.Equal(t, "az://lake/exports/daily?account_name=myaccount", got)

	got, err = Config{AccountName: "myaccount", ContainerName: "lake"}.GetIngestrURI()
	require.NoError(t, err)
	require.Equal(t, "az://lake?account_name=myaccount", got)

	_, err = Config{AccountName: "myaccount", PathToFile: "exports"}.GetIngestrURI()
	require.ErrorContains(t, err, "container_name")
}
