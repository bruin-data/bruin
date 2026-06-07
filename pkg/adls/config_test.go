package adls

import (
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI_MissingAccountName_ReturnsError(t *testing.T) {
	t.Parallel()

	for _, accountName := range []string{"", "   "} {
		config := Config{AccountName: accountName}
		_, err := config.GetIngestrURI()
		require.Error(t, err)
		require.Contains(t, err.Error(), "account_name")
	}
}

func TestConfig_GetIngestrURI_AccountName_ReturnsURI(t *testing.T) {
	t.Parallel()

	got, err := Config{AccountName: "myaccount"}.GetIngestrURI()

	require.NoError(t, err)
	require.Equal(t, "adls://?account_name=myaccount", got)
}

func TestConfig_GetIngestrURI_TrimsAccountName(t *testing.T) {
	t.Parallel()

	got, err := Config{AccountName: "  myaccount  "}.GetIngestrURI()

	require.NoError(t, err)
	require.Equal(t, "adls://?account_name=myaccount", got)
}

func TestConfig_GetIngestrURI_ServicePrincipal_ReturnsURI(t *testing.T) {
	t.Parallel()

	got, err := Config{
		AccountName:  "myaccount",
		TenantID:     "tenant",
		ClientID:     "client",
		ClientSecret: "secret",
	}.GetIngestrURI()

	require.NoError(t, err)
	require.True(t, strings.HasPrefix(got, "adls://?"), "got %s", got)
	requireQueryValues(t, got, map[string]string{
		"account_name":  "myaccount",
		"tenant_id":     "tenant",
		"client_id":     "client",
		"client_secret": "secret",
	})
}

func TestConfig_GetIngestrURI_AccountKeyAndSASTokenAreEncoded(t *testing.T) {
	t.Parallel()

	got, err := Config{
		AccountName: "myaccount",
		AccountKey:  "abc+/=",
		SASToken:    "sv=2024-01-01&sig=a+b/c",
	}.GetIngestrURI()

	require.NoError(t, err)
	require.NotContains(t, strings.TrimPrefix(got, "adls://?"), "&sig=")
	requireQueryValues(t, got, map[string]string{
		"account_name": "myaccount",
		"account_key":  "abc+/=",
		"sas_token":    "sv=2024-01-01&sig=a+b/c",
	})
}

func TestConfig_GetIngestrURI_LayoutIsOptionalAndTrimmed(t *testing.T) {
	t.Parallel()

	got, err := Config{
		AccountName: "myaccount",
		Layout:      "  {table_name}/{load_id}.{file_id}.{ext}  ",
	}.GetIngestrURI()

	require.NoError(t, err)
	requireQueryValues(t, got, map[string]string{
		"account_name": "myaccount",
		"layout":       "{table_name}/{load_id}.{file_id}.{ext}",
	})
}

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{AccountName: "myaccount"})
	require.NoError(t, err)

	got, err := client.GetIngestrURI()

	require.NoError(t, err)
	require.Equal(t, "adls://?account_name=myaccount", got)
}

func requireQueryValues(t *testing.T, rawURI string, want map[string]string) {
	t.Helper()

	u, err := url.Parse(rawURI)
	require.NoError(t, err)
	require.Equal(t, "adls", u.Scheme)
	require.Empty(t, u.Host)
	require.Empty(t, u.Path)

	query := u.Query()
	require.Len(t, query, len(want))
	for key, value := range want {
		require.Equal(t, value, query.Get(key), "query param %s", key)
	}
}
