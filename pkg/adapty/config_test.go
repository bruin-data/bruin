package adapty

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	lookbackDays := 0
	cfg := Config{
		APIKey:       "secret with spaces",
		LookbackDays: &lookbackDays,
		Timezone:     "Europe/Istanbul",
	}

	uri, err := cfg.GetIngestrURI()
	require.NoError(t, err)

	parsed, err := url.Parse(uri)
	require.NoError(t, err)
	require.Equal(t, "adapty", parsed.Scheme)
	require.Empty(t, parsed.Host)

	query := parsed.Query()
	require.Equal(t, "secret with spaces", query.Get("api_key"))
	require.Equal(t, "0", query.Get("lookback_days"))
	require.Equal(t, "Europe/Istanbul", query.Get("timezone"))
}

func TestConfig_GetIngestrURI_OmitsOptionalFields(t *testing.T) {
	t.Parallel()

	uri, err := (Config{APIKey: "secret"}).GetIngestrURI()
	require.NoError(t, err)

	parsed, err := url.Parse(uri)
	require.NoError(t, err)

	query := parsed.Query()
	require.Equal(t, "secret", query.Get("api_key"))
	require.False(t, query.Has("lookback_days"))
	require.False(t, query.Has("timezone"))
}

func TestConfig_GetIngestrURI_RequiresAPIKey(t *testing.T) {
	t.Parallel()

	_, err := (Config{}).GetIngestrURI()
	require.ErrorContains(t, err, "adapty: api_key must be provided")
}

func TestConfig_GetIngestrURI_RejectsNegativeLookbackDays(t *testing.T) {
	t.Parallel()

	negative := -1
	cfg := Config{
		APIKey:       "secret",
		LookbackDays: &negative,
	}

	_, err := cfg.GetIngestrURI()
	require.ErrorContains(t, err, "lookback_days cannot be negative")
}

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{APIKey: "secret"})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	require.Contains(t, uri, "adapty://?")
}
