package sharepoint

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	maxFileSize := int64(1048576)
	maxFiles := int64(0)
	cfg := Config{
		TenantID:     "tenant-id",
		ClientID:     "client-id",
		ClientSecret: "secret with spaces",
		Hostname:     "example.sharepoint.com",
		Site:         "sites/Example",
		Library:      "Shared Documents",
		MaxFileSize:  &maxFileSize,
		MaxFiles:     &maxFiles,
	}

	uri, err := cfg.GetIngestrURI()
	require.NoError(t, err)

	parsed, err := url.Parse(uri)
	require.NoError(t, err)
	require.Equal(t, "sharepoint", parsed.Scheme)
	require.Empty(t, parsed.Host)

	query := parsed.Query()
	require.Equal(t, "tenant-id", query.Get("tenant_id"))
	require.Equal(t, "client-id", query.Get("client_id"))
	require.Equal(t, "secret with spaces", query.Get("client_secret"))
	require.Equal(t, "example.sharepoint.com", query.Get("hostname"))
	require.Equal(t, "sites/Example", query.Get("site"))
	require.Equal(t, "Shared Documents", query.Get("library"))
	require.Equal(t, "1048576", query.Get("max_file_size"))
	require.Equal(t, "0", query.Get("max_files"))
}

func TestConfig_GetIngestrURI_RequiresCredentials(t *testing.T) {
	t.Parallel()

	_, err := (Config{}).GetIngestrURI()
	require.ErrorContains(t, err, "sharepoint:")
}

func TestConfig_GetIngestrURI_RejectsNegativeLimits(t *testing.T) {
	t.Parallel()

	negative := int64(-1)
	cfg := Config{
		TenantID:     "tenant-id",
		ClientID:     "client-id",
		ClientSecret: "secret",
		Hostname:     "example.sharepoint.com",
		Site:         "sites/Example",
		MaxFiles:     &negative,
	}

	_, err := cfg.GetIngestrURI()
	require.ErrorContains(t, err, "max_files cannot be negative")
}

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{
		TenantID:     "tenant-id",
		ClientID:     "client-id",
		ClientSecret: "secret",
		Hostname:     "example.sharepoint.com",
		Site:         "sites/Example",
	})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	require.Contains(t, uri, "sharepoint://?")
}
