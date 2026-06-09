package http_test

import (
	"testing"

	bruinhttp "github.com/bruin-data/bruin/pkg/http"
	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI_ReturnsHTTPURL(t *testing.T) {
	t.Parallel()

	config := bruinhttp.Config{
		URL: "http://example.com/path/to/file.csv",
	}

	got, err := config.GetIngestrURI()
	require.NoError(t, err)
	require.Equal(t, "http://example.com/path/to/file.csv", got)
}

func TestConfig_GetIngestrURI_ReturnsHTTPSURLWithQuery(t *testing.T) {
	t.Parallel()

	config := bruinhttp.Config{
		URL: "https://example.com/api/export?format=json",
	}

	got, err := config.GetIngestrURI()
	require.NoError(t, err)
	require.Equal(t, "https://example.com/api/export?format=json", got)
}

func TestConfig_GetIngestrURI_TrimsURL(t *testing.T) {
	t.Parallel()

	config := bruinhttp.Config{
		URL: "  https://example.com/data.parquet  ",
	}

	got, err := config.GetIngestrURI()
	require.NoError(t, err)
	require.Equal(t, "https://example.com/data.parquet", got)
}

func TestConfig_GetIngestrURI_MissingURLReturnsError(t *testing.T) {
	t.Parallel()

	config := bruinhttp.Config{}

	_, err := config.GetIngestrURI()
	require.Error(t, err)
	require.Contains(t, err.Error(), "url")
}

func TestConfig_GetIngestrURI_UnsupportedSchemeReturnsError(t *testing.T) {
	t.Parallel()

	config := bruinhttp.Config{
		URL: "ftp://example.com/data.csv",
	}

	_, err := config.GetIngestrURI()
	require.Error(t, err)
	require.Contains(t, err.Error(), "http or https")
}

func TestConfig_GetIngestrURI_MissingHostReturnsError(t *testing.T) {
	t.Parallel()

	config := bruinhttp.Config{
		URL: "https:///data.csv",
	}

	_, err := config.GetIngestrURI()
	require.Error(t, err)
	require.Contains(t, err.Error(), "host")
}

func TestClient_GetIngestrURI_ReturnsConfigURI(t *testing.T) {
	t.Parallel()

	client, err := bruinhttp.NewClient(bruinhttp.Config{
		URL: "https://example.com/data.jsonl",
	})
	require.NoError(t, err)

	got, err := client.GetIngestrURI()
	require.NoError(t, err)
	require.Equal(t, "https://example.com/data.jsonl", got)
}
