package gcs

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI_NoCredentials_ReturnsError(t *testing.T) {
	t.Parallel()
	config := Config{
		BucketName: "bucket",
		PathToFile: "path",
	}
	_, err := config.GetIngestrURI()
	require.Error(t, err)
	require.Contains(t, err.Error(), "service_account") // service_account_file or service_account_json must be provided
}

// Source GCS: bucket/path is not set in .bruin.yml
func TestConfig_GetIngestrURI_EmptyBucketAndPath_ReturnsURIWithDoubleSlash(t *testing.T) {
	t.Parallel()
	config := Config{
		BucketName:         "",
		PathToFile:         "",
		ServiceAccountFile: "/path/to/creds.json",
	}
	got, err := config.GetIngestrURI()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(got, "gs://"), "want prefix gs://")
	require.False(t, strings.Contains(got, "gs:?"), "should not be gs:? (missing //)")
	require.Contains(t, got, "credentials_path=")
}

func TestConfig_GetIngestrURI_WhitespaceOnlyBucketAndPath_ReturnsURIWithDoubleSlash(t *testing.T) {
	t.Parallel()
	config := Config{
		BucketName:         "  ",
		PathToFile:         "   ",
		ServiceAccountFile: "/path/to/creds.json",
	}
	got, err := config.GetIngestrURI()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(got, "gs://"), "want prefix gs://")
}

// Destination GCS: bucket/path is set in .bruin.yml
func TestConfig_GetIngestrURI_BucketAndPathSet_ReturnsFullURI(t *testing.T) {
	t.Parallel()
	config := Config{
		BucketName:         "mybucket",
		PathToFile:         "prefix/file.csv",
		ServiceAccountFile: "creds.json",
	}
	got, err := config.GetIngestrURI()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(got, "gs://mybucket/prefix/file.csv"), "want full URI with bucket/path")
	require.Contains(t, got, "?credentials_path=creds.json")
}

func TestConfig_GetIngestrURI_TrimsBucketAndPathInURL(t *testing.T) {
	t.Parallel()
	config := Config{
		BucketName:         "  mybucket  ",
		PathToFile:         "  path/to/file  ",
		ServiceAccountFile: "creds.json",
	}
	got, err := config.GetIngestrURI()
	require.NoError(t, err)
	require.False(t, strings.Contains(got, "  mybucket") || strings.Contains(got, "mybucket  "), "bucket should be trimmed")
	require.Contains(t, got, "gs://mybucket/path/to/file")
	require.Contains(t, got, "credentials_path=creds.json")
}
