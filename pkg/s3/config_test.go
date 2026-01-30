package s3

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Source S3: bucket/path is not set in .bruin.yml
func TestConfig_GetIngestrURI_EmptyBucketAndPath_ReturnsURIWithDoubleSlash(t *testing.T) {
	t.Parallel()
	config := Config{
		BucketName:      "",
		PathToFile:      "",
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
	}
	got := config.GetIngestrURI()
	require.True(t, strings.HasPrefix(got, "s3://"), "want prefix s3://")
	require.False(t, strings.Contains(got, "s3:?"), "should not be s3:? (missing //)")
	require.Contains(t, got, "access_key_id=")
	require.Contains(t, got, "secret_access_key=")
}

func TestConfig_GetIngestrURI_WhitespaceOnlyBucketAndPath_ReturnsURIWithDoubleSlash(t *testing.T) {
	t.Parallel()
	config := Config{
		BucketName:      "  ",
		PathToFile:      "   ",
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
	}
	got := config.GetIngestrURI()
	require.True(t, strings.HasPrefix(got, "s3://"), "want prefix s3://")
}

// Destination S3: bucket/path is set in .bruin.yml
func TestConfig_GetIngestrURI_BucketAndPathSet_ReturnsFullURI(t *testing.T) {
	t.Parallel()
	config := Config{
		BucketName:      "mybucket",
		PathToFile:      "prefix/file.csv",
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
	}
	got := config.GetIngestrURI()
	require.True(t, strings.HasPrefix(got, "s3://mybucket/prefix/file.csv"), "want full URI with bucket/path")
	require.Contains(t, got, "?access_key_id=key&secret_access_key=secret")
}

func TestConfig_GetIngestrURI_TrimsBucketAndPathInURL(t *testing.T) {
	t.Parallel()
	config := Config{
		BucketName:      "  mybucket  ",
		PathToFile:      "  path/to/file  ",
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
	}
	got := config.GetIngestrURI()
	require.False(t, strings.Contains(got, "  mybucket") || strings.Contains(got, "mybucket  "), "bucket should be trimmed")
	require.Contains(t, got, "s3://mybucket/path/to/file?access_key_id=key&secret_access_key=secret")
}
