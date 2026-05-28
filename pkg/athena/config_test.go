package athena

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_ToDSNNoQuery(t *testing.T) {
	t.Parallel()
	c := Config{
		OutputBucket:    "s3://bucket",
		Region:          "us-west-2",
		AccessID:        "access",
		SecretAccessKey: "secret",
		Database:        "some_db",
	}

	expected := "s3://bucket?WGRemoteCreation=true&accessID=access&db=some_db&missingAsEmptyString=true&region=us-west-2&resultPollIntervalSeconds=3&secretAccessKey=secret"
	actual, err := c.ToDBConnectionURI()
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestConfig_GetIngestrURI_EncodesSpecialChars(t *testing.T) {
	t.Parallel()

	// AWS secrets are base64-ish and frequently contain '+', '/', '='.
	// Session tokens also include these characters. Raw concatenation
	// produces a URI that ingestr parses incorrectly.
	c := Config{
		OutputBucket:    "s3://my-bucket/path",
		Region:          "us-west-2",
		AccessID:        "AKIA/EXAMPLE+ID",
		SecretAccessKey: "abc+def/ghi=jkl",
		SessionToken:    "tok+en/with=chars&extra",
		Database:        "some_db",
	}

	uri := c.GetIngestrURI()

	parsed, err := url.Parse(uri)
	require.NoError(t, err)
	require.Equal(t, "athena", parsed.Scheme)

	q := parsed.Query()
	require.Equal(t, "s3://my-bucket/path", q.Get("bucket"))
	require.Equal(t, "AKIA/EXAMPLE+ID", q.Get("access_key_id"))
	require.Equal(t, "abc+def/ghi=jkl", q.Get("secret_access_key"))
	require.Equal(t, "us-west-2", q.Get("region_name"))
	require.Equal(t, "tok+en/with=chars&extra", q.Get("session_token"))
}
