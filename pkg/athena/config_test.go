package athena

import (
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
