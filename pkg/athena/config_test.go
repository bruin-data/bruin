package athena

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_ToDSNNoQuery(t *testing.T) {
	t.Parallel()
	c := Config{
		OutputBucket:    "s3://bucket",
		Region:          "us-west-2",
		AccessID:        "access",
		SecretAccessKey: "secret",
	}

	expected := "s3://bucket?WGRemoteCreation=true&accessID=access&db=default&missingAsEmptyString=true&region=us-west-2&resultPollIntervalSeconds=3&secretAccessKey=secret"

	assert.Equal(t, expected, c.ToDBConnectionURI())
}
