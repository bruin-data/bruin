package athena

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_ToDSNNoQuery(t *testing.T) {
	t.Parallel()
	c := Config{
		OutputBucket:    "bucket",
		Region:          "us-west-2",
		AccessID:        "access",
		SecretAccessKey: "secret",
	}

	expected := "token:xxxxxx@localhost:443/sql/1.0/endpoints/a1b234c5678901d2"

	assert.Equal(t, expected, c.ToDBConnectionURI())
}
