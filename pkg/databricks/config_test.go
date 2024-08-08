package databricks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_ToIngestr(t *testing.T) {
	t.Parallel()
	c := Config{
		Host:  "localhost",
		Token: "xxxxxx",
		Path:  "sql/1.0/endpoints/a1b234c5678901d2",
		Port:  443,
	}

	expected := "token:xxxxxx@localhost:443/sql/1.0/endpoints/a1b234c5678901d2"

	assert.Equal(t, expected, c.ToDBConnectionURI())
}
