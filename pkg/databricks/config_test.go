package databricks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_ToDSNNoQuery(t *testing.T) {
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

func TestConfig_ToDSN(t *testing.T) {
	t.Parallel()
	c := Config{
		Host:    "azuredatabricks.com",
		Token:   "yyyyy",
		Path:    "sql/1.0/endpoints/a1b234c5678901d2",
		Port:    444,
		Catalog: "my_catalog",
		Schema:  "my_schema",
	}

	expected := "token:yyyyy@azuredatabricks.com:444/sql/1.0/endpoints/a1b234c5678901d2?catalog=my_catalog&schema=my_schema"

	assert.Equal(t, expected, c.ToDBConnectionURI())
}
