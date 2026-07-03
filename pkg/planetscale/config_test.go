package planetscale

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	c := Config{
		Username: "user",
		Password: "pscale_pw_secret",
		Host:     "aws.connect.psdb.cloud",
		Port:     3306,
		Database: "my_database",
	}
	assert.Equal(t, "ps_mysql://user:pscale_pw_secret@aws.connect.psdb.cloud:3306/my_database", c.GetIngestrURI())

	// Default port is applied when unset.
	c.Port = 0
	assert.Equal(t, "ps_mysql://user:pscale_pw_secret@aws.connect.psdb.cloud:3306/my_database", c.GetIngestrURI())
}

func TestConfig_ToDBConnectionURI(t *testing.T) {
	t.Parallel()

	c := Config{
		Username: "user",
		Password: "pscale_pw_secret",
		Host:     "aws.connect.psdb.cloud",
		Port:     3306,
		Database: "my_database",
	}
	assert.Equal(t, "user:pscale_pw_secret@tcp(aws.connect.psdb.cloud:3306)/my_database?tls=true&multiStatements=true", c.ToDBConnectionURI())
}
