package mongo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()
	c := Config{
		Username: "user",
		Password: "password",
		Host:     "localhost",
		Port:     27017,
		Database: "test",
	}

	assert.Equal(t, "mongodb://user:password@localhost:27017/test", c.GetIngestrURI())
}
