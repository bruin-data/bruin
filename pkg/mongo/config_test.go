package mongo

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	c := Config{
		Username: "user",
		Password: "password",
		Host:     "localhost",
		Port:     27017,
		Database: "test",
	}

	assert.Equal(t, "mongodb://user:password@localhost:27017/test", c.GetIngestrURI())
}
