package hana

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
		Port:     39013,
		Database: "test",
	}

	assert.Equal(t, "hana://user:password@localhost:39013/test", c.GetIngestrURI())
}
