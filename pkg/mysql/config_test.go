package mysql

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
		Port:     3306,
		Database: "test",
	}

	assert.Equal(t, "mysql://user:password@localhost:3306/test", c.GetIngestrURI())

	c = Config{
		Username: "user",
		Password: "password",
		Host:     "localhost",
		Port:     3306,
		Database: "test",
		Driver:   "mysqlconnector",
	}

	assert.Equal(t, "mysql+mysqlconnector://user:password@localhost:3306/test", c.GetIngestrURI())
}
