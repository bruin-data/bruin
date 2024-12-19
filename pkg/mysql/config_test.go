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
		Username:    "user",
		Password:    "password",
		Host:        "localhost",
		Port:        3306,
		Database:    "test",
		SslCaPath:   "/path/to/ca.pem",
		SslCertPath: "/path/to/cert.pem",
		SslKeyPath:  "/path/to/key.pem",
	}

	assert.Equal(t, "mysql://user:password@localhost:3306/test?ssl_ca=%2Fpath%2Fto%2Fca.pem&ssl_cert=%2Fpath%2Fto%2Fcert.pem&ssl_key=%2Fpath%2Fto%2Fkey.pem", c.GetIngestrURI())

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
