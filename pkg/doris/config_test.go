package doris

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	c := Config{
		Username: "root",
		Password: "password",
		Host:     "localhost",
		Database: "test",
	}

	assert.Equal(t, "mysql+pymysql://root:password@localhost:9030/test", c.GetIngestrURI())

	c = Config{
		Username:    "root",
		Password:    "password",
		Host:        "localhost",
		Port:        9030,
		Database:    "test",
		SslCaPath:   "/path/to/ca.pem",
		SslCertPath: "/path/to/cert.pem",
		SslKeyPath:  "/path/to/key.pem",
	}

	assert.Equal(t, "mysql+pymysql://root:password@localhost:9030/test?ssl_ca=%2Fpath%2Fto%2Fca.pem&ssl_cert=%2Fpath%2Fto%2Fcert.pem&ssl_key=%2Fpath%2Fto%2Fkey.pem", c.GetIngestrURI())

	c = Config{
		Username: "root",
		Password: "password",
		Host:     "localhost",
		Port:     9030,
		Database: "test",
		Driver:   "mysqlconnector",
	}

	assert.Equal(t, "mysql+mysqlconnector://root:password@localhost:9030/test", c.GetIngestrURI())
}

func TestConfig_ToDBConnectionURI(t *testing.T) {
	t.Parallel()

	c := Config{
		Username: "root",
		Password: "password",
		Host:     "localhost",
		Database: "test",
	}

	got, err := c.ToDBConnectionURI()
	require.NoError(t, err)

	assert.Equal(t, "root:password@tcp(localhost:9030)/test?multiStatements=true&parseTime=true", got)
}

func TestConfig_ToDBConnectionURIWithInvalidTLSPath(t *testing.T) {
	t.Parallel()

	c := Config{
		Username:  "root",
		Password:  "password",
		Host:      "localhost",
		Database:  "test",
		SslCaPath: "/does/not/exist.pem",
	}

	_, err := c.ToDBConnectionURI()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ssl_ca_path")
}
