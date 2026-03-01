package vertica

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_ToDBConnectionURI(t *testing.T) {
	t.Parallel()

	t.Run("basic connection URI", func(t *testing.T) {
		t.Parallel()
		c := Config{
			Username: "user",
			Password: "password",
			Host:     "localhost",
			Port:     5433,
			Database: "mydb",
		}

		uri := c.ToDBConnectionURI()
		assert.Contains(t, uri, "vertica://user:password@localhost:5433")
		assert.Contains(t, uri, "/mydb")
	})

	t.Run("connection URI with schema", func(t *testing.T) {
		t.Parallel()
		c := Config{
			Username: "user",
			Password: "password",
			Host:     "localhost",
			Port:     5433,
			Database: "mydb",
			Schema:   "analytics",
		}

		uri := c.ToDBConnectionURI()
		assert.Contains(t, uri, "vertica://user:password@localhost:5433")
		assert.Contains(t, uri, "/mydb")
		assert.Contains(t, uri, "search_path=analytics")
	})

	t.Run("connection URI without schema has no search_path", func(t *testing.T) {
		t.Parallel()
		c := Config{
			Username: "user",
			Password: "password",
			Host:     "vertica-host",
			Port:     5433,
			Database: "testdb",
		}

		uri := c.ToDBConnectionURI()
		assert.NotContains(t, uri, "search_path")
	})

	t.Run("special characters in password are encoded", func(t *testing.T) {
		t.Parallel()
		c := Config{
			Username: "user",
			Password: "p@ss w0rd!",
			Host:     "localhost",
			Port:     5433,
			Database: "mydb",
		}

		uri := c.ToDBConnectionURI()
		assert.Contains(t, uri, "vertica://user:")
		assert.Contains(t, uri, "@localhost:5433")
		// Password should be URL-encoded
		assert.NotContains(t, uri, "p@ss w0rd!")
	})
}

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	t.Run("basic ingestr URI", func(t *testing.T) {
		t.Parallel()
		c := Config{
			Username: "user",
			Password: "password",
			Host:     "localhost",
			Port:     5433,
			Database: "mydb",
		}

		uri := c.GetIngestrURI()
		assert.Equal(t, "vertica://user:password@localhost:5433/mydb", uri)
	})

	t.Run("ingestr URI ignores schema", func(t *testing.T) {
		t.Parallel()
		c := Config{
			Username: "user",
			Password: "password",
			Host:     "localhost",
			Port:     5433,
			Database: "mydb",
			Schema:   "analytics",
		}

		uri := c.GetIngestrURI()
		assert.Equal(t, "vertica://user:password@localhost:5433/mydb", uri)
		assert.NotContains(t, uri, "search_path")
		assert.NotContains(t, uri, "analytics")
	})
}
