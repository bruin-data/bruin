package cassandra

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI_DefaultPort(t *testing.T) {
	t.Parallel()

	c := Config{
		Host:     "localhost",
		Keyspace: "analytics",
	}

	assert.Equal(t, "cassandra://localhost:9042/analytics", c.GetIngestrURI())
}

func TestConfig_GetIngestrURI_WithCredentials(t *testing.T) {
	t.Parallel()

	c := Config{
		Username: "user",
		Password: "password",
		Host:     "localhost",
		Port:     9142,
		Keyspace: "analytics",
	}

	assert.Equal(t, "cassandra://user:password@localhost:9142/analytics", c.GetIngestrURI())
}

func TestConfig_GetIngestrURI_EncodesSpecialChars(t *testing.T) {
	t.Parallel()

	c := Config{
		Username:       "user@example.com",
		Password:       "p@ss:w/rd?",
		Host:           "cass.example.com",
		Port:           9042,
		Keyspace:       "tenant keyspace",
		Consistency:    "local_quorum",
		Timeout:        "10s",
		ConnectTimeout: "5s",
	}

	assert.Equal(t, "cassandra://user%40example.com:p%40ss%3Aw%2Frd%3F@cass.example.com:9042/tenant%20keyspace?connect_timeout=5s&consistency=local_quorum&timeout=10s", c.GetIngestrURI())
}

func TestConfig_GetIngestrURI_OptionalQueryParameters(t *testing.T) {
	t.Parallel()

	c := Config{
		Host:                     "cass-1",
		Hosts:                    []string{"cass-1", "cass-2", " cass-3 "},
		Keyspace:                 "analytics",
		Consistency:              "quorum",
		PageSize:                 1000,
		Timeout:                  "30s",
		ConnectTimeout:           "10s",
		SSL:                      true,
		DisableInitialHostLookup: true,
	}

	assert.Equal(t, "cassandra://cass-1:9042/analytics?connect_timeout=10s&consistency=quorum&disable_initial_host_lookup=true&hosts=cass-1%2Ccass-2%2Ccass-3&page_size=1000&ssl=true&timeout=30s", c.GetIngestrURI())
}

func TestConfig_GetIngestrURI_UsesFirstHostWhenHostIsEmpty(t *testing.T) {
	t.Parallel()

	c := Config{
		Hosts:    []string{"cass-1", "cass-2"},
		Keyspace: "analytics",
	}

	assert.Equal(t, "cassandra://cass-1:9042/analytics?hosts=cass-1%2Ccass-2", c.GetIngestrURI())
}

func TestConfig_GetIngestrURI_OmitsOptionalAuthAndKeyspace(t *testing.T) {
	t.Parallel()

	c := Config{
		Host: "localhost",
	}

	assert.Equal(t, "cassandra://localhost:9042", c.GetIngestrURI())
}

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{
		Host:     "localhost",
		Keyspace: "analytics",
	})
	require.NoError(t, err)

	got, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "cassandra://localhost:9042/analytics", got)
}
