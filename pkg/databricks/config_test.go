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

func TestConfig_UseOAuthM2M(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		clientID     string
		clientSecret string
		expected     bool
	}{
		{
			name:         "both credentials provided",
			clientID:     "my-client-id",
			clientSecret: "my-client-secret",
			expected:     true,
		},
		{
			name:         "only client ID provided",
			clientID:     "my-client-id",
			clientSecret: "",
			expected:     false,
		},
		{
			name:         "only client secret provided",
			clientID:     "",
			clientSecret: "my-client-secret",
			expected:     false,
		},
		{
			name:         "neither provided",
			clientID:     "",
			clientSecret: "",
			expected:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := Config{
				ClientID:     tt.clientID,
				ClientSecret: tt.clientSecret,
			}
			assert.Equal(t, tt.expected, c.UseOAuthM2M())
		})
	}
}

func TestConfig_ToDSNWithOAuthM2M(t *testing.T) {
	t.Parallel()
	c := Config{
		Host:         "azuredatabricks.com",
		Path:         "sql/1.0/endpoints/a1b234c5678901d2",
		Port:         443,
		Catalog:      "my_catalog",
		Schema:       "my_schema",
		ClientID:     "my-client-id",
		ClientSecret: "my-client-secret",
	}

	result := c.ToDBConnectionURI()

	// Verify OAuth M2M parameters are present
	assert.Contains(t, result, "authType=OAuthM2M")
	assert.Contains(t, result, "clientID=my-client-id")
	assert.Contains(t, result, "clientSecret=my-client-secret")
	assert.Contains(t, result, "catalog=my_catalog")
	assert.Contains(t, result, "schema=my_schema")
	// Should NOT contain token:xxx@ format
	assert.NotContains(t, result, "token:")
	// Should contain host and path
	assert.Contains(t, result, "azuredatabricks.com:443")
	assert.Contains(t, result, "/sql/1.0/endpoints/a1b234c5678901d2")
}

func TestConfig_ToDSNWithOAuthM2MNoQuery(t *testing.T) {
	t.Parallel()
	c := Config{
		Host:         "localhost",
		Path:         "sql/1.0/endpoints/a1b234c5678901d2",
		Port:         443,
		ClientID:     "my-client-id",
		ClientSecret: "my-client-secret",
	}

	result := c.ToDBConnectionURI()

	// Verify OAuth M2M parameters are present
	assert.Contains(t, result, "authType=OAuthM2M")
	assert.Contains(t, result, "clientID=my-client-id")
	assert.Contains(t, result, "clientSecret=my-client-secret")
	// Should NOT contain token:xxx@ format
	assert.NotContains(t, result, "token:")
}

func TestConfig_GetIngestrURIWithToken(t *testing.T) {
	t.Parallel()
	c := Config{
		Host:    "azuredatabricks.com",
		Token:   "my-token",
		Path:    "sql/1.0/endpoints/a1b234c5678901d2",
		Catalog: "my_catalog",
		Schema:  "my_schema",
	}

	expected := "databricks://token:my-token@azuredatabricks.com?catalog=my_catalog&http_path=sql%2F1.0%2Fendpoints%2Fa1b234c5678901d2&schema=my_schema"

	assert.Equal(t, expected, c.GetIngestrURI())
}

func TestConfig_GetIngestrURIWithOAuthM2M(t *testing.T) {
	t.Parallel()
	c := Config{
		Host:         "azuredatabricks.com",
		Path:         "sql/1.0/endpoints/a1b234c5678901d2",
		Catalog:      "my_catalog",
		Schema:       "my_schema",
		ClientID:     "my-client-id",
		ClientSecret: "my-client-secret",
	}

	result := c.GetIngestrURI()

	// Verify OAuth M2M parameters are present
	assert.Contains(t, result, "client_id=my-client-id")
	assert.Contains(t, result, "client_secret=my-client-secret")
	assert.Contains(t, result, "catalog=my_catalog")
	assert.Contains(t, result, "schema=my_schema")
	// Should NOT contain token:xxx@ format
	assert.NotContains(t, result, "token:")
	// Should have databricks scheme
	assert.Contains(t, result, "databricks://")
}
