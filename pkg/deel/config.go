package deel

import (
	"fmt"
	"net/url"
	"strings"
)

// Config holds the credentials required to connect to the Deel REST API.
type Config struct {
	// APIKey is a Deel organization API token, sent as a bearer token. Give the
	// token the read scopes needed by the tables you plan to ingest. Required.
	APIKey string
	// Environment selects which Deel environment to reach, production or sandbox.
	// Optional, defaults to production. Sandbox tokens are separate from
	// production tokens.
	Environment string
}

// GetIngestrURI builds the ingestr source URI for Deel.
// Format: deel://?api_key=<api_key>[&environment=<production|sandbox>]
func (c *Config) GetIngestrURI() (string, error) {
	apiKey := strings.TrimSpace(c.APIKey)
	if apiKey == "" {
		return "", fmt.Errorf("deel: api_key must be provided")
	}

	params := url.Values{}
	params.Set("api_key", apiKey)

	if environment := strings.TrimSpace(c.Environment); environment != "" {
		if environment != "production" && environment != "sandbox" {
			return "", fmt.Errorf("deel: environment must be production or sandbox, got %q", environment)
		}
		params.Set("environment", environment)
	}

	return "deel://?" + params.Encode(), nil
}
