package sumble

import (
	"errors"
	"net/url"
	"strings"
)

// Config holds the credentials required to connect to the Sumble v9 API.
type Config struct {
	// APIKey is the Sumble API key, sent to Sumble as a bearer token. Required.
	APIKey string
}

// GetIngestrURI builds the ingestr source URI for Sumble.
// Format: sumble://?api_key=<api_key>
func (c *Config) GetIngestrURI() (string, error) {
	apiKey := strings.TrimSpace(c.APIKey)
	if apiKey == "" {
		return "", errors.New("sumble: api_key must be provided")
	}

	params := url.Values{}
	params.Set("api_key", apiKey)

	return "sumble://?" + params.Encode(), nil
}
