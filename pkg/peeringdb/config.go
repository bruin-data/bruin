package peeringdb

import (
	"errors"
	"net/url"
	"strings"
)

type Config struct {
	APIKey string
}

// GetIngestrURI builds a peeringdb://?api_key=... URI. Guests use the connectionless
// peeringdb.com source instead, so a connection always carries a key.
func (c *Config) GetIngestrURI() (string, error) {
	// The key goes into the URI exactly as configured: credential masking registers the configured
	// value, so a trimmed copy would leak in debug output.
	if strings.TrimSpace(c.APIKey) == "" {
		return "", errors.New("peeringdb: api_key must be provided")
	}

	params := url.Values{}
	params.Set("api_key", c.APIKey)

	// url.URL.String drops the "//" when there is no host, and ingestr requires the peeringdb:// prefix.
	return "peeringdb://?" + params.Encode(), nil
}
