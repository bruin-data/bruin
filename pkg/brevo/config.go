package brevo

import (
	"errors"
	"net/url"
	"strings"
)

type Config struct {
	APIKey string
}

// GetIngestrURI builds a brevo://?api_key=... URI.
func (c *Config) GetIngestrURI() (string, error) {
	// The key goes into the URI exactly as configured: credential masking registers the configured
	// value, so a trimmed copy would leak in debug output.
	if strings.TrimSpace(c.APIKey) == "" {
		return "", errors.New("brevo: api_key must be provided")
	}

	params := url.Values{}
	params.Set("api_key", c.APIKey)

	// url.URL.String drops the "//" when there is no host, and ingestr requires the brevo:// prefix.
	return "brevo://?" + params.Encode(), nil
}
