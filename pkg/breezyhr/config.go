package breezyhr

import (
	"errors"
	"net/url"
	"strings"
)

type Config struct {
	APIKey    string
	CompanyID string
}

// GetIngestrURI builds a breezyhr://?api_key=...&company_id=... URI.
func (c *Config) GetIngestrURI() (string, error) {
	// The key goes into the URI exactly as configured: credential masking registers the configured
	// value, so a trimmed copy would leak in debug output.
	if strings.TrimSpace(c.APIKey) == "" {
		return "", errors.New("breezyhr: api_key must be provided")
	}

	companyID := strings.TrimSpace(c.CompanyID)
	if companyID == "" {
		return "", errors.New("breezyhr: company_id must be provided")
	}

	params := url.Values{}
	params.Set("api_key", c.APIKey)
	params.Set("company_id", companyID)

	// url.URL.String drops the "//" when there is no host, and ingestr requires the breezyhr:// prefix.
	return "breezyhr://?" + params.Encode(), nil
}
