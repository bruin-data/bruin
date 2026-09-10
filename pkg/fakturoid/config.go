package fakturoid

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// Config holds the settings required to connect to the Fakturoid API v3, a Czech
// invoicing and accounting service.
type Config struct {
	// ClientID is the OAuth client id from the Fakturoid account settings. Required.
	ClientID string
	// ClientSecret is the matching OAuth client secret. Required.
	ClientSecret string
	// Slug is the account slug as it appears in the Fakturoid URL. One set of
	// credentials can reach several accounts, so it is never defaulted. Required.
	Slug string
	// UserAgent must carry a contact address, e.g. "MyCompany (billing@mycompany.com)".
	// Fakturoid rejects requests with a missing or generic User-Agent with a 403 on
	// every endpoint, so it has no default. Required.
	UserAgent string
	// RateLimit is the number of requests per second. Optional, defaults to 1.5
	// (~90/min).
	RateLimit *float64
}

type requiredField struct {
	key   string
	value string
}

// GetIngestrURI builds the ingestr source URI for Fakturoid.
// Format: fakturoid://?client_id=<id>&client_secret=<secret>&slug=<slug>&user_agent=<ua>[&rate_limit=<n>]
func (c *Config) GetIngestrURI() (string, error) {
	requiredFields := []requiredField{
		{"client_id", c.ClientID},
		{"client_secret", c.ClientSecret},
		{"slug", c.Slug},
		{"user_agent", c.UserAgent},
	}

	params := url.Values{}
	for _, field := range requiredFields {
		value := strings.TrimSpace(field.value)
		if value == "" {
			return "", fmt.Errorf("fakturoid: %s must be provided", field.key)
		}
		params.Set(field.key, value)
	}

	if c.RateLimit != nil {
		if *c.RateLimit <= 0 {
			return "", fmt.Errorf("fakturoid: rate_limit must be a positive number, got %v", *c.RateLimit)
		}
		params.Set("rate_limit", strconv.FormatFloat(*c.RateLimit, 'f', -1, 64))
	}

	return "fakturoid://?" + params.Encode(), nil
}
