package okta

import (
	"fmt"
	"net/url"
	"strings"
)

// Config describes credentials for an Okta connection.
type Config struct {
	Domain string `yaml:"domain" json:"domain" mapstructure:"domain"`
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

type requiredField struct {
	key   string
	value string
}

// GetIngestrURI builds the Okta ingestr URI.
func (c *Config) GetIngestrURI() (string, error) {
	requiredFields := []requiredField{
		{"domain", c.Domain},
		{"api_key", c.APIKey},
	}

	values := make(map[string]string, len(requiredFields))
	for _, field := range requiredFields {
		value := strings.TrimSpace(field.value)
		if value == "" {
			return "", fmt.Errorf("okta: %s must be provided", field.key)
		}
		values[field.key] = value
	}

	u := &url.URL{
		Scheme:   "okta",
		Host:     values["domain"],
		RawQuery: url.Values{"api_key": {values["api_key"]}}.Encode(),
	}
	return u.String(), nil
}
