package clevertap

import (
	"fmt"
	"net/url"
	"strings"
)

// Config describes credentials for a CleverTap connection.
type Config struct {
	AccountID string `yaml:"account_id" json:"account_id" mapstructure:"account_id"`
	Passcode  string `yaml:"passcode" json:"passcode" mapstructure:"passcode"`
	Region    string `yaml:"region" json:"region" mapstructure:"region"`
	Timezone  string `yaml:"timezone" json:"timezone" mapstructure:"timezone"`
}

type requiredField struct {
	key   string
	value string
}

// GetIngestrURI builds the CleverTap ingestr URI.
func (c *Config) GetIngestrURI() (string, error) {
	params := url.Values{}

	requiredFields := []requiredField{
		{"account_id", c.AccountID},
		{"passcode", c.Passcode},
	}

	for _, field := range requiredFields {
		field.value = strings.TrimSpace(field.value)
		if field.value == "" {
			return "", fmt.Errorf("clevertap: %s must be provided", field.key)
		}
		params.Set(field.key, field.value)
	}

	if region := strings.TrimSpace(c.Region); region != "" {
		params.Set("region", region)
	}
	if timezone := strings.TrimSpace(c.Timezone); timezone != "" {
		params.Set("timezone", timezone)
	}

	return "clevertap://?" + params.Encode(), nil
}
