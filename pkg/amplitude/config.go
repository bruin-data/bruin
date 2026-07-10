package amplitude

import (
	"fmt"
	"net/url"
	"strings"
)

// Config describes credentials for Amplitude connection.
type Config struct {
	APIKey    string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
	SecretKey string `yaml:"secret_key" json:"secret_key" mapstructure:"secret_key"`
	Region    string `yaml:"region,omitempty" json:"region,omitempty" mapstructure:"region"`
}

type requiredField struct {
	key   string
	value string
}

// GetIngestrURI builds the Amplitude ingestr URI.
func (c *Config) GetIngestrURI() (string, error) {
	params := url.Values{}

	requiredFields := []requiredField{
		{"api_key", c.APIKey},
		{"secret_key", c.SecretKey},
	}

	for _, field := range requiredFields {
		field.value = strings.TrimSpace(field.value)
		if field.value == "" {
			return "", fmt.Errorf("amplitude: %s must be provided", field.key)
		}
		params.Set(field.key, field.value)
	}

	region := strings.TrimSpace(c.Region)
	if region == "" {
		region = "us"
	}
	params.Set("region", region)

	return "amplitude://?" + params.Encode(), nil
}
