package twocheckout

import (
	"fmt"
	"net/url"
	"strings"
)

// Config describes credentials for a 2Checkout (Verifone) connection.
type Config struct {
	MerchantCode string `yaml:"merchant_code" json:"merchant_code" mapstructure:"merchant_code"`
	SecretKey    string `yaml:"secret_key" json:"secret_key" mapstructure:"secret_key"`
	// BaseURL overrides the default 2Checkout REST API host. Optional.
	BaseURL string `yaml:"base_url" json:"base_url" mapstructure:"base_url"`
}

type requiredField struct {
	key   string
	value string
}

// GetIngestrURI builds the 2Checkout ingestr URI.
//
// The scheme is `twocheckout`, not `2checkout`: RFC 3986 requires a URI scheme
// to begin with a letter, and a leading digit breaks url.Parse.
func (c *Config) GetIngestrURI() (string, error) {
	params := url.Values{}

	requiredFields := []requiredField{
		{"merchant_code", c.MerchantCode},
		{"secret_key", c.SecretKey},
	}

	for _, field := range requiredFields {
		field.value = strings.TrimSpace(field.value)
		if field.value == "" {
			return "", fmt.Errorf("twocheckout: %s must be provided", field.key)
		}
		params.Set(field.key, field.value)
	}

	if baseURL := strings.TrimSpace(c.BaseURL); baseURL != "" {
		params.Set("base_url", baseURL)
	}

	return "twocheckout://?" + params.Encode(), nil
}
