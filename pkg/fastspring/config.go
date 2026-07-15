package fastspring

import (
	"fmt"
	"net/url"
	"strings"
)

// Config describes credentials for a FastSpring connection.
type Config struct {
	Username string `yaml:"username" json:"username" mapstructure:"username"`
	Password string `yaml:"password" json:"password" mapstructure:"password"`
}

type requiredField struct {
	key   string
	value string
}

// GetIngestrURI builds the FastSpring ingestr URI.
func (c *Config) GetIngestrURI() (string, error) {
	params := url.Values{}

	requiredFields := []requiredField{
		{"username", c.Username},
		{"password", c.Password},
	}

	for _, field := range requiredFields {
		field.value = strings.TrimSpace(field.value)
		if field.value == "" {
			return "", fmt.Errorf("fastspring: %s must be provided", field.key)
		}
		params.Set(field.key, field.value)
	}

	return "fastspring://?" + params.Encode(), nil
}
