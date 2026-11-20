package clickup

import "net/url"

// Config describes credentials for ClickUp connection.
type Config struct {
	APIToken string `yaml:"api_token" json:"api_token" mapstructure:"api_token"`
}

// GetIngestrURI builds the ClickUp ingestr URI. Ingestr reads the token from the api_key parameter.
func (c *Config) GetIngestrURI() string {
	return "clickup://?" + url.Values{"api_key": {c.APIToken}}.Encode()
}
