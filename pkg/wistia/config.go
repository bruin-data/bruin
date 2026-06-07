package wistia

import "net/url"

// Config describes credentials and optional settings for Wistia.
type Config struct {
	AccessToken string `yaml:"access_token" json:"access_token" mapstructure:"access_token"`
	APIKey      string `yaml:"api_key,omitempty" json:"api_key,omitempty" mapstructure:"api_key"`
	Token       string `yaml:"token,omitempty" json:"token,omitempty" mapstructure:"token"`
	APIVersion  string `yaml:"api_version,omitempty" json:"api_version,omitempty" mapstructure:"api_version"`
	BaseURL     string `yaml:"base_url,omitempty" json:"base_url,omitempty" mapstructure:"base_url"`
}

// GetIngestrURI builds the Wistia ingestr URI.
func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("access_token", c.authToken())
	if c.APIVersion != "" {
		params.Set("api_version", c.APIVersion)
	}
	if c.BaseURL != "" {
		params.Set("base_url", c.BaseURL)
	}
	return "wistia://?" + params.Encode()
}

func (c *Config) authToken() string {
	if c.AccessToken != "" {
		return c.AccessToken
	}
	if c.APIKey != "" {
		return c.APIKey
	}
	return c.Token
}
