package cloudflareradar

import (
	"errors"
	"net/url"
)

type Config struct {
	APIToken string `yaml:"api_token" json:"api_token" mapstructure:"api_token"`
}

func (c *Config) GetIngestrURI() (string, error) {
	if c.APIToken == "" {
		return "", errors.New("cloudflare_radar: api_token must be provided")
	}

	params := url.Values{}
	params.Set("api_token", c.APIToken)

	return "cloudflare-radar://?" + params.Encode(), nil
}
