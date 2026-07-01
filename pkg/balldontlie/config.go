package balldontlie

import (
	"errors"
	"net/url"
)

type Config struct {
	APIKey  string
	Season  string
	BaseURL string
}

func (c *Config) GetIngestrURI() (string, error) {
	if c.APIKey == "" {
		return "", errors.New("balldontlie: api_key must be provided")
	}

	params := url.Values{}
	params.Set("api_key", c.APIKey)
	if c.Season != "" {
		params.Set("season", c.Season)
	}
	if c.BaseURL != "" {
		params.Set("base_url", c.BaseURL)
	}
	return "balldontlie://?" + params.Encode(), nil
}
