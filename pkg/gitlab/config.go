package gitlab

import "net/url"

type Config struct {
	AccessToken string
	BaseURL     string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("access_token", c.AccessToken)
	if c.BaseURL != "" {
		params.Set("base_url", c.BaseURL)
	}
	return "gitlab://?" + params.Encode()
}
