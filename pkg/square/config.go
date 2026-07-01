package square

import "net/url"

type Config struct {
	AccessToken string
	Environment string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("access_token", c.AccessToken)
	if c.Environment != "" {
		params.Set("environment", c.Environment)
	}
	return "square://?" + params.Encode()
}
