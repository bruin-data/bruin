package smartsheet

import (
	"net/url"
)

type Config struct {
	AccessToken string
}

func (c *Config) GetIngestrURI() string {
	q := url.Values{}
	q.Set("access_token", c.AccessToken)
	return "smartsheet://?" + q.Encode()
}
