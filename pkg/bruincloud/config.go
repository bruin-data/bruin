package bruincloud

import (
	"net/url"
)

type Config struct {
	APIToken string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("api_token", c.APIToken)

	uri := url.URL{
		Scheme:   "bruin",
		RawQuery: params.Encode(),
	}
	return uri.String()
}
