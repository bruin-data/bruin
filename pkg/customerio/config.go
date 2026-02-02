package customerio

import "net/url"

type Config struct {
	APIKey string
	Region string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("api_key", c.APIKey)
	if c.Region != "" {
		params.Set("region", c.Region)
	}
	uri := url.URL{
		Scheme:   "customerio",
		RawQuery: params.Encode(),
	}
	return uri.String()
}
