package sendgrid

import "net/url"

type Config struct {
	APIKey     string
	OnBehalfOf string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("api_key", c.APIKey)
	if c.OnBehalfOf != "" {
		params.Set("on_behalf_of", c.OnBehalfOf)
	}
	return "sendgrid://?" + params.Encode()
}
