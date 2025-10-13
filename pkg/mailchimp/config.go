package mailchimp

import "net/url"

type Config struct {
	APIKey string
	Server string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	if c.APIKey != "" {
		params.Set("api_key", c.APIKey)
	}
	if c.Server != "" {
		params.Set("server", c.Server)
	}

	uri := "mailchimp://"
	if len(params) > 0 {
		uri += "?" + params.Encode()
	}

	return uri
}
