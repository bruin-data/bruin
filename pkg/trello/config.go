package trello

import "net/url"

type Config struct {
	APIKey string
	Token  string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("api_key", c.APIKey)
	params.Set("token", c.Token)

	uri := url.URL{
		Scheme:   "trello",
		RawQuery: params.Encode(),
	}
	return uri.String()
}
