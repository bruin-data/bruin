package espn

import "net/url"

type Config struct {
	Sport   string
	League  string
	BaseURL string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	if c.Sport != "" {
		params.Set("sport", c.Sport)
	}
	if c.League != "" {
		params.Set("league", c.League)
	}
	if c.BaseURL != "" {
		params.Set("base_url", c.BaseURL)
	}
	uri := url.URL{
		Scheme:   "espn",
		RawQuery: params.Encode(),
	}
	return uri.String()
}
