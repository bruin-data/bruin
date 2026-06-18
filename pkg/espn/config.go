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
	if len(params) == 0 {
		return "espn://"
	}
	return "espn://?" + params.Encode()
}
