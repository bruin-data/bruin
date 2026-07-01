package espn

import (
	"net/url"
	"strconv"
)

type Config struct {
	Sport   string
	League  string
	Season  string
	Limit   int
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
	if c.Season != "" {
		params.Set("season", c.Season)
	}
	if c.Limit > 0 {
		params.Set("limit", strconv.Itoa(c.Limit))
	}
	if c.BaseURL != "" {
		params.Set("base_url", c.BaseURL)
	}
	if len(params) == 0 {
		return "espn://"
	}
	return "espn://?" + params.Encode()
}
