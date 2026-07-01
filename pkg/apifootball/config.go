package apifootball

import "net/url"

type Config struct {
	APIKey   string
	League   string
	Season   string
	Timezone string
	BaseURL  string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("api_key", c.APIKey)
	if c.League != "" {
		params.Set("league", c.League)
	}
	if c.Season != "" {
		params.Set("season", c.Season)
	}
	if c.Timezone != "" {
		params.Set("timezone", c.Timezone)
	}
	if c.BaseURL != "" {
		params.Set("base_url", c.BaseURL)
	}
	return "apifootball://?" + params.Encode()
}
