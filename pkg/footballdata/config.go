package footballdata

import (
	"errors"
	"net/url"
)

type Config struct {
	APIKey         string
	Competition    string
	Season         string
	BaseURL        string
	Matchday       string
	Status         string
	Stage          string
	Group          string
	UnfoldGoals    bool
	UnfoldBookings bool
	UnfoldSubs     bool
	UnfoldLineups  bool
}

func (c *Config) GetIngestrURI() (string, error) {
	if c.APIKey == "" {
		return "", errors.New("footballdata: api_key must be provided")
	}

	params := url.Values{}
	params.Set("api_key", c.APIKey)
	if c.Competition != "" {
		params.Set("competition", c.Competition)
	}
	if c.Season != "" {
		params.Set("season", c.Season)
	}
	if c.BaseURL != "" {
		params.Set("base_url", c.BaseURL)
	}
	if c.Matchday != "" {
		params.Set("matchday", c.Matchday)
	}
	if c.Status != "" {
		params.Set("status", c.Status)
	}
	if c.Stage != "" {
		params.Set("stage", c.Stage)
	}
	if c.Group != "" {
		params.Set("group", c.Group)
	}
	if c.UnfoldGoals {
		params.Set("unfold_goals", "true")
	}
	if c.UnfoldBookings {
		params.Set("unfold_bookings", "true")
	}
	if c.UnfoldSubs {
		params.Set("unfold_subs", "true")
	}
	if c.UnfoldLineups {
		params.Set("unfold_lineups", "true")
	}
	return "footballdata://?" + params.Encode(), nil
}
