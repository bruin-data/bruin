package lumify

import (
	"errors"
	"net/url"
	"strings"
)

// Config holds the settings required to connect to Lumify, an agent-ready sports
// intelligence API.
type Config struct {
	// APIKey is the Lumify API key (lmfy-...), sent as a bearer token. Required.
	APIKey string
	// Sport is an optional sport slug filter (e.g. nba, nfl, mlb) applied to
	// seasons, teams, players, events, and leagues.
	Sport string
	// League is an optional league slug filter applied to teams and events.
	League string
	// BaseURL overrides the API base URL. Optional, defaults to https://lumify.ai.
	BaseURL string
}

// GetIngestrURI builds the ingestr source URI for Lumify.
// Format: lumify://?api_key=<api_key>[&sport=<sport>][&league=<league>][&base_url=<url>]
func (c *Config) GetIngestrURI() (string, error) {
	apiKey := strings.TrimSpace(c.APIKey)
	if apiKey == "" {
		return "", errors.New("lumify: api_key must be provided")
	}

	params := url.Values{}
	params.Set("api_key", apiKey)
	if sport := strings.TrimSpace(c.Sport); sport != "" {
		params.Set("sport", sport)
	}
	if league := strings.TrimSpace(c.League); league != "" {
		params.Set("league", league)
	}
	if baseURL := strings.TrimSpace(c.BaseURL); baseURL != "" {
		params.Set("base_url", baseURL)
	}

	return "lumify://?" + params.Encode(), nil
}
