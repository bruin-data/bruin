package adapty

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
)

// Config describes credentials for an Adapty connection.
type Config struct {
	APIKey       string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
	LookbackDays *int   `yaml:"lookback_days,omitempty" json:"lookback_days,omitempty" mapstructure:"lookback_days"`
	Timezone     string `yaml:"timezone,omitempty" json:"timezone,omitempty" mapstructure:"timezone"`
}

// GetIngestrURI builds the Adapty ingestr URI.
func (c Config) GetIngestrURI() (string, error) {
	params := url.Values{}

	apiKey := strings.TrimSpace(c.APIKey)
	if apiKey == "" {
		return "", errors.New("adapty: api_key must be provided")
	}
	params.Set("api_key", apiKey)

	if c.LookbackDays != nil {
		if *c.LookbackDays < 0 {
			return "", errors.New("adapty: lookback_days cannot be negative")
		}
		params.Set("lookback_days", strconv.Itoa(*c.LookbackDays))
	}

	if timezone := strings.TrimSpace(c.Timezone); timezone != "" {
		params.Set("timezone", timezone)
	}

	return "adapty://?" + params.Encode(), nil
}
