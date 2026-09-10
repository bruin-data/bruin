package exchangeratesapi

import (
	"errors"
	"net/url"
	"strings"
)

// Config holds the settings required to connect to exchangeratesapi.io (an
// APILayer product) serving current and historical foreign exchange rates.
type Config struct {
	// AccessKey is the exchangeratesapi.io API access key. Required.
	AccessKey string
	// Base is the base currency for the returned rates, e.g. USD. Optional,
	// defaults to the API's own default (EUR). Changing the base requires a paid
	// plan.
	Base string
}

// GetIngestrURI builds the ingestr source URI for exchangeratesapi.io.
// Format: exchangeratesapi://?access_key=<access_key>[&base=<currency-code>]
func (c *Config) GetIngestrURI() (string, error) {
	accessKey := strings.TrimSpace(c.AccessKey)
	if accessKey == "" {
		return "", errors.New("exchangeratesapi: access_key must be provided")
	}

	params := url.Values{}
	params.Set("access_key", accessKey)
	if base := strings.TrimSpace(c.Base); base != "" {
		params.Set("base", strings.ToUpper(base))
	}

	return "exchangeratesapi://?" + params.Encode(), nil
}
