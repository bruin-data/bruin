package googleads

import (
	"encoding/base64"
	"net/url"
	"strings"
)

type Config struct {
	CustomerID         string
	DeveloperToken     string
	ServiceAccountFile string
	ServiceAccountJSON string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("dev_token", c.DeveloperToken)

	svcFile := strings.TrimSpace(c.ServiceAccountFile)
	svcJSON := strings.TrimSpace(c.ServiceAccountJSON)
	if svcFile != "" {
		params.Set("credentials_path", svcFile)
	} else if svcJSON != "" {
		params.Set(
			"credentials_base64",
			base64.StdEncoding.EncodeToString(
				[]byte(svcJSON),
			),
		)
	}

	uri := url.URL{
		Scheme:   "googleads",
		Host:     strings.TrimSpace(c.CustomerID),
		RawQuery: params.Encode(),
	}
	return uri.String()
}
