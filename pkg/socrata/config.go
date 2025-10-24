package socrata

import "net/url"

type Config struct {
	Domain   string
	AppToken string
	Username string
	Password string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	// Note: dataset_id is not included in the URI as it comes from source_table parameter
	if c.AppToken != "" {
		params.Set("app_token", c.AppToken)
	}
	if c.Username != "" {
		params.Set("username", c.Username)
	}
	if c.Password != "" {
		params.Set("password", c.Password)
	}

	// Domain should be in the host part of the URI, not as a query parameter
	// Format: socrata://domain?app_token=TOKEN
	uri := "socrata://"
	if c.Domain != "" {
		uri += c.Domain
	}
	if len(params) > 0 {
		uri += "?" + params.Encode()
	}

	return uri
}
