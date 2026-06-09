package kalshi

import "net/url"

type Config struct {
	QueryParams map[string]string `yaml:"query_params,omitempty" json:"query_params,omitempty" mapstructure:"query_params"`
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	for key, value := range c.QueryParams {
		if key == "" || value == "" {
			continue
		}
		params.Set(key, value)
	}

	if len(params) == 0 {
		return "kalshi://"
	}

	return "kalshi://?" + params.Encode()
}
