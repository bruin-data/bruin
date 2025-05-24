package attio

import "net/url"

type Config struct {
	APIKey string `yaml:"api_key,omitempty" json:"api_key,omitempty" mapstructure:"api_key"`
}

func (c Config) GetIngestrURI() string {
	u := url.Values{}
	u.Set("api_key", c.APIKey)

	return "attio://?" + u.Encode()
}
