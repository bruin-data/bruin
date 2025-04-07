package pipedrive

import "fmt"

type Config struct {
	ApiToken string `yaml:"api_token" json:"api_token" mapstructure:"api_token"`
}

func (c *Config) GetIngestrURI() string {
	return fmt.Sprintf("pipedrive://?api_token=%s", c.ApiToken)
}
