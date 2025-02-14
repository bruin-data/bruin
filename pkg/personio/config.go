package personio

import "fmt"

type Config struct {
	ClientID     string `yaml:"client_id" json:"client_id" mapstructure:"client_id"`
	ClientSecret string `yaml:"client_secret" json:"client_secret" mapstructure:"client_secret"`
}

func (c *Config) GetIngestrURI() string {
	return fmt.Sprintf("personio://?client_id=%s&client_secret=%s", c.ClientID, c.ClientSecret)
}
