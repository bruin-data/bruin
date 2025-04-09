package pipedrive

type Config struct {
	APIToken string `yaml:"api_token" json:"api_token" mapstructure:"api_token"`
}

func (c *Config) GetIngestrURI() string {
	return "pipedrive://?api_token=" + c.APIToken
}
