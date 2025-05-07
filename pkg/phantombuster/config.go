package phantombuster

type Config struct {
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

func (c *Config) GetIngestrURI() string {
	return "phantombuster://?api_key=" + c.APIKey
}
