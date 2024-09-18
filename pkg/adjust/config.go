package adjust

type Config struct {
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	return "adjust://?api_key=" + c.APIKey
}
