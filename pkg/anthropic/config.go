package anthropic

type Config struct {
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	return "anthropic://?api_key=" + c.APIKey
}
