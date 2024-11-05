package slack

type Config struct {
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	return "slack://?api_key=" + c.APIKey
}
