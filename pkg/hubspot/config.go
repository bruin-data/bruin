package hubspot

type Config struct {
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	return "hubspot://?api_key=" + c.APIKey
}
