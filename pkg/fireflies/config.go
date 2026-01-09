package fireflies

type Config struct {
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	return "fireflies://?api_key=" + c.APIKey
}
