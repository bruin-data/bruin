package fundraiseup

type Config struct {
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	return "fundraiseup://?api_key=" + c.APIKey
}
