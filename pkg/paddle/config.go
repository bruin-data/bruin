package paddle

type Config struct {
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	return "paddle://?api_key=" + c.APIKey
}
