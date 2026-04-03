package dune

type Config struct {
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	return "dune://?api_key=" + c.APIKey
}
