package stripe

type Config struct {
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	return "stripe://?api_key=" + c.APIKey

}
