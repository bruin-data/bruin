package klaviyo

type Config struct {
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	return "klaviyo://?api_key=" + c.APIKey
}
