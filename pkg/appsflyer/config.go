package appsflyer

type Config struct {
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	return "appsflyer://?api_key=" + c.APIKey
}
