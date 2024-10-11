package appsflyer

type Config struct {
	ApiKey string
}

func (c *Config) GetIngestrURI() string {
	return "appsflyer://?api_key=" + c.ApiKey
}
