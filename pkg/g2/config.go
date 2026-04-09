package g2

type Config struct {
	APIToken string
}

func (c *Config) GetIngestrURI() string {
	return "g2://?api_token=" + c.APIToken
}
