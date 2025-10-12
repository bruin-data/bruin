package mailchimp

type Config struct {
	APIKey string
	Server string
}

func (c *Config) GetIngestrURI() string {
	return "mailchimp://?api_key=" + c.APIKey + "&server=" + c.Server
}
