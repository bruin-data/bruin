package docebo

type Config struct {
	BaseURL      string
	ClientID     string
	ClientSecret string
	Username     string
	Password     string
}

func (c *Config) GetIngestrURI() string {
	return "docebo://?base_url=" + c.BaseURL +
		"&client_id=" + c.ClientID +
		"&client_secret=" + c.ClientSecret +
		"&username=" + c.Username +
		"&password=" + c.Password
}
