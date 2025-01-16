package linkedinads

type Config struct {
	AccessToken string
	AccountIds  string
}

func (c *Config) GetIngestrURI() string {
	return "linkedinads://?access_token=" + c.AccessToken + "&account_ids=" + c.AccountIds
}
