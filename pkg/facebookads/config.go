package facebookads

type Config struct {
	AccessToken string
	AccountID   string
}

func (c *Config) GetIngestrURI() string {
	return "facebookads://?access_token=" + c.AccessToken + "&account_id=" + c.AccountID
}
