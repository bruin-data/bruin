package airtable

type Config struct {
	BaseID      string
	AccessToken string
}

func (c *Config) GetIngestrURI() string {
	return "airtable://?base_id=" + c.BaseID + "&access_token=" + c.AccessToken
}
