package airtable

type Config struct {
	BaseId      string
	AccessToken string
}

func (c *Config) GetIngestrURI() string {
	return "airtable://?base_id=" + c.BaseId + "&access_token=" + c.AccessToken
}
