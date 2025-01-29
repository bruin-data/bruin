package github

type Config struct {
	AccessToken string
	Owner       string
	Repo        string
}

func (c *Config) GetIngestrURI() string {
	if c.AccessToken != "" {
		return "github://?access_token=" + c.AccessToken + "&owner=" + c.Owner + "&repo=" + c.Repo
	} else {
		return "github://?owner=" + c.Owner + "&repo=" + c.Repo
	}
}
