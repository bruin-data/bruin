package gsheets

type Config struct {
	CredentialsPath string
}

func (c *Config) GetIngestrURI() string {
	return "gsheets://?credentials_path=" + c.CredentialsPath
}
