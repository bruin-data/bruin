package revenuecat

type Config struct {
	ApiKey    string
	ProjectId string
}

func (c *Config) GetIngestrURI() string {
	return "revenuecat://?api_key=" + c.ApiKey + "&project_id=" + c.ProjectId
}
