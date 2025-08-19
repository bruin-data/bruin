package revenuecat

type Config struct {
	APIKey    string
	ProjectID string
}

func (c *Config) GetIngestrURI() string {
	return "revenuecat://?api_key=" + c.APIKey + "&project_id=" + c.ProjectID
}
