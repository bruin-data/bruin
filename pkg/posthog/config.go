package posthog

type Config struct {
	PersonalAPIKey string `yaml:"personal_api_key" json:"personal_api_key" mapstructure:"personal_api_key"`
	ProjectID      string `yaml:"project_id" json:"project_id" mapstructure:"project_id"`
}

func (c *Config) GetIngestrURI() string {
	return "posthog://?personal_api_key=" + c.PersonalAPIKey + "&project_id=" + c.ProjectID
}
