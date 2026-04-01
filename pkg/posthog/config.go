package posthog

import "net/url"

type Config struct {
	PersonalAPIKey string `yaml:"personal_api_key" json:"personal_api_key" mapstructure:"personal_api_key"`
	ProjectID      string `yaml:"project_id" json:"project_id" mapstructure:"project_id"`
	BaseURL        string `yaml:"base_url" json:"base_url" mapstructure:"base_url"`
}

func (c *Config) GetIngestrURI() string {
	uri := "posthog://?personal_api_key=" + c.PersonalAPIKey + "&project_id=" + c.ProjectID
	if c.BaseURL != "" {
		uri += "&base_url=" + url.QueryEscape(c.BaseURL)
	}
	return uri
}
