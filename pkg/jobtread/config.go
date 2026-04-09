package jobtread

type Config struct {
	GrantKey       string `yaml:"grant_key" json:"grant_key" mapstructure:"grant_key"`
	OrganizationID string `yaml:"organization_id" json:"organization_id" mapstructure:"organization_id"`
}

func (c *Config) GetIngestrURI() string {
	return "jobtread://?grant_key=" + c.GrantKey + "&organization_id=" + c.OrganizationID
}
