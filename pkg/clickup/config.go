package clickup

// Config describes credentials for ClickUp connection.
type Config struct {
	APIToken string `yaml:"api_token" json:"api_token" mapstructure:"api_token"`
}

// GetIngestrURI builds the ClickUp ingestr URI.
func (c *Config) GetIngestrURI() string {
	return "clickup://?api_token=" + c.APIToken
}
