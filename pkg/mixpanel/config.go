package mixpanel

import (
	"net/url"
	"strings"
)

// Config describes credentials for Mixpanel connection.
type Config struct {
	Username  string `yaml:"username" json:"username" mapstructure:"username"`
	Password  string `yaml:"password" json:"password" mapstructure:"password"`
	ProjectID string `yaml:"project_id" json:"project_id" mapstructure:"project_id"`
	Server    string `yaml:"server,omitempty" json:"server,omitempty" mapstructure:"server"`
}

// GetIngestrURI builds the Mixpanel ingestr URI.
func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("username", c.Username)
	params.Set("password", c.Password)
	params.Set("project_id", c.ProjectID)

	server := strings.TrimSpace(c.Server)
	if server == "" {
		server = "eu"
	}
	params.Set("server", server)

	return "mixpanel://?" + params.Encode()
}

