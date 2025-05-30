package spanner

import (
	"net/url"
)

type Config struct {
	ProjectID         string
	InstanceID        string
	Database          string
	CredentialsBase64 string
	CredentialsPath   string
}

func (c *Config) GetIngestrURI() string {
	q := url.Values{}
	q.Set("project_id", c.ProjectID)
	q.Set("instance_id", c.InstanceID)
	q.Set("database", c.Database)
	if c.CredentialsBase64 != "" {
		q.Set("credentials_base64", c.CredentialsBase64)
	} else if c.CredentialsPath != "" {
		q.Set("credentials_path", c.CredentialsPath)
	}
	return "spanner://?" + q.Encode()
}
