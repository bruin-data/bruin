package spanner

import (
	"net/url"
)

type Config struct {
	ProjectId         string
	InstanceId        string
	Database          string
	CredentialsBase64 string
	CredentialsPath   string
}

func (c *Config) GetIngestrURI() string {

	q := url.Values{}
	q.Set("project_id", c.ProjectId)
	q.Set("instance_id", c.InstanceId)
	q.Set("database", c.Database)
	if c.CredentialsBase64 != "" {
		q.Set("credentials_base64", c.CredentialsBase64)
	} else if c.CredentialsPath != "" {
		q.Set("credentials_path", c.CredentialsPath)
	}
	return "spanner://?" + q.Encode()
}
