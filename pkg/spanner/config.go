package spanner

import (
	"encoding/base64"
	"net/url"
)

type Config struct {
	ProjectID          string
	InstanceID         string
	Database           string
	ServiceAccountJSON string
	ServiceAccountFile string
}

func (c *Config) GetIngestrURI() string {
	q := url.Values{}
	q.Set("project_id", c.ProjectID)
	q.Set("instance_id", c.InstanceID)
	q.Set("database", c.Database)

	if c.ServiceAccountJSON != "" {
		creds := base64.StdEncoding.EncodeToString([]byte(c.ServiceAccountJSON))
		q.Set("credentials_base64", creds)
	} else if c.ServiceAccountFile != "" {
		q.Set("credentials_path", c.ServiceAccountFile)
	}
	return "spanner://?" + q.Encode()
}
