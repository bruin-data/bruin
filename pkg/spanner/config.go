package spanner

import (
	"encoding/base64"
	"net/url"
)

type Config struct {
	ProjectID                  string
	InstanceID                 string
	Database                   string
	ServiceAccountJSON         string
	ServiceAccountJSONFilePath string
}

func (c *Config) GetIngestrURI() string {
	q := url.Values{}
	q.Set("project_id", c.ProjectID)
	q.Set("instance_id", c.InstanceID)
	q.Set("database", c.Database)
	if c.ServiceAccountJSON != "" {
		creds, err := base64.StdEncoding.DecodeString(c.ServiceAccountJSON)
		if err != nil {
			return ""
		}
		q.Set("service_account_json", string(creds))
	} else if c.ServiceAccountJSONFilePath != "" {
		q.Set("service_account_file_path", c.ServiceAccountJSONFilePath)
	}
	return "spanner://?" + q.Encode()
}
