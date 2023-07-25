package bigquery

import (
	"golang.org/x/oauth2/google"
)

type Config struct {
	ProjectID           string `envconfig:"BIGQUERY_PROJECT"`
	CredentialsFilePath string `envconfig:"BIGQUERY_CREDENTIALS_FILE"`
	CredentialsJSON     string
	Credentials         *google.Credentials
	Location            string `envconfig:"BIGQUERY_LOCATION"`
}

func (c Config) IsValid() bool {
	return c.ProjectID != "" && c.CredentialsFilePath != ""
}
