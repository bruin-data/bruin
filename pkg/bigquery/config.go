package bigquery

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"

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
	// Valid if we have a project ID and at least one credential method
	// (including Application Default Credentials when no explicit credentials are provided)
	return c.ProjectID != "" && (c.CredentialsFilePath != "" || c.CredentialsJSON != "" || c.Credentials != nil || c.UsesApplicationDefaultCredentials())
}

func (c Config) GetConnectionURI() (string, error) {
	var creds []byte
	switch {
	case c.CredentialsJSON != "":
		creds = []byte(c.CredentialsJSON)
	case c.CredentialsFilePath != "":
		var err error
		creds, err = os.ReadFile(c.CredentialsFilePath)
		if err != nil {
			return "", err
		}
	case c.Credentials != nil:
		return "", errors.New("only `service_account_json` or `service_account_file` supported")
	case c.UsesApplicationDefaultCredentials():
		// For Application Default Credentials, we don't need to encode credentials
		// The BigQuery client will automatically use ADC
		URI := fmt.Sprintf("bigquery://%s", c.ProjectID)
		if c.Location != "" {
			URI += "?location=" + c.Location
		}
		return URI, nil
	default:
		return "", errors.New("could not find google credentials")
	}

	URI := fmt.Sprintf("bigquery://%s?credentials_base64=%s", c.ProjectID, base64.StdEncoding.EncodeToString(creds))

	if c.Location != "" {
		URI += "&location=" + c.Location
	}

	return URI, nil
}

func (c Config) GetIngestrURI() (string, error) {
	return c.GetConnectionURI()
}

// UsesApplicationDefaultCredentials returns true if no explicit credentials are provided
// and the configuration should use Application Default Credentials (gcloud auth)
func (c Config) UsesApplicationDefaultCredentials() bool {
	return c.CredentialsFilePath == "" && c.CredentialsJSON == "" && c.Credentials == nil
}
