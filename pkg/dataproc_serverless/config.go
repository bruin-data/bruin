package dataprocserverless

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"google.golang.org/api/option"
)

type MissingFieldsError struct {
	Fields []string
}

func (e *MissingFieldsError) Error() string {
	return fmt.Sprintf("missing required fields: %v", strings.Join(e.Fields, ", "))
}

type Config struct {
	config.GoogleCloudPlatformConnection
	Workspace string `yaml:"workspace"`
}

func (c *Config) validate() error {
	missing := []string{}

	if c.ProjectID == "" {
		missing = append(missing, "project_id")
	}
	if c.Location == "" {
		missing = append(missing, "location")
	}
	if c.Workspace == "" {
		missing = append(missing, "workspace")
	}

	hasCredentials := c.ServiceAccountJSON != "" || c.ServiceAccountFile != "" || c.UseApplicationDefaultCredentials
	if !hasCredentials {
		missing = append(missing, "service_account_json, service_account_file, or use_application_default_credentials")
	}

	if len(missing) > 0 {
		return &MissingFieldsError{Fields: missing}
	}
	return nil
}

type Client struct {
	Config
}

// getClientOptions returns the appropriate options for GCP client authentication.
// Returns an empty slice when using Application Default Credentials.
func (c *Client) getClientOptions() []option.ClientOption {
	if c.ServiceAccountJSON != "" {
		return []option.ClientOption{option.WithCredentialsJSON([]byte(c.ServiceAccountJSON))}
	}
	if c.ServiceAccountFile != "" {
		return []option.ClientOption{option.WithCredentialsFile(c.ServiceAccountFile)}
	}
	// Use Application Default Credentials - no explicit option needed
	return []option.ClientOption{}
}

func NewClient(c Config) (*Client, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &Client{
		Config: c,
	}, nil
}
