package dataprocserverless

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
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

	// check for credentials: either service_account_json, service_account_file, or use_application_default_credentials
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

func NewClient(c Config) (*Client, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &Client{
		Config: c,
	}, nil
}
