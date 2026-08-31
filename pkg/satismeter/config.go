package satismeter

import (
	"fmt"
	"net/url"
	"strings"
)

// Config holds the credentials required to connect to the SatisMeter REST API v3.
type Config struct {
	// APIKey is the project-scoped SatisMeter API key (Settings > Integrations >
	// API), sent as a bearer token. Required.
	APIKey string
	// ProjectID is the project the key belongs to. Every endpoint is nested under
	// /projects/{projectId}, so it cannot be inferred from the key. Required.
	ProjectID string
}

type requiredField struct {
	key   string
	value string
}

// GetIngestrURI builds the ingestr source URI for SatisMeter.
// Format: satismeter://?api_key=<api_key>&project_id=<project_id>
func (c *Config) GetIngestrURI() (string, error) {
	requiredFields := []requiredField{
		{"api_key", c.APIKey},
		{"project_id", c.ProjectID},
	}

	params := url.Values{}
	for _, field := range requiredFields {
		value := strings.TrimSpace(field.value)
		if value == "" {
			return "", fmt.Errorf("satismeter: %s must be provided", field.key)
		}
		params.Set(field.key, value)
	}

	return "satismeter://?" + params.Encode(), nil
}
