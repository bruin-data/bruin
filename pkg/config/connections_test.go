package config

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestGoogleCloudPlatformConnection_AccessTokenRoundTrip(t *testing.T) {
	t.Parallel()

	conn := GoogleCloudPlatformConnection{
		ConnectionMetadata: ConnectionMetadata{Name: "gcp-oauth"},
		ProjectID:          "project-id",
		Location:           "EU",
		AccessToken:        "ya29.some-token",
	}

	yamlBytes, err := yaml.Marshal(conn)
	require.NoError(t, err)

	var fromYaml GoogleCloudPlatformConnection
	require.NoError(t, yaml.Unmarshal(yamlBytes, &fromYaml))
	require.Equal(t, "ya29.some-token", fromYaml.AccessToken)
	require.Equal(t, "project-id", fromYaml.ProjectID)

	jsonBytes, err := json.Marshal(conn)
	require.NoError(t, err)

	var fromJSON map[string]interface{}
	require.NoError(t, json.Unmarshal(jsonBytes, &fromJSON))
	require.Equal(t, "ya29.some-token", fromJSON["access_token"])
}
