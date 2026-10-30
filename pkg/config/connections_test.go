package config

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestSnowflakeConnection_EndpointRoundTrip(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		host string
		port int
	}{
		{name: "defaults"},
		{name: "host only", host: "snowflake.example.com"},
		{name: "port only", port: 8443},
		{name: "host and port", host: "127.0.0.1", port: 8443},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conn := SnowflakeConnection{
				ConnectionMetadata: ConnectionMetadata{Name: "sf"},
				Account:            "my-account",
				Host:               tt.host,
				Port:               tt.port,
			}
			yamlBytes, err := yaml.Marshal(conn)
			require.NoError(t, err)
			var fromYAML SnowflakeConnection
			require.NoError(t, yaml.Unmarshal(yamlBytes, &fromYAML))
			require.Equal(t, conn, fromYAML)

			jsonBytes, err := json.Marshal(conn)
			require.NoError(t, err)
			var fromJSON SnowflakeConnection
			require.NoError(t, json.Unmarshal(jsonBytes, &fromJSON))
			require.Equal(t, conn, fromJSON)

			var yamlFields, jsonFields map[string]any
			require.NoError(t, yaml.Unmarshal(yamlBytes, &yamlFields))
			require.NoError(t, json.Unmarshal(jsonBytes, &jsonFields))
			if tt.host == "" {
				require.NotContains(t, yamlFields, "host")
				require.NotContains(t, jsonFields, "host")
			}
			if tt.port == 0 {
				require.NotContains(t, yamlFields, "port")
				require.NotContains(t, jsonFields, "port")
			} else {
				require.Equal(t, tt.port, yamlFields["port"], "port must remain a YAML integer")
			}
		})
	}
}

func TestAthenaConnection_EndpointRoundTrip(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		endpoint string
	}{
		{name: "default endpoint"},
		{name: "custom endpoint", endpoint: "https://athena.example.com"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conn := AthenaConnection{
				ConnectionMetadata: ConnectionMetadata{Name: "athena"},
				AccessKey:          "key",
				SecretKey:          "secret",
				QueryResultsPath:   "s3://results",
				Region:             "us-east-1",
				EndpointURL:        tt.endpoint,
			}
			yamlBytes, err := yaml.Marshal(conn)
			require.NoError(t, err)
			var fromYAML AthenaConnection
			require.NoError(t, yaml.Unmarshal(yamlBytes, &fromYAML))
			require.Equal(t, conn, fromYAML)

			jsonBytes, err := json.Marshal(conn)
			require.NoError(t, err)
			var fromJSON AthenaConnection
			require.NoError(t, json.Unmarshal(jsonBytes, &fromJSON))
			require.Equal(t, conn, fromJSON)

			if tt.endpoint == "" {
				require.NotContains(t, string(yamlBytes), "endpoint_url")
				require.NotContains(t, string(jsonBytes), "endpoint_url")
			}
		})
	}
}

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

func TestS3ConnectionSchemaOnlyRequiresName(t *testing.T) {
	t.Parallel()

	schema, err := GetConnectionsSchema()
	require.NoError(t, err)
	var document struct {
		Definitions map[string]struct {
			Required []string `json:"required"`
		} `json:"$defs"`
	}
	require.NoError(t, json.Unmarshal([]byte(schema), &document))
	require.Equal(t, []string{"name"}, document.Definitions["S3Connection"].Required)
}
