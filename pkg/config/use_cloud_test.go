package config

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// TestConnectionUseCloudRoundTrip verifies that use_cloud survives YAML/JSON
// round-tripping for both connection types with hand-written marshalers
// (GoogleCloudPlatform, Snowflake, Athena) and those relying on struct tags
// (Postgres). It also checks the IsCloud accessor used for query routing.
func TestConnectionUseCloudRoundTrip(t *testing.T) {
	t.Parallel()

	type cloudReader interface{ IsCloud() bool }

	cases := []struct {
		name string
		newC func() any
	}{
		{"gcp", func() any { return &GoogleCloudPlatformConnection{} }},
		{"snowflake", func() any { return &SnowflakeConnection{} }},
		{"athena", func() any { return &AthenaConnection{} }},
		{"postgres", func() any { return &PostgresConnection{} }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			conn := tc.newC()
			require.NoError(t, yaml.Unmarshal([]byte("name: warehouse\nuse_cloud: true\n"), conn))
			require.True(t, conn.(cloudReader).IsCloud())

			data, err := json.Marshal(conn)
			require.NoError(t, err)
			var values map[string]any
			require.NoError(t, json.Unmarshal(data, &values))
			require.Equal(t, true, values["use_cloud"])

			data, err = yaml.Marshal(conn)
			require.NoError(t, err)
			require.Contains(t, string(data), "use_cloud: true")

			// A connection without use_cloud omits the field from both representations.
			plain := tc.newC()
			require.NoError(t, yaml.Unmarshal([]byte("name: warehouse\n"), plain))
			require.False(t, plain.(cloudReader).IsCloud())
			data, err = json.Marshal(plain)
			require.NoError(t, err)
			require.NotContains(t, string(data), "use_cloud")
			data, err = yaml.Marshal(plain)
			require.NoError(t, err)
			require.NotContains(t, string(data), "use_cloud")
		})
	}
}

// TestGetConnectionIsCloud exercises the predicate the query command uses to
// route a connection through Bruin Cloud: GetConnection returns a typed pointer
// whose promoted IsCloud reflects use_cloud.
func TestGetConnectionIsCloud(t *testing.T) {
	t.Parallel()
	conns := &Connections{
		GoogleCloudPlatform: []GoogleCloudPlatformConnection{
			{ConnectionMetadata: ConnectionMetadata{Name: "bq-cloud"}, CloudRouting: CloudRouting{UseCloud: true}},
			{ConnectionMetadata: ConnectionMetadata{Name: "bq-local"}},
		},
	}

	cloud, ok := conns.GetConnection("bq-cloud").(interface{ IsCloud() bool })
	require.True(t, ok)
	require.True(t, cloud.IsCloud())

	local, ok := conns.GetConnection("bq-local").(interface{ IsCloud() bool })
	require.True(t, ok)
	require.False(t, local.IsCloud())

	require.Nil(t, conns.GetConnection("missing"))
}

func TestConnectionUseCloudSchema(t *testing.T) {
	t.Parallel()
	schema, err := GetConnectionsSchema()
	require.NoError(t, err)
	var document struct {
		Definitions map[string]struct {
			Properties map[string]struct {
				Type string `json:"type"`
			} `json:"properties"`
			Required []string `json:"required"`
		} `json:"$defs"`
	}
	require.NoError(t, json.Unmarshal([]byte(schema), &document))
	for _, name := range []string{"GoogleCloudPlatformConnection", "SnowflakeConnection", "PostgresConnection"} {
		def := document.Definitions[name]
		require.Equal(t, "boolean", def.Properties["use_cloud"].Type)
		require.NotContains(t, def.Required, "use_cloud")
	}

	// use_cloud is scoped to database/warehouse connections; non-database
	// connection types (e.g. ingestion sources) must not expose it.
	for _, name := range []string{"ShopifyConnection", "StripeConnection", "NotionConnection"} {
		def := document.Definitions[name]
		_, ok := def.Properties["use_cloud"]
		require.Falsef(t, ok, "%s should not expose use_cloud", name)
	}
}
