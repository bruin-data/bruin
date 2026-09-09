package config

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestClickHouseConnectionReadOnlyRoundTrip(t *testing.T) {
	t.Parallel()
	for _, conn := range []any{&ClickHouseConnection{}} {
		require.NoError(t, yaml.Unmarshal([]byte("name: reader\nread_only: true\n"), conn))
		data, err := json.Marshal(conn)
		require.NoError(t, err)
		var values map[string]any
		require.NoError(t, json.Unmarshal(data, &values))
		require.Equal(t, true, values["read_only"])
		data, err = yaml.Marshal(conn)
		require.NoError(t, err)
		require.Contains(t, string(data), "read_only: true")
		require.NoError(t, json.Unmarshal([]byte(`{"read_only":false}`), conn))
		data, err = json.Marshal(conn)
		require.NoError(t, err)
		require.NotContains(t, string(data), "read_only")
	}
}

func TestClickHouseConnectionReadOnlySchema(t *testing.T) {
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
	for _, name := range []string{"ClickHouseConnection"} {
		def := document.Definitions[name]
		require.Equal(t, "boolean", def.Properties["read_only"].Type)
		require.NotContains(t, def.Required, "read_only")
	}
}
