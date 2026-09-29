package config

import (
	"encoding/json"
	"testing"

	"github.com/go-viper/mapstructure/v2"
	"github.com/stretchr/testify/require"
	"github.com/xeipuuv/gojsonschema"
	"gopkg.in/yaml.v3"
)

func TestClickHouseConnectionSettingsRoundTrip(t *testing.T) {
	t.Parallel()
	var conn ClickHouseConnection
	require.NoError(t, yaml.Unmarshal([]byte(`
name: warehouse
settings:
  max_threads: 4
  max_execution_time: 1.5
  join_algorithm: grace_hash
  allow_experimental_analyzer: true
`), &conn))
	expected := map[string]any{
		"max_threads":                 4,
		"max_execution_time":          1.5,
		"join_algorithm":              "grace_hash",
		"allow_experimental_analyzer": true,
	}
	require.Equal(t, expected, conn.Settings)

	data, err := yaml.Marshal(conn)
	require.NoError(t, err)
	var yamlConn ClickHouseConnection
	require.NoError(t, yaml.Unmarshal(data, &yamlConn))
	require.Equal(t, expected, yamlConn.Settings)

	data, err = json.Marshal(conn)
	require.NoError(t, err)
	var jsonConn ClickHouseConnection
	require.NoError(t, json.Unmarshal(data, &jsonConn))
	expected["max_threads"] = float64(4)
	require.Equal(t, expected, jsonConn.Settings)

	var decoded ClickHouseConnection
	require.NoError(t, mapstructure.Decode(map[string]any{"settings": expected}, &decoded))
	require.Equal(t, expected, decoded.Settings)
}

func TestClickHouseConnectionSettingsOmittedWhenEmpty(t *testing.T) {
	t.Parallel()
	for _, settings := range []map[string]any{nil, {}} {
		conn := ClickHouseConnection{Settings: settings}
		data, err := json.Marshal(conn)
		require.NoError(t, err)
		require.NotContains(t, string(data), `"settings"`)
		data, err = yaml.Marshal(conn)
		require.NoError(t, err)
		require.NotContains(t, string(data), "settings:")
	}
}

func TestClickHouseConnectionSettingsSchema(t *testing.T) {
	t.Parallel()
	schema, err := GetConnectionsSchema()
	require.NoError(t, err)
	var document struct {
		Definitions map[string]struct {
			Properties map[string]json.RawMessage `json:"properties"`
			Required   []string                   `json:"required"`
		} `json:"$defs"`
	}
	require.NoError(t, json.Unmarshal([]byte(schema), &document))
	definition := document.Definitions["ClickHouseConnection"]
	require.Contains(t, definition.Properties, "settings")
	require.NotContains(t, definition.Required, "settings")
	settingsSchema, err := gojsonschema.NewSchema(gojsonschema.NewBytesLoader(definition.Properties["settings"]))
	require.NoError(t, err)

	for _, tt := range []struct {
		name     string
		settings any
		valid    bool
	}{
		{name: "empty map", settings: map[string]any{}, valid: true},
		{name: "integer", settings: map[string]any{"max_threads": 4}, valid: true},
		{name: "float", settings: map[string]any{"max_execution_time": 1.5}, valid: true},
		{name: "boolean", settings: map[string]any{"allow_experimental_analyzer": true}, valid: true},
		{name: "string", settings: map[string]any{"join_algorithm": "grace_hash"}, valid: true},
		{name: "object value", settings: map[string]any{"max_threads": map[string]any{"value": 4}}},
		{name: "array value", settings: map[string]any{"max_threads": []any{4}}},
		{name: "null value", settings: map[string]any{"max_threads": nil}},
		{name: "scalar instead of map", settings: 4},
		{name: "array instead of map", settings: []any{4}},
		{name: "null instead of map", settings: nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := settingsSchema.Validate(gojsonschema.NewGoLoader(tt.settings))
			require.NoError(t, err)
			require.Equal(t, tt.valid, result.Valid(), "schema validation errors: %v", result.Errors())
		})
	}
}
