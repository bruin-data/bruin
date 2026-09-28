package pipeline

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xeipuuv/gojsonschema"
)

func TestCompileVariableSchemaBlocksExternalLoading(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`{"type":"integer"}`))
	}))
	t.Cleanup(server.Close)
	file := filepath.Join(t.TempDir(), "schema.json")
	require.NoError(t, os.WriteFile(file, []byte(`{"type":"integer"}`), 0o600))
	fileURL := (&url.URL{Scheme: "file", Path: file}).String()

	t.Cleanup(func() { assert.Zero(t, requests.Load()) })
	for name, document := range map[string]map[string]any{
		"http":   {"$ref": server.URL},
		"https":  {"$ref": "https://example.invalid/schema.json"},
		"file":   {"$ref": fileURL},
		"nested": {"properties": map[string]any{"value": map[string]any{"$ref": server.URL}}},
		"relative": {
			"$id": server.URL + "/root.json", "$ref": "other.json",
		},
		"other scheme": {"$ref": "ftp://example.invalid/schema.json"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Bypass SchemaDiagnostics' reference guard to exercise the actual
			// compiler boundary. An existing file/server would allow compilation
			// to succeed if the default reference loader were used accidentally.
			_, err := compileVariableSchema(gojsonschema.NewGoLoader(document))
			require.EqualError(t, err, "external schema loading is disabled for pipeline variables")
		})
	}
}

func TestCompileVariableSchemaEmbeddedMetaSchema(t *testing.T) {
	t.Parallel()
	metaSchema, err := compileVariableSchema(gojsonschema.NewStringLoader(variableMetaSchema))
	require.NoError(t, err, "the embedded meta-schema and its internal references must compile without external loading")
	for _, typ := range []string{"integer", "invalid"} {
		result, err := metaSchema.Validate(gojsonschema.NewGoLoader(map[string]any{"type": typ}))
		require.NoError(t, err)
		assert.Equal(t, typ == "integer", result.Valid())
	}
}
