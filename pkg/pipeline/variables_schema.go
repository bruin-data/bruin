package pipeline

import (
	_ "embed"
	"errors"

	"github.com/xeipuuv/gojsonschema"
)

// variableMetaSchema is the Draft 7 meta-schema bundled with gojsonschema v1.2.0.
// It is embedded here so compilation never needs a URL or file loader, even
// when validating the meta-schema itself. See schemas/NOTICE for attribution.
//
//go:embed schemas/draft-07.json
var variableMetaSchema string

func compileVariableSchema(document gojsonschema.JSONLoader) (*gojsonschema.Schema, error) {
	loader := gojsonschema.NewSchemaLoader()
	loader.AutoDetect = false
	loader.Draft = gojsonschema.Draft7
	// Validate against the isolated embedded meta-schema in SchemaDiagnostics.
	// Automatic validation would install the library's default reference loader.
	loader.Validate = false
	return loader.Compile(variableJSONLoader{JSONLoader: document})
}

type variableJSONLoader struct {
	gojsonschema.JSONLoader
}

func (variableJSONLoader) LoaderFactory() gojsonschema.JSONLoaderFactory { //nolint:ireturn // Required by the gojsonschema.JSONLoader interface.
	return variableJSONLoaderFactory{}
}

type variableJSONLoaderFactory struct{}

func (variableJSONLoaderFactory) New(_ string) gojsonschema.JSONLoader {
	return blockedVariableReference{JSONLoader: gojsonschema.NewStringLoader(`{}`)}
}

type blockedVariableReference struct {
	gojsonschema.JSONLoader
}

func (blockedVariableReference) LoadJSON() (any, error) {
	return nil, errors.New("external schema loading is disabled for pipeline variables")
}
