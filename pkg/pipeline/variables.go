package pipeline

import (
	"fmt"

	"github.com/xeipuuv/gojsonschema"
)

func varSchemaLoader() *gojsonschema.SchemaLoader {
	loader := gojsonschema.NewSchemaLoader()
	loader.Draft = gojsonschema.Draft7
	loader.Validate = true
	return loader
}

type Variables map[string]any

func (v Variables) Validate() error {
	schema := map[string]any{
		"type":       "object",
		"properties": v,
	}

	_, err := varSchemaLoader().Compile(gojsonschema.NewGoLoader(schema))
	if err != nil {
		return fmt.Errorf("invalid variables schema: %w", err)
	}
	return nil
}

func (v Variables) Value() map[string]any {
	return v
}
