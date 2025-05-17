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
	// TODO(turtledev):
	// - validate the the defaults actually satisfy the schema
	// - make "properties" a required field for object types
	schema := map[string]any{
		"type":       "object",
		"properties": v,
	}

	_, err := varSchemaLoader().Compile(gojsonschema.NewGoLoader(schema))
	if err != nil {
		return fmt.Errorf("invalid variables schema: %w", err)
	}
	for key, value := range v {
		if _, ok := value.(map[string]any); !ok {
			return fmt.Errorf("invalid variable %q: must be an object", key)
		}
		if _, ok := value.(map[string]any)["default"]; !ok {
			return fmt.Errorf("invalid variable %q: must have a default value", key)
		}
	}
	return nil
}

func (v Variables) Value() map[string]any {
	values := make(map[string]any)
	for key, value := range v {
		if valueMap, ok := value.(map[string]any); ok {
			if defaultValue, ok := valueMap["default"]; ok {
				values[key] = defaultValue
			}
		}
	}
	return values

}
