package pipeline

import (
	"encoding/json"
	"fmt"

	"github.com/xeipuuv/gojsonschema"
)

func varSchemaLoader() *gojsonschema.SchemaLoader { //nolint:unused
	loader := gojsonschema.NewSchemaLoader()
	loader.Draft = gojsonschema.Draft7
	loader.Validate = true
	return loader
}

type Variables map[string]map[string]any

func (v Variables) Validate() error {
	// TODO(turtledev):
	// - validate the defaults actually satisfy the schema
	// - make "properties" a required field for object types
	//
	// BUG: Schema compiler fetches the schema from the spec URL, which may break
	// in environments where internet access is restricted.

	// TODO: Use a local copy of the schema.
	// _, err := varSchemaLoader().Compile(gojsonschema.NewGoLoader(v.Schema()))
	// if err != nil {
	// 	return fmt.Errorf("invalid variables schema: %w", err)
	// }
	for key, value := range v {
		if _, ok := value["default"]; !ok {
			return fmt.Errorf("invalid variable %q: must have a default value", key)
		}
	}
	return nil
}

func (v Variables) Value() map[string]any {
	values := make(map[string]any)
	for key, value := range v {
		if defaultValue, ok := value["default"]; ok {
			values[key] = defaultValue
		}
	}
	return values
}

func (v Variables) Schema() any {
	return map[string]any{
		"$schema":    "https://json-schema.org/draft-07/schema",
		"type":       "object",
		"properties": v,
	}
}

func (v Variables) Merge(other map[string]any) error {
	for key, value := range other {
		if _, ok := v[key]; !ok {
			return fmt.Errorf("no such variable %q", key)
		}
		v[key]["default"] = value
	}
	return nil
}

// This ensures that when an empty object {} is provided, it clears the variables.
func (v *Variables) UnmarshalJSON(data []byte) error {
	*v = make(Variables)

	if len(data) == 0 || string(data) == "{}" {
		return nil
	}

	var temp map[string]map[string]any
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	*v = Variables(temp)
	return nil
}
