package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/xeipuuv/gojsonschema"
)

func varSchemaLoader() *gojsonschema.SchemaLoader {
	loader := gojsonschema.NewSchemaLoader()
	// gojsonschema bundles the Draft 7 meta-schema under its canonical HTTP URL.
	// Disabling auto-detection makes the loader use that bundled schema instead of
	// trying to fetch the HTTPS URL returned by Variables.Schema.
	loader.AutoDetect = false
	loader.Draft = gojsonschema.Draft7
	loader.Validate = true
	return loader
}

type Variables map[string]map[string]any

func (v *Variables) Validate() error {
	// The missing-default check runs first because its message is far more
	// actionable than the meta-schema complaint an empty variable body produces.
	for _, key := range sortedVariableKeys(*v) {
		value := (*v)[key]
		if _, ok := value["default"]; !ok {
			return fmt.Errorf("invalid variable %q: must have a default value", key)
		}
	}

	schema, err := v.compiledSchema()
	if err != nil {
		return err
	}

	if err := validateVariableValues(schema, v.Value()); err != nil {
		return fmt.Errorf("invalid variable defaults: %w", err)
	}
	return nil
}

func (v *Variables) Value() map[string]any {
	values := make(map[string]any)
	for key, value := range *v {
		if defaultValue, ok := value["default"]; ok {
			values[key] = defaultValue
		}
	}
	return values
}

func (v *Variables) SchemaMap() map[string]any {
	schema := make(map[string]any)
	for key, value := range *v {
		def := make(map[string]any)
		for k, val := range value {
			if k != "default" {
				def[k] = val
			}
		}
		schema[key] = def
	}
	return schema
}

// Schema returns the JSON Schema describing the pipeline's variables. It is built
// from SchemaMap so that the declared defaults stay out of the document: a default
// is data, and embedding it here makes an unmarshallable value (a YAML map with
// non-string keys, say) surface as a bogus "invalid variables schema" error.
func (v *Variables) Schema() any {
	return map[string]any{
		"$schema":    "https://json-schema.org/draft-07/schema",
		"type":       "object",
		"properties": v.SchemaMap(),
	}
}

func (v *Variables) Merge(other map[string]any) error {
	if len(other) == 0 {
		return nil
	}

	for _, key := range sortedVariableKeys(other) {
		if _, ok := (*v)[key]; !ok {
			return fmt.Errorf("no such variable %q", key)
		}
	}

	schema, err := v.compiledSchema()
	if err != nil {
		return err
	}
	// Callers already describe the context (a --var override, a variant), so the
	// validation error is returned as-is to avoid stuttering messages.
	if err := validateVariableValues(schema, other); err != nil {
		return err
	}

	for key, value := range other {
		// A variable declared with an empty body decodes to a nil map, which cannot
		// be assigned to.
		if (*v)[key] == nil {
			(*v)[key] = make(map[string]any, 1)
		}
		(*v)[key]["default"] = value
	}
	return nil
}

func (v *Variables) compiledSchema() (*gojsonschema.Schema, error) {
	schema, err := varSchemaLoader().Compile(gojsonschema.NewGoLoader(v.Schema()))
	if err != nil {
		return nil, fmt.Errorf("invalid variables schema: %s", flattenMessages(strings.Split(err.Error(), "\n")))
	}
	return schema, nil
}

func validateVariableValues(schema *gojsonschema.Schema, values map[string]any) error {
	result, err := schema.Validate(gojsonschema.NewGoLoader(values))
	if err != nil {
		// YAML mappings with non-string keys decode to map[any]any, which has no JSON
		// representation. Say so, because the raw error names only the Go type.
		var unsupported *json.UnsupportedTypeError
		if errors.As(err, &unsupported) {
			return fmt.Errorf("value is not JSON-encodable (%s): object keys must be strings, so quote them", unsupported.Type)
		}
		return fmt.Errorf("failed to validate values: %w", err)
	}
	if result.Valid() {
		return nil
	}

	messages := make([]string, 0, len(result.Errors()))
	for _, validationErr := range result.Errors() {
		messages = append(messages, validationErr.String())
	}
	return fmt.Errorf("schema validation failed: %s", flattenMessages(messages))
}

// flattenMessages turns a set of validation messages into a single sorted line.
// gojsonschema reports meta-schema failures as a newline-joined string with a
// trailing newline, which renders badly once the error becomes a lint issue.
func flattenMessages(messages []string) string {
	cleaned := make([]string, 0, len(messages))
	for _, message := range messages {
		if message = strings.TrimSpace(message); message != "" {
			cleaned = append(cleaned, message)
		}
	}
	sort.Strings(cleaned)
	return strings.Join(cleaned, "; ")
}

func sortedVariableKeys[V any](variables map[string]V) []string {
	keys := make([]string, 0, len(variables))
	for key := range variables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
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
