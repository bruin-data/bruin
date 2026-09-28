package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/xeipuuv/gojsonschema"
)

type Variables map[string]map[string]any

func (v *Variables) Validate() error {
	// Keep runtime validation backwards-compatible. Schema and default checks
	// are opt-in diagnostics used by the warning-only lint rule.
	for key, value := range *v {
		if _, ok := value["default"]; !ok {
			return fmt.Errorf("invalid variable %q: must have a default value", key)
		}
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
	schema := make(map[string]any, len(*v))
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

func (v *Variables) Schema() any {
	return map[string]any{
		"$schema":    "https://json-schema.org/draft-07/schema",
		"type":       "object",
		"properties": *v,
	}
}

// SchemaDiagnostics validates each variable's normalized Draft 7 schema and
// value on a private copy, without changing the schemas exported to live jobs.
// Variables are checked independently so one invalid variable cannot hide
// problems in the others. Referenced schemas are skipped before compilation
// because local cycles can overflow gojsonschema's stack. The loader also
// blocks all external lookups. This must remain separate from runtime
// Validate, Merge, Schema and SchemaMap.
func (v *Variables) SchemaDiagnostics() []string {
	diagnostics := make([]string, 0)
	if len(*v) == 0 {
		return diagnostics
	}

	metaSchema, err := compiledVariableMetaSchema()
	if err != nil {
		return append(diagnostics, "failed to load variable meta-schema: "+err.Error())
	}

	schemas := v.SchemaMap()
	for _, key := range sortedVariableKeys(*v) {
		diagnostics = append(diagnostics, variableSchemaDiagnosticsFor(metaSchema, "variables."+key, schemas[key], (*v)[key])...)
	}
	return diagnostics
}

func variableSchemaDiagnosticsFor(metaSchema *gojsonschema.Schema, path string, schema any, definition map[string]any) []string {
	// Canonicalize Go maps/slices to JSON containers before walking schemas.
	// This also handles typed maps supplied by callers, so they cannot bypass
	// the reference guard. NewGoLoader.LoadJSON only marshals/unmarshals data.
	document, err := gojsonschema.NewGoLoader(schema).LoadJSON()
	if err != nil {
		return []string{path + ": invalid schema: " + describeJSONEncodingError(err)}
	}

	var state variableSchemaDiagnostics
	document = normalizeVariableSchema(document, path, &state)
	diagnostics := state.messages
	if state.hasReferences {
		return append(diagnostics, path+": schema and value validation skipped because $ref is not supported by warning validation")
	}

	metaResult, err := metaSchema.Validate(gojsonschema.NewGoLoader(document))
	if err != nil {
		return append(diagnostics, path+": invalid schema: "+err.Error())
	}
	if !metaResult.Valid() {
		return append(diagnostics, path+": invalid schema: "+formatVariableSchemaErrors(metaResult.Errors(), "schema"))
	}

	compiled, err := compileVariableSchema(gojsonschema.NewGoLoader(document))
	if err != nil {
		return append(diagnostics, path+": invalid schema: "+flattenMessages(strings.Split(err.Error(), "\n")))
	}

	value, ok := definition["default"]
	if !ok {
		return diagnostics
	}
	result, err := compiled.Validate(gojsonschema.NewGoLoader(value))
	if err != nil {
		return append(diagnostics, path+": failed to validate value: "+describeJSONEncodingError(err))
	}
	if !result.Valid() {
		diagnostics = append(diagnostics, path+": value does not satisfy its schema: "+formatVariableSchemaErrors(result.Errors(), "value"))
	}
	return diagnostics
}

func describeJSONEncodingError(err error) string {
	var unsupported *json.UnsupportedTypeError
	if errors.As(err, &unsupported) {
		if unsupported.Type.Kind() == reflect.Map {
			return fmt.Sprintf("value is not JSON-encodable (%s): object keys must be strings", unsupported.Type)
		}
		return fmt.Sprintf("value is not JSON-encodable (%s)", unsupported.Type)
	}
	return flattenMessages(strings.Split(err.Error(), "\n"))
}

// formatVariableSchemaErrors reports errors relative to the variable being
// checked, since each variable is validated as its own root document. Some
// gojsonschema descriptions embed the "(root)" context, so name it instead.
func formatVariableSchemaErrors(errs []gojsonschema.ResultError, root string) string {
	messages := make([]string, 0, len(errs))
	for _, validationErr := range errs {
		description := strings.ReplaceAll(validationErr.Description(), gojsonschema.STRING_CONTEXT_ROOT, root)
		if field := validationErr.Field(); field != gojsonschema.STRING_CONTEXT_ROOT {
			description = field + ": " + description
		}
		messages = append(messages, description)
	}
	return flattenMessages(messages)
}

type variableSchemaDiagnostics struct {
	messages      []string
	hasReferences bool
}

// normalizeVariableSchema receives a value that is known to be a schema. It
// follows schema-bearing Draft 7 keywords explicitly so an ordinary object in
// enum or const is never mistaken for a schema merely because it has a "type"
// property.
func normalizeVariableSchema(value any, path string, diagnostics *variableSchemaDiagnostics) any {
	switch typed := value.(type) {
	case map[string]any:
		normalized := make(map[string]any, len(typed))
		for _, key := range sortedVariableKeys(typed) {
			childPath := path + "." + key
			switch key {
			case "$ref":
				diagnostics.hasReferences = true
				normalized[key] = typed[key]
			case "type":
				normalized[key] = normalizeVariableSchemaType(typed[key], childPath, diagnostics)
			case "properties", "patternProperties", "definitions", "$defs":
				normalized[key] = normalizeVariableSchemaMap(typed[key], childPath, diagnostics)
			case "allOf", "anyOf", "oneOf":
				normalized[key] = normalizeVariableSchemaList(typed[key], childPath, diagnostics)
			case "items":
				normalized[key] = normalizeVariableSchemaOrList(typed[key], childPath, diagnostics)
			case "additionalItems", "additionalProperties", "contains", "else", "if", "not", "propertyNames", "then":
				normalized[key] = normalizeVariableSchema(typed[key], childPath, diagnostics)
			case "dependencies":
				normalized[key] = normalizeVariableSchemaDependencies(typed[key], childPath, diagnostics)
			default:
				normalized[key] = cloneVariableSchemaValue(typed[key])
			}
		}
		return normalized
	default:
		return value
	}
}

func normalizeVariableSchemaMap(value any, path string, diagnostics *variableSchemaDiagnostics) any {
	typed, ok := value.(map[string]any)
	if !ok {
		return cloneVariableSchemaValue(value)
	}
	normalized := make(map[string]any, len(typed))
	for _, key := range sortedVariableKeys(typed) {
		normalized[key] = normalizeVariableSchema(typed[key], path+"."+key, diagnostics)
	}
	return normalized
}

func normalizeVariableSchemaList(value any, path string, diagnostics *variableSchemaDiagnostics) any {
	typed, ok := value.([]any)
	if !ok {
		return cloneVariableSchemaValue(value)
	}
	normalized := make([]any, len(typed))
	for index, item := range typed {
		normalized[index] = normalizeVariableSchema(item, fmt.Sprintf("%s[%d]", path, index), diagnostics)
	}
	return normalized
}

func normalizeVariableSchemaOrList(value any, path string, diagnostics *variableSchemaDiagnostics) any {
	if _, ok := value.([]any); ok {
		return normalizeVariableSchemaList(value, path, diagnostics)
	}
	return normalizeVariableSchema(value, path, diagnostics)
}

func normalizeVariableSchemaDependencies(value any, path string, diagnostics *variableSchemaDiagnostics) any {
	typed, ok := value.(map[string]any)
	if !ok {
		return cloneVariableSchemaValue(value)
	}
	normalized := make(map[string]any, len(typed))
	for _, key := range sortedVariableKeys(typed) {
		dependency := typed[key]
		if _, isSchema := dependency.(map[string]any); isSchema {
			normalized[key] = normalizeVariableSchema(dependency, path+"."+key, diagnostics)
		} else {
			normalized[key] = cloneVariableSchemaValue(dependency)
		}
	}
	return normalized
}

func cloneVariableSchemaValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		cloned := make(map[string]any, len(typed))
		for key, item := range typed {
			cloned[key] = cloneVariableSchemaValue(item)
		}
		return cloned
	case []any:
		cloned := make([]any, len(typed))
		for index, item := range typed {
			cloned[index] = cloneVariableSchemaValue(item)
		}
		return cloned
	default:
		return value
	}
}

func normalizeVariableSchemaType(value any, path string, diagnostics *variableSchemaDiagnostics) any {
	switch typed := value.(type) {
	case string:
		if typed == "int" {
			diagnostics.messages = append(diagnostics.messages, path+`: legacy type "int" is treated as "integer"`)
			return "integer"
		}
		return typed
	case nil:
		diagnostics.messages = append(diagnostics.messages, path+`: YAML null is treated as JSON Schema type "null"`)
		return "null"
	case []any:
		normalized := make([]any, len(typed))
		for index, item := range typed {
			normalized[index] = normalizeVariableSchemaType(item, fmt.Sprintf("%s[%d]", path, index), diagnostics)
		}
		return normalized
	case []string:
		normalized := make([]any, len(typed))
		for index, item := range typed {
			normalized[index] = normalizeVariableSchemaType(item, fmt.Sprintf("%s[%d]", path, index), diagnostics)
		}
		return normalized
	default:
		return value
	}
}

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

func (v *Variables) Merge(other map[string]any) error {
	for key, value := range other {
		if _, ok := (*v)[key]; !ok {
			return fmt.Errorf("no such variable %q", key)
		}
		(*v)[key]["default"] = value
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
