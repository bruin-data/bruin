package pipeline

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
)

type Variables map[string]map[string]any

func (v *Variables) Validate() error {
	if v == nil || *v == nil {
		return nil
	}

	for _, key := range sortedVariableKeys(*v) {
		value := (*v)[key]
		if _, ok := value["default"]; !ok {
			return fmt.Errorf("invalid variable %q: must have a default value", key)
		}
		if err := validateVariableValue(value["default"], value); err != nil {
			return fmt.Errorf("invalid variable %q: %w", key, err)
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

func (v *Variables) Schema() any {
	return map[string]any{
		"$schema":    "https://json-schema.org/draft-07/schema",
		"type":       "object",
		"properties": *v,
	}
}

func (v *Variables) Merge(other map[string]any) error {
	if len(other) == 0 {
		return nil
	}

	for _, key := range sortedVariableKeys(other) {
		schema, ok := (*v)[key]
		if !ok {
			return fmt.Errorf("no such variable %q", key)
		}
		if err := validateVariableValue(other[key], schema); err != nil {
			return fmt.Errorf("invalid variable %q: %w", key, err)
		}
	}

	for key, value := range other {
		// An empty YAML body such as `foo:` decodes to a nil map, which cannot be assigned to.
		if (*v)[key] == nil {
			(*v)[key] = make(map[string]any, 1)
		}
		(*v)[key]["default"] = value
	}
	return nil
}

// validateVariableValue checks a default or override against the constraints
// declared on the variable. Unknown `type` values are accepted so in-progress
// templates (for example `type: TODO`) keep parsing. Nested keywords such as
// `items`, `properties`, and `pattern` are stored but not compiled.
func validateVariableValue(value any, schema map[string]any) error {
	if err := validateOverrideType(value, schema); err != nil {
		return err
	}
	if err := validateEnum(value, schema); err != nil {
		return err
	}
	if err := validateConst(value, schema); err != nil {
		return err
	}
	return validateNumericBounds(value, schema)
}

func validateEnum(value any, schema map[string]any) error {
	allowed, ok := schemaSlice(schema, "enum")
	if !ok {
		return nil
	}
	for _, candidate := range allowed {
		if jsonValuesEqual(value, candidate) {
			return nil
		}
	}
	return fmt.Errorf("value %v is not one of the allowed values %v", value, allowed)
}

func validateConst(value any, schema map[string]any) error {
	expected, ok := schema["const"]
	if !ok {
		return nil
	}
	if jsonValuesEqual(value, expected) {
		return nil
	}
	return fmt.Errorf("value %v does not match const %v", value, expected)
}

func validateNumericBounds(value any, schema map[string]any) error {
	number, isNumber := asFloat64(value)
	if !isNumber {
		return nil
	}

	if minBound, ok := asFloat64(schema["minimum"]); ok && number < minBound {
		return fmt.Errorf("value %v is below minimum %v", value, schema["minimum"])
	}
	if maxBound, ok := asFloat64(schema["maximum"]); ok && number > maxBound {
		return fmt.Errorf("value %v is above maximum %v", value, schema["maximum"])
	}
	return nil
}

func schemaSlice(schema map[string]any, key string) ([]any, bool) {
	raw, ok := schema[key]
	if !ok || raw == nil {
		return nil, false
	}
	switch values := raw.(type) {
	case []any:
		return values, true
	default:
		rv := reflect.ValueOf(raw)
		if rv.Kind() != reflect.Slice {
			return nil, false
		}
		out := make([]any, rv.Len())
		for i := range rv.Len() {
			out[i] = rv.Index(i).Interface()
		}
		return out, true
	}
}

func jsonValuesEqual(a, b any) bool {
	af, aNum := asFloat64(a)
	bf, bNum := asFloat64(b)
	if aNum && bNum {
		return af == bf
	}
	return reflect.DeepEqual(a, b)
}

func asFloat64(value any) (float64, bool) {
	switch n := value.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
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
