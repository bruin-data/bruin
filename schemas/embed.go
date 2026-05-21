package schemas

import (
	"embed"
	"fmt"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v3"
)

const SemanticModelV1ID = "v1"

//go:embed semantic-model/v1/schema.json
var files embed.FS

var registry = struct {
	once    sync.Once
	schemas map[string]*jsonschema.Schema
	err     error
}{}

func ValidateYAML(schemaID string, data []byte) error {
	schemas, err := compiledSchemas()
	if err != nil {
		return err
	}
	schema, ok := schemas[schemaID]
	if !ok {
		return fmt.Errorf("unknown schema %q", schemaID)
	}

	var value any
	if err := yaml.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("parsing YAML for schema validation: %w", err)
	}
	if err := schema.Validate(normalizeYAML(value)); err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}
	return nil
}

func compiledSchemas() (map[string]*jsonschema.Schema, error) {
	registry.once.Do(func() {
		compiler := jsonschema.NewCompiler()
		compiler.DefaultDraft(jsonschema.Draft2020)

		resources := map[string]string{
			SemanticModelV1ID: "semantic-model/v1/schema.json",
		}
		for id, path := range resources {
			data, err := files.ReadFile(path)
			if err != nil {
				registry.err = fmt.Errorf("reading embedded schema %s: %w", path, err)
				return
			}
			var schemaDoc any
			if err := yaml.Unmarshal(data, &schemaDoc); err != nil {
				registry.err = fmt.Errorf("parsing embedded schema %s: %w", path, err)
				return
			}
			if err := compiler.AddResource(id, normalizeYAML(schemaDoc)); err != nil {
				registry.err = fmt.Errorf("adding embedded schema %s: %w", path, err)
				return
			}
		}

		compiled := make(map[string]*jsonschema.Schema, len(resources))
		for id := range resources {
			schema, err := compiler.Compile(id)
			if err != nil {
				registry.err = fmt.Errorf("compiling schema %s: %w", id, err)
				return
			}
			compiled[id] = schema
		}
		registry.schemas = compiled
	})
	if registry.err != nil {
		return nil, registry.err
	}
	return registry.schemas, nil
}

func normalizeYAML(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, val := range v {
			out[key] = normalizeYAML(val)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(v))
		for key, val := range v {
			out[fmt.Sprint(key)] = normalizeYAML(val)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, val := range v {
			out[i] = normalizeYAML(val)
		}
		return out
	default:
		return value
	}
}
