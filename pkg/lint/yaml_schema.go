package lint

import (
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type yamlFieldSchema struct {
	children     map[string]*yamlFieldSchema
	allowUnknown bool
}

func unknownYAMLFieldPaths(content []byte, target reflect.Type) ([]string, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(content, &root); err != nil {
		return nil, errors.Wrap(err, "failed to parse YAML")
	}

	schema := buildYAMLFieldSchema(target, map[reflect.Type]*yamlFieldSchema{})
	if schema == nil {
		return nil, nil
	}

	unknown := make([]string, 0)
	collectUnknownYAMLPaths(&root, schema, "", &unknown)
	return unknown, nil
}

func buildYAMLFieldSchema(t reflect.Type, cache map[reflect.Type]*yamlFieldSchema) *yamlFieldSchema {
	if t == nil {
		return &yamlFieldSchema{children: map[string]*yamlFieldSchema{}}
	}

	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t == nil {
		return &yamlFieldSchema{children: map[string]*yamlFieldSchema{}}
	}

	if schema, ok := cache[t]; ok {
		return schema
	}

	switch t.Kind() {
	case reflect.Map, reflect.Interface:
		schema := &yamlFieldSchema{allowUnknown: true, children: map[string]*yamlFieldSchema{}}
		cache[t] = schema
		return schema
	case reflect.Slice, reflect.Array:
		schema := buildYAMLFieldSchema(t.Elem(), cache)
		cache[t] = schema
		return schema
	case reflect.Struct:
		schema := &yamlFieldSchema{children: map[string]*yamlFieldSchema{}}
		cache[t] = schema

		for i := range t.NumField() {
			field := t.Field(i)
			tag := field.Tag.Get("yaml")
			if tag == "-" {
				continue
			}

			name, inline := parseYAMLTag(tag, field.Name)
			child := buildYAMLFieldSchema(field.Type, cache)

			if inline {
				for k, v := range child.children {
					schema.children[k] = v
				}
				continue
			}
			if name == "" {
				continue
			}
			schema.children[name] = child
		}

		return schema
	default:
		schema := &yamlFieldSchema{children: map[string]*yamlFieldSchema{}}
		cache[t] = schema
		return schema
	}
}

func parseYAMLTag(tag string, fallback string) (name string, inline bool) {
	if tag == "" {
		return strings.ToLower(fallback), false
	}

	parts := strings.Split(tag, ",")
	fieldName := parts[0]
	for _, opt := range parts[1:] {
		if opt == "inline" {
			return fieldName, true
		}
	}
	return fieldName, false
}

func collectUnknownYAMLPaths(node *yaml.Node, schema *yamlFieldSchema, prefix string, out *[]string) {
	if node == nil || schema == nil {
		return
	}

	switch node.Kind {
	case yaml.DocumentNode:
		if len(node.Content) > 0 {
			collectUnknownYAMLPaths(node.Content[0], schema, prefix, out)
		}
	case yaml.MappingNode:
		for i := 0; i+1 < len(node.Content); i += 2 {
			keyNode := node.Content[i]
			valueNode := node.Content[i+1]
			field := keyNode.Value

			path := field
			if prefix != "" {
				path = prefix + "." + field
			}

			child, ok := schema.children[field]
			if !ok {
				*out = append(*out, path)
				continue
			}

			if child.allowUnknown {
				continue
			}

			collectUnknownYAMLPaths(valueNode, child, path, out)
		}
	case yaml.SequenceNode:
		for _, item := range node.Content {
			collectUnknownYAMLPaths(item, schema, prefix, out)
		}
	case yaml.ScalarNode, yaml.AliasNode:
		return
	}
}
