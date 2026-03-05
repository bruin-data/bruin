package config

import (
	"reflect"
	"sort"
	"strings"
)

// ConnectionFieldDef describes a single field on a connection struct.
type ConnectionFieldDef struct {
	Name         string // mapstructure tag value (e.g. "username")
	Type         string // "string", "int", or "bool"
	DefaultValue string // from jsonschema "default=..." tag, empty if none
	IsRequired   bool   // true when the yaml tag does NOT contain "omitempty"
}

// ConnectionTypeDef groups a connection type name with its credential fields.
type ConnectionTypeDef struct {
	TypeName string               // yaml tag on the Connections struct (e.g. "snowflake")
	Fields   []ConnectionFieldDef // sorted alphabetically by Name
}

// GetConnectionTypeDefs reflects over the Connections struct and returns a
// sorted list of all connection types with their fields.
func GetConnectionTypeDefs() []ConnectionTypeDef {
	ct := reflect.TypeFor[Connections]()
	defs := make([]ConnectionTypeDef, 0, ct.NumField())
	for i := range ct.NumField() {
		sf := ct.Field(i)
		if !sf.IsExported() {
			continue
		}
		if sf.Type.Kind() != reflect.Slice {
			continue
		}

		yamlTag := sf.Tag.Get("yaml")
		if yamlTag == "" {
			continue
		}
		typeName := yamlTag
		if idx := strings.Index(typeName, ","); idx > 0 {
			typeName = typeName[:idx]
		}

		elemType := sf.Type.Elem()
		// Handle pointer element types
		if elemType.Kind() == reflect.Pointer {
			elemType = elemType.Elem()
		}
		if elemType.Kind() != reflect.Struct {
			continue
		}

		fields := extractFields(elemType)
		defs = append(defs, ConnectionTypeDef{
			TypeName: typeName,
			Fields:   fields,
		})
	}

	sort.Slice(defs, func(i, j int) bool {
		return defs[i].TypeName < defs[j].TypeName
	})
	return defs
}

// GetConnectionTypeNames returns a sorted list of all connection type names.
func GetConnectionTypeNames() []string {
	defs := GetConnectionTypeDefs()
	names := make([]string, len(defs))
	for i, d := range defs {
		names[i] = d.TypeName
	}
	return names
}

// GetConnectionFieldsForType returns the credential fields for a specific
// connection type, or nil if the type is not found.
func GetConnectionFieldsForType(typeName string) []ConnectionFieldDef {
	defs := GetConnectionTypeDefs()
	for _, d := range defs {
		if d.TypeName == typeName {
			return d.Fields
		}
	}
	return nil
}

// extractFields reads the exported fields of a connection struct, skipping the
// "name" field and non-primitive types (slices, maps, nested structs, complex
// pointer types).
func extractFields(t reflect.Type) []ConnectionFieldDef {
	fields := make([]ConnectionFieldDef, 0, t.NumField())

	for i := range t.NumField() {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}

		msTag := sf.Tag.Get("mapstructure")
		if msTag == "" {
			continue
		}
		// strip options after comma
		if idx := strings.Index(msTag, ","); idx > 0 {
			msTag = msTag[:idx]
		}
		if msTag == "name" {
			continue
		}

		fieldType := kindToTypeString(sf.Type.Kind())
		if fieldType == "" {
			continue
		}

		defaultVal := ""
		if jsTag := sf.Tag.Get("jsonschema"); jsTag != "" {
			for part := range strings.SplitSeq(jsTag, ",") {
				part = strings.TrimSpace(part)
				if v, ok := strings.CutPrefix(part, "default="); ok {
					defaultVal = v
				}
			}
		}
		// Also check for "default" struct tag
		if defaultVal == "" {
			if defTag := sf.Tag.Get("default"); defTag != "" {
				defaultVal = defTag
			}
		}

		yamlTag := sf.Tag.Get("yaml")
		isRequired := !strings.Contains(yamlTag, "omitempty")

		fields = append(fields, ConnectionFieldDef{
			Name:         msTag,
			Type:         fieldType,
			DefaultValue: defaultVal,
			IsRequired:   isRequired,
		})
	}

	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	})
	return fields
}

func kindToTypeString(k reflect.Kind) string {
	switch k { //nolint:exhaustive
	case reflect.String:
		return "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "int"
	case reflect.Bool:
		return "bool"
	default:
		return ""
	}
}
