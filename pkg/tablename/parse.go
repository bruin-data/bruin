package tablename

import (
	"fmt"
	"strings"
)

// TableName is a parsed, component-resolved table identifier. Absent components
// are the empty string (e.g. Catalog == "" for a two-component name with no
// catalog supplied or defaulted).
type TableName struct {
	Catalog string // database / catalog / project
	Schema  string // schema / dataset
	Table   string
}

// Defaults supplies the leading components to fall back on when the raw name
// omits them. They come from the platform's connection config (e.g. Snowflake's
// configured Database and Schema).
type Defaults struct {
	Catalog string
	Schema  string
}

// CheckName validates the component count and shape of raw without resolving it.
// It rejects empty components and counts outside the platform's [Min,Max] range.
// Unbounded platforms (e.g. Dremio) only get the empty-component check.
func (c Capability) CheckName(raw string) error {
	parts := strings.Split(raw, ".")
	for _, p := range parts {
		if strings.TrimSpace(p) == "" {
			return fmt.Errorf("table name %q contains an empty component", raw)
		}
	}

	if c.Unbounded {
		return nil
	}

	n := len(parts)
	if n < c.MinComponents || n > c.MaxComponents {
		return fmt.Errorf("table name must be in format %s, %q given", c.FormatDesc, raw)
	}
	return nil
}

// Parse splits raw into components and right-aligns them: the last component is
// always the table, the next is the schema, and (only when the platform allows
// three components) the first is the catalog. Missing leading components are
// filled from d. It returns an error when CheckName fails.
//
// Parse is not meaningful for Unbounded platforms (Dremio) and should not be
// used for them; their runtime code keeps its own path handling.
func (c Capability) Parse(raw string, d Defaults) (TableName, error) {
	if err := c.CheckName(raw); err != nil {
		return TableName{}, err
	}

	parts := strings.Split(raw, ".")
	n := len(parts)

	tn := TableName{Catalog: d.Catalog, Schema: d.Schema, Table: parts[n-1]}
	if n >= 2 {
		tn.Schema = parts[n-2]
	}
	if n >= 3 {
		tn.Catalog = parts[n-3]
	}
	return tn, nil
}

// SchemaToCreate returns the schema identifier that should be ensured to exist
// for an asset materialized as name, qualified by its catalog/database when the
// name is three-part (catalog.schema.table) so the schema is created in the
// named container rather than the connection's default. It returns ok=false when
// there is no schema to create (a single-component name) or the name has more
// components than a three-level identifier.
//
// transform is applied to each component (e.g. strings.ToUpper for Snowflake,
// strings.ToLower for DuckDB) so the result — used both as the CREATE SCHEMA
// target and the dedup cache key — matches how the platform stores identifiers.
func SchemaToCreate(name string, transform func(string) string) (string, bool) {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		parts[i] = transform(p)
	}
	switch len(parts) {
	case 2:
		return parts[0], true
	case 3:
		return parts[0] + "." + parts[1], true
	default:
		return "", false
	}
}

// Upper returns a copy with every present component upper-cased. Snowflake and
// MSSQL information_schema lookups compare against upper-cased identifiers.
func (t TableName) Upper() TableName {
	return TableName{
		Catalog: strings.ToUpper(t.Catalog),
		Schema:  strings.ToUpper(t.Schema),
		Table:   strings.ToUpper(t.Table),
	}
}

// QualifiedSchema returns the schema qualified by the catalog when present,
// e.g. "db" + "." + "schema" => "db.schema", or just "schema". Returns "" when
// there is no schema component.
func (t TableName) QualifiedSchema(sep string) string {
	if t.Schema == "" {
		return ""
	}
	if t.Catalog != "" {
		return t.Catalog + sep + t.Schema
	}
	return t.Schema
}

// String renders the full qualified name, joining the present components with
// sep (e.g. "db.schema.table").
func (t TableName) String(sep string) string {
	parts := make([]string, 0, 3)
	if t.Catalog != "" {
		parts = append(parts, t.Catalog)
	}
	if t.Schema != "" {
		parts = append(parts, t.Schema)
	}
	parts = append(parts, t.Table)
	return strings.Join(parts, sep)
}
