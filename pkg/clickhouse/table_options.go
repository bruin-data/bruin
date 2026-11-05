package clickhouse

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

// ValidateTableOptions checks the structure of native ClickHouse table options.
// ClickHouse validates SQL expressions, index types and engine compatibility.
func ValidateTableOptions(asset *pipeline.Asset) error {
	options := asset.ClickHouse
	for i, key := range options.PrimaryKey {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("clickhouse.primary_key[%d] must not be empty", i)
		}
	}
	if orderBy := sortingKey(asset); len(orderBy) > 0 {
		for i, key := range tablePrimaryKey(asset) {
			if i >= len(orderBy) || unquoteKey(key) != unquoteKey(orderBy[i]) {
				return fmt.Errorf("ClickHouse primary key columns must be a prefix of %s", sortingKeyField(asset))
			}
		}
	}

	indexNames := make(map[string]bool, len(options.Indexes))
	for i, index := range options.Indexes {
		for _, field := range []struct{ name, value string }{
			{"name", index.Name}, {"expression", index.Expression}, {"type", index.Type},
		} {
			if strings.TrimSpace(field.value) == "" {
				return fmt.Errorf("clickhouse.indexes[%d].%s is required", i, field.name)
			}
		}
		if index.Granularity < 0 {
			return fmt.Errorf("clickhouse.indexes[%d].granularity must be greater than zero", i)
		}
		name := unquoteKey(index.Name)
		if indexNames[name] {
			return fmt.Errorf("duplicate ClickHouse index name %q", name)
		}
		indexNames[name] = true
	}

	projectionNames := make(map[string]bool, len(options.Projections))
	for i, projection := range options.Projections {
		if strings.TrimSpace(projection.Name) == "" {
			return fmt.Errorf("clickhouse.projections[%d].name is required", i)
		}
		if strings.TrimSpace(projection.Select) == "" {
			return fmt.Errorf("clickhouse.projections[%d].select is required", i)
		}
		name := unquoteKey(projection.Name)
		if projectionNames[name] {
			return fmt.Errorf("duplicate ClickHouse projection name %q", name)
		}
		projectionNames[name] = true
	}
	return nil
}
