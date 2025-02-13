package ingestr

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

// typHints mapping from different destination
// types to dlt types
// 'text' mappings are omitted, since they are the default
var typeHintMapping = map[string]string{
	// duckdb
	"bigint":      "bigint",
	"int8":        "bigint",
	"long":        "bigint",
	"blob":        "binary",
	"bytea":       "binary",
	"varbinary":   "binary",
	"binary":      "binary",
	"bool":        "bool",
	"boolean":     "bool",
	"logical":     "bool",
	"date":        "date",
	"double":      "double",
	"float":       "double",
	"float8":      "double",
	"float4":      "double",
	"real":        "double",
	"integer":     "bigint",
	"int4":        "bigint",
	"int":         "bigint",
	"signed":      "bigint",
	"json":        "json",
	"smallint":    "bigint",
	"int2":        "bigint",
	"short":       "bigint",
	"time":        "time",
	"timestamptz": "timestamp",
	"timestamp":   "timestamp",
	"datetime":    "timestamp",
	"tinyint":     "bigint",
	"int1":        "bigint",
	"ubigint":     "bigint",
	"uinteger":    "bigint",
	"usmallint":   "bigint",
	"utinyint":    "bigint",
}

// columnHints returns an ingestr compatiable type hint string
// that can be passed via the --column flag to the CLI
func columnHints(cols []pipeline.Column) string {
	var hints []string
	for _, col := range cols {
		typ := strings.ToLower(col.Type)
		hint, exists := typeHintMapping[typ]
		if !exists {
			continue
		}
		hints = append(hints, fmt.Sprintf("%s:%s", col.Name, hint))
	}
	return strings.Join(hints, ",")
}
