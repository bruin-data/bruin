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
	"number":           "bigint",
	"decimal":          "bigint",
	"numeric":          "bigint",
	"int":              "bigint",
	"integer":          "bigint",
	"bigint":           "bigint",
	"smallint":         "bigint",
	"tinyint":          "bigint",
	"byteint":          "bigint",
	"float":            "double",
	"float4":           "double",
	"float8":           "double",
	"double":           "double",
	"double precision": "double",
	"real":             "double",
	"binary":           "binary",
	"varbinary":        "binary",
	"boolean":          "bool",
	"date":             "date",
	"datetime":         "timestamp",
	"time":             "time",
	"timestamp":        "timestamp",
	"timestamp_ltz":    "timestamp",
	"timestamp_ntz":    "timestamp",
	"timestamp_tz":     "timestamp",
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
