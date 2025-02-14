package ingestr

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

// typeHints mapping from different destination types to dlt types.
// 'text' mappings are omitted, since they are the default.
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

// columnHints returns an ingestr compatible type hint string
// that can be passed via the --column flag to the CLI.
func columnHints(cols []pipeline.Column) string {
	var hints = make([]string, 0)
	for _, col := range cols {
		typ := normaliseColumnType(col.Type)
		hint, exists := typeHintMapping[typ]
		if !exists {
			continue
		}
		name := normalizeColumnName(col.Name)
		hints = append(hints, fmt.Sprintf("%s:%s", name, hint))
	}
	return strings.Join(hints, ",")
}

var (
	camelPattern         = regexp.MustCompile(`([\w])([A-Z][a-z]+)`)
	multipleSpacePattern = regexp.MustCompile(`\s+`)
)

func normaliseColumnType(typ string) string {
	typ = multipleSpacePattern.ReplaceAllString(typ, " ")
	typ = strings.ToLower(typ)
	typ = strings.TrimSpace(typ)
	return typ
}

func normalizeColumnName(name string) string {
	// https://dlthub.com/docs/general-usage/schema#naming-convention
	// nested column normalization is not implemented.

	// remove non ASCII characters
	name = strings.Map(func(c rune) rune {
		if c > unicode.MaxASCII {
			return rune(-1)
		}
		return c
	}, name)

	name = strings.TrimSpace(name)

	// merge multiple spaces into one
	name = multipleSpacePattern.ReplaceAllString(name, " ")

	// convert to snake case
	name = camelPattern.ReplaceAllString(name, "${1}_${2}")

	// replace space with underscore
	name = strings.ReplaceAll(name, " ", "_")

	// add underscore if name starts with a number
	if unicode.IsDigit(rune(name[0])) {
		name = "_" + name
	}

	return strings.ToLower(name)

}
