package python

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

// TypeHintMapping maps different destination types to dlt types.
// 'text' mappings are omitted, since they are the default.
var TypeHintMapping = map[string]string{
	// Integer types
	"int":         "bigint",
	"integer":     "bigint",
	"bigint":      "bigint",
	"smallint":    "bigint",
	"tinyint":     "bigint",
	"byteint":     "bigint",
	"mediumint":   "bigint", // MySQL
	"int2":        "bigint", // PostgreSQL alias
	"int4":        "bigint", // PostgreSQL alias
	"int8":        "bigint", // PostgreSQL alias
	"int16":       "bigint", // ClickHouse
	"int32":       "bigint", // ClickHouse
	"int64":       "bigint", // ClickHouse
	"int128":      "bigint", // ClickHouse
	"int256":      "bigint", // ClickHouse
	"uint8":       "bigint", // ClickHouse
	"uint16":      "bigint", // ClickHouse
	"uint32":      "bigint", // ClickHouse
	"uint64":      "bigint", // ClickHouse
	"uint128":     "bigint", // ClickHouse
	"uint256":     "bigint", // ClickHouse
	"serial":      "bigint", // PostgreSQL auto-increment
	"bigserial":   "bigint", // PostgreSQL auto-increment
	"smallserial": "bigint", // PostgreSQL auto-increment
	"serial2":     "bigint", // PostgreSQL alias
	"serial4":     "bigint", // PostgreSQL alias
	"serial8":     "bigint", // PostgreSQL alias
	"long":        "bigint", // Generic
	"short":       "bigint", // Generic

	// Floating point types
	"float":            "double",
	"float4":           "double",
	"float8":           "double",
	"float16":          "double", // Some systems
	"float32":          "double", // ClickHouse
	"float64":          "double", // ClickHouse/BigQuery
	"double":           "double",
	"double precision": "double",
	"real":             "double",
	// Decimal/Numeric types (with precision and scale)
	"decimal":    "decimal",
	"numeric":    "decimal",
	"number":     "decimal", // Oracle/Snowflake
	"dec":        "decimal", // Alias for decimal
	"money":      "decimal", // SQL Server/PostgreSQL
	"smallmoney": "decimal", // SQL Server

	// Binary types
	"binary":      "binary",
	"varbinary":   "binary",
	"blob":        "binary",
	"tinyblob":    "binary", // MySQL
	"mediumblob":  "binary", // MySQL
	"longblob":    "binary", // MySQL
	"bytea":       "binary", // PostgreSQL
	"bytes":       "binary", // BigQuery/Generic
	"image":       "binary", // SQL Server (deprecated)
	"raw":         "binary", // Oracle
	"long raw":    "binary", // Oracle
	"bit":         "binary", // SQL Server/MySQL bit field
	"bit varying": "binary", // PostgreSQL
	"varbit":      "binary", // PostgreSQL alias

	// Boolean types
	"boolean": "bool",
	"bool":    "bool",

	// Date types
	"date": "date",

	// Time types
	"time":                   "time",
	"time with time zone":    "time",
	"time without time zone": "time",
	"timetz":                 "time", // PostgreSQL

	// Timestamp/Datetime types
	"datetime":                    "timestamp",
	"timestamp":                   "timestamp",
	"timestamp_ltz":               "timestamp", // Snowflake
	"timestamp_ntz":               "timestamp", // Snowflake
	"timestamp_tz":                "timestamp", // Snowflake
	"timestamptz":                 "timestamp", // PostgreSQL
	"timestamp with time zone":    "timestamp", // PostgreSQL/Standard SQL
	"timestamp without time zone": "timestamp", // PostgreSQL/Standard SQL
	"datetime2":                   "timestamp", // SQL Server
	"datetimeoffset":              "timestamp", // SQL Server
	"smalldatetime":               "timestamp", // SQL Server
	"datetime64":                  "timestamp", // ClickHouse
	"interval":                    "timestamp", // PostgreSQL (time interval)

	// String/Text types
	"string":            "text",
	"char":              "text",
	"nchar":             "text",
	"varchar":           "text",
	"nvarchar":          "text",
	"text":              "text",
	"tinytext":          "text", // MySQL
	"mediumtext":        "text", // MySQL
	"longtext":          "text", // MySQL
	"clob":              "text", // Oracle/DB2
	"nclob":             "text", // Oracle
	"ntext":             "text", // SQL Server (deprecated)
	"character":         "text", // PostgreSQL full name
	"character varying": "text", // PostgreSQL full name
	"bpchar":            "text", // PostgreSQL blank-padded char
	"citext":            "text", // PostgreSQL case-insensitive
	"name":              "text", // PostgreSQL internal
	"longvarchar":       "text", // JDBC
	"nvarchar2":         "text", // Oracle
	"varchar2":          "text", // Oracle
	"fixedstring":       "text", // ClickHouse
	"lowcardinality":    "text", // ClickHouse

	// UUID/GUID types
	"uuid":             "text",
	"uniqueidentifier": "text", // SQL Server
	"guid":             "text",

	// JSON/Complex types
	"json":    "json",
	"jsonb":   "json", // PostgreSQL binary JSON
	"variant": "json", // Snowflake
	"object":  "json", // Snowflake/BigQuery
	"array":   "json", // Various databases
	"struct":  "json", // BigQuery/Spark
	"map":     "json", // Spark/ClickHouse
	"record":  "json", // BigQuery
	"super":   "json", // Redshift
	"hstore":  "json", // PostgreSQL key-value
	"tuple":   "json", // ClickHouse
	"nested":  "json", // ClickHouse

	// XML type
	"xml": "text",

	// Enum/Set types (treated as text)
	"enum": "text", // MySQL/PostgreSQL
	"set":  "text", // MySQL

	// Network types (PostgreSQL)
	"inet":     "text",
	"cidr":     "text",
	"macaddr":  "text",
	"macaddr8": "text",

	// Geometric types
	"geometry":  "json",
	"geography": "json",
	"point":     "text",
	"line":      "text",
	"lseg":      "text",
	"box":       "text",
	"path":      "text",
	"polygon":   "text",
	"circle":    "text",

	// Full-text search types (PostgreSQL)
	"tsvector": "text",
	"tsquery":  "text",

	// Range types (PostgreSQL)
	"int4range":      "text",
	"int8range":      "text",
	"numrange":       "text",
	"tsrange":        "text",
	"tstzrange":      "text",
	"daterange":      "text",
	"int4multirange": "text",
	"int8multirange": "text",
	"nummultirange":  "text",
	"tsmultirange":   "text",
	"tstzmultirange": "text",
	"datemultirange": "text",

	// Other PostgreSQL types
	"oid":      "bigint",
	"regclass": "text",
	"regproc":  "text",
	"regtype":  "text",
	"pg_lsn":   "text",

	// Databricks/Spark types
	"byte":             "bigint",
	"void":             "text",
	"calendarinterval": "text",

	// Year type (MySQL)
	"year": "bigint",
}

var multipleSpacePattern = regexp.MustCompile(`\s+`)

// ColumnHints returns an ingestr compatible type hint string
// that can be passed via the --column flag to the CLI.
func ColumnHints(cols []pipeline.Column, normaliseNames bool) string {
	hints := make([]string, 0)
	for _, col := range cols {
		typ := NormaliseColumnType(col.Type)

		hint, exists := TypeHintMapping[typ]
		if !exists {
			continue
		}
		name := col.Name
		if normaliseNames {
			name = NormalizeColumnName(name)
		}

		hints = append(hints, fmt.Sprintf("%s:%s", name, hint))
	}
	return strings.Join(hints, ",")
}

func NormaliseColumnType(typ string) string {
	typ = multipleSpacePattern.ReplaceAllString(typ, " ")
	typ = strings.ToLower(typ)
	typ = strings.TrimSpace(typ)
	return typ
}

func NormalizeColumnName(name string) string {
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
	var sb strings.Builder
	for i, r := range name {
		if unicode.IsUpper(r) {
			// Add underscore before uppercase letter (not at start)
			if i > 0 && (unicode.IsLower(rune(name[i-1])) || unicode.IsDigit(rune(name[i-1]))) {
				sb.WriteRune('_')
			}
			sb.WriteRune(unicode.ToLower(r))
		} else {
			sb.WriteRune(r)
		}
	}
	name = sb.String()

	// replace space with underscore
	name = strings.ReplaceAll(name, " ", "_")

	// add underscore if name starts with a number
	if len(name) > 0 && unicode.IsDigit(rune(name[0])) {
		name = "_" + name
	}

	return strings.ToLower(name)
}

// ColumnHintOptions controls how column hints are added to ingestr commands.
type ColumnHintOptions struct {
	// NormalizeColumnNames converts column names to snake_case (used for CSV/seed assets).
	NormalizeColumnNames bool
	// EnforceSchemaByDefault determines behavior when enforce_schema parameter is not set.
	// If true, schema will be enforced by default (used for seed assets).
	// If false, schema will only be enforced when enforce_schema="true" is explicitly set.
	EnforceSchemaByDefault bool
}
