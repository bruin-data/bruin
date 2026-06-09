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
	// Integer types — mapped to the narrowest ingestr type that preserves the
	// declared width, so a column the source detected as Int16/Int32 is not
	// silently widened to Int64.
	// 8-bit (-> Int16 in ingestr; ingestr has no Int8 type)
	"tinyint": "tinyint",
	"byte":    "tinyint", // Spark/Databricks ByteType (-128..127)
	"uint8":   "tinyint", // ClickHouse (0..255)
	// 16-bit
	"smallint":    "smallint",
	"int2":        "smallint", // PostgreSQL alias
	"int16":       "smallint", // ClickHouse
	"smallserial": "smallint", // PostgreSQL auto-increment
	"serial2":     "smallint", // PostgreSQL alias
	"short":       "smallint", // Generic
	// 32-bit
	"int":       "int",
	"integer":   "int",
	"int4":      "int", // PostgreSQL alias
	"int32":     "int", // ClickHouse
	"mediumint": "int", // MySQL (24-bit, fits Int32)
	"serial":    "int", // PostgreSQL auto-increment
	"serial4":   "int", // PostgreSQL alias
	"uint16":    "int", // ClickHouse (0..65535, exceeds Int16)
	"year":      "int", // MySQL
	// 64-bit (and wider types clamped to Int64, the widest ingestr offers)
	"bigint":    "bigint",
	"int8":      "bigint", // PostgreSQL alias
	"int64":     "bigint", // ClickHouse
	"int128":    "bigint", // ClickHouse (no wider ingestr type)
	"int256":    "bigint", // ClickHouse (no wider ingestr type)
	"uint32":    "bigint", // ClickHouse (0..4.3B, exceeds Int32)
	"uint64":    "bigint", // ClickHouse (best-effort; may exceed Int64)
	"uint128":   "bigint", // ClickHouse
	"uint256":   "bigint", // ClickHouse
	"bigserial": "bigint", // PostgreSQL auto-increment
	"serial8":   "bigint", // PostgreSQL alias
	"long":      "bigint", // Generic
	"byteint":   "bigint", // Snowflake (synonym for NUMBER(38,0))

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
	"bigdecimal": "decimal", // High precision decimal for BigQuery (ingestr has no separate bigdecimal type)

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

	// Timestamp/Datetime types.
	// ingestr maps "timestamp" to a timezone-aware type and "timestamp_ntz" to a
	// naive (no time zone) type, so types that are unambiguously without a time
	// zone are emitted as "timestamp_ntz" to avoid silently promoting them.
	"datetime":                    "timestamp",
	"timestamp":                   "timestamp",
	"timestamp_ltz":               "timestamp",     // Snowflake (tz-aware local)
	"timestamp_ntz":               "timestamp_ntz", // Snowflake (no time zone)
	"timestamp_tz":                "timestamp",     // Snowflake
	"timestamptz":                 "timestamp",     // PostgreSQL
	"timestamp with time zone":    "timestamp",     // PostgreSQL/Standard SQL
	"timestamp without time zone": "timestamp_ntz", // PostgreSQL/Standard SQL (no time zone)
	"datetime2":                   "timestamp_ntz", // SQL Server (no time zone)
	"datetimeoffset":              "timestamp",     // SQL Server (has offset)
	"smalldatetime":               "timestamp_ntz", // SQL Server (no time zone)
	"datetime64":                  "timestamp",     // ClickHouse
	"interval":                    "interval",      // PostgreSQL (time interval)

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
	"void":             "text",
	"calendarinterval": "text",
}

var multipleSpacePattern = regexp.MustCompile(`\s+`)

// ColumnHints returns an ingestr compatible type hint string
// that can be passed via the --column flag to the CLI.
func ColumnHints(cols []pipeline.Column, normaliseNames bool) string {
	hints := make([]string, 0)
	for _, col := range cols {
		typ := NormaliseColumnType(col.Type)
		hint, typeKnown := TypeHintMapping[typ]

		if !typeKnown && col.SourceColumn == "" {
			continue
		}

		name := col.Name
		if normaliseNames {
			name = NormalizeColumnName(name)
		}

		switch {
		case col.SourceColumn != "" && typeKnown:
			hints = append(hints, fmt.Sprintf("%s:%s:%s", name, hint, col.SourceColumn))
		case col.SourceColumn != "":
			hints = append(hints, fmt.Sprintf("%s::%s", name, col.SourceColumn))
		default:
			hints = append(hints, fmt.Sprintf("%s:%s", name, hint))
		}
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
