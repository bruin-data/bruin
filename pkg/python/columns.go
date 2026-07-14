package python

import (
	"fmt"
	"maps"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

// IngestrTypeHintProvider is an optional capability on destination connections.
// Packages like clickhouse implement this so DB-native type aliases can overlay
// the shared defaults without pkg/python needing to know about each platform.
type IngestrTypeHintProvider interface {
	IngestrTypeHints() map[string]string
}

// IngestrTypeWrapperProvider is an optional capability for destinations that use
// transparent type wrappers (e.g. ClickHouse Nullable(T) / LowCardinality(T)).
type IngestrTypeWrapperProvider interface {
	IngestrTypeWrappers() map[string]bool
}

// TypeHintMapping maps portable / cross-platform column type aliases to ingestr
// (dlt) types. Destination-specific aliases live on the connection via
// IngestrTypeHintProvider. 'text' is the ingestr default when no hint is emitted.
var TypeHintMapping = map[string]string{
	// Integer types — mapped to the narrowest ingestr type that preserves the
	// declared width, so a column the source detected as Int16/Int32 is not
	// silently widened to Int64.
	// 8-bit (-> Int16 in ingestr; ingestr has no Int8 type)
	"tinyint": "tinyint",
	"byte":    "tinyint", // Spark/Databricks ByteType (-128..127)
	// 16-bit
	"smallint":    "smallint",
	"int2":        "smallint", // PostgreSQL alias
	"smallserial": "smallint", // PostgreSQL auto-increment
	"serial2":     "smallint", // PostgreSQL alias
	"short":       "smallint", // Generic
	// 32-bit
	"int":       "int",
	"integer":   "int",
	"int4":      "int", // PostgreSQL alias
	"mediumint": "int", // MySQL (24-bit, fits Int32)
	"serial":    "int", // PostgreSQL auto-increment
	"serial4":   "int", // PostgreSQL alias
	"year":      "int", // MySQL
	// 64-bit (and wider types clamped to Int64, the widest ingestr offers)
	"bigint":    "bigint",
	"int8":      "bigint", // PostgreSQL alias
	"bigserial": "bigint", // PostgreSQL auto-increment
	"serial8":   "bigint", // PostgreSQL alias
	"long":      "bigint", // Generic
	"byteint":   "bigint", // Snowflake (synonym for NUMBER(38,0))

	// Floating point types
	"float":            "double",
	"float4":           "double",
	"float8":           "double",
	"float16":          "double", // Some systems
	"float64":          "double", // BigQuery / generic
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

// ingestrSizedTypes are the ingestr types that accept a single length parameter, e.g.
// text(50). Add an entry if a source type starts mapping to another sized type.
var ingestrSizedTypes = map[string]bool{
	"text": true,
}

// TypeHintOverlayForConnection returns DB-specific type aliases when the
// connection implements IngestrTypeHintProvider; otherwise nil.
func TypeHintOverlayForConnection(conn any) map[string]string {
	if conn == nil {
		return nil
	}
	if p, ok := conn.(IngestrTypeHintProvider); ok {
		return p.IngestrTypeHints()
	}
	return nil
}

// TypeWrappersForConnection returns transparent type wrappers when the
// connection implements IngestrTypeWrapperProvider; otherwise nil.
func TypeWrappersForConnection(conn any) map[string]bool {
	if conn == nil {
		return nil
	}
	if p, ok := conn.(IngestrTypeWrapperProvider); ok {
		return p.IngestrTypeWrappers()
	}
	return nil
}

// MergeTypeHints returns a copy of base with overlay entries applied. Overlay
// keys are normalised the same way as column types. Overlay wins on conflict.
// The returned map is always a clone so callers cannot mutate TypeHintMapping.
func MergeTypeHints(base, overlay map[string]string) map[string]string {
	merged := maps.Clone(base)
	if merged == nil {
		merged = make(map[string]string, len(overlay))
	}
	for k, v := range overlay {
		merged[NormaliseColumnType(k)] = v
	}
	return merged
}

// resolveColumnTypeHint maps a declared column type to an ingestr hint, peeling
// destination-specific transparent wrappers and resolving parameterized bases
// (e.g. DateTime64(3)).
func resolveColumnTypeHint(typ string, mapping map[string]string, wrappers map[string]bool) (hint string, known bool, inlineLength string) {
	typ = NormaliseColumnType(typ)
	for {
		if h, ok := mapping[typ]; ok {
			return h, true, ""
		}
		base, inner, ok := splitParenType(typ)
		if !ok {
			return "", false, ""
		}
		base = NormaliseColumnType(base)
		if wrappers[base] {
			typ = NormaliseColumnType(inner)
			continue
		}
		if h, ok := mapping[base]; ok {
			if ingestrSizedTypes[h] {
				return h, true, inner
			}
			return h, true, ""
		}
		return "", false, ""
	}
}

// ColumnHints returns an ingestr compatible type hint string
// that can be passed via the --column flag to the CLI.
// overlay may be nil; when set, its aliases are merged over TypeHintMapping.
// wrappers may be nil; when set, those parameterized types peel to their inner type.
func ColumnHints(cols []pipeline.Column, normaliseNames bool, overlay map[string]string, wrappers map[string]bool) string {
	mapping := MergeTypeHints(TypeHintMapping, overlay)
	hints := make([]string, 0)
	for _, col := range cols {
		hint, typeKnown, inlineLength := resolveColumnTypeHint(col.Type, mapping, wrappers)

		if !typeKnown && col.SourceColumn == "" {
			continue
		}

		// Append a length to sized types; an inline length takes precedence over the
		// length field, and a non-numeric size is treated as unbounded.
		if typeKnown && ingestrSizedTypes[hint] {
			if length := sizedStringLength(inlineLength, col.Length); length != "" {
				hint = fmt.Sprintf("%s(%s)", hint, length)
			}
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

// splitParenType splits a type such as "varchar(100)" into base ("varchar") and inner
// ("100"), returning ok=false when there is no trailing parenthesised section.
func splitParenType(typ string) (base, inner string, ok bool) {
	open := strings.IndexByte(typ, '(')
	if open <= 0 || !strings.HasSuffix(typ, ")") {
		return "", "", false
	}
	base = strings.TrimSpace(typ[:open])
	inner = strings.TrimSpace(typ[open+1 : len(typ)-1])
	return base, inner, true
}

// sizedStringLength resolves the length for a sized string type, preferring an inline
// length over the length field; only positive integers yield a bounded column.
func sizedStringLength(inlineLength string, lengthField *int) string {
	if inlineLength != "" {
		if n, err := strconv.Atoi(inlineLength); err == nil && n > 0 {
			return strconv.Itoa(n)
		}
		return ""
	}
	if lengthField != nil && *lengthField > 0 {
		return strconv.Itoa(*lengthField)
	}
	return ""
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
	// TypeHintOverlay is an optional destination-specific alias map merged over
	// TypeHintMapping (e.g. from IngestrTypeHintProvider on the dest connection).
	TypeHintOverlay map[string]string
	// TypeWrappers is an optional set of transparent parameterized type names
	// (e.g. from IngestrTypeWrapperProvider) whose inner type should be resolved.
	TypeWrappers map[string]bool
}
