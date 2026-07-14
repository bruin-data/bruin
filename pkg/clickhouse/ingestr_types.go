package clickhouse

// IngestrTypeHints maps ClickHouse-native column type names (and ClickHouse-specific
// aliases) to ingestr/dlt types used for --columns hints on seed and ingest assets.
//
// Parameterized forms such as DateTime64(3), Decimal64(2), FixedString(16),
// Enum8('a' = 1), and Array(UInt64) resolve via their base name. Nullable(T) and
// LowCardinality(T) are peeled by pkg/python so the inner type is used.
//
// Entries that also exist in the shared defaults intentionally override them when
// this destination is ClickHouse (e.g. int8 is Int8 here, not PostgreSQL bigint;
// time is the Int64 alias, not a time-of-day type).
func IngestrTypeHints() map[string]string {
	return map[string]string{
		// Signed integers
		"int8":   "tinyint",
		"int16":  "smallint",
		"int32":  "int",
		"int64":  "bigint",
		"int128": "bigint",
		"int256": "bigint",

		// Unsigned integers
		"uint8":   "tinyint",
		"uint16":  "int",
		"uint32":  "bigint",
		"uint64":  "bigint",
		"uint128": "bigint",
		"uint256": "bigint",

		// Floating point
		"float32":  "double",
		"float64":  "double",
		"bfloat16": "double",

		// Fixed-point decimals (Decimal(P, S) / Decimal32(S) / …)
		"decimal":    "decimal",
		"decimal32":  "decimal",
		"decimal64":  "decimal",
		"decimal128": "decimal",
		"decimal256": "decimal",

		// Boolean
		"bool":    "bool",
		"boolean": "bool",

		// Strings
		"string":      "text",
		"fixedstring": "text",

		// UUID
		"uuid": "text",

		// Date / time
		"date":       "date",
		"date32":     "date",
		"datetime":   "timestamp",
		"datetime64": "timestamp",
		// ClickHouse TIME is an alias for Int64, not a time-of-day type.
		"time": "bigint",

		// Enums
		"enum":   "text",
		"enum8":  "text",
		"enum16": "text",

		// Nested / composite
		"array":   "json",
		"tuple":   "json",
		"map":     "json",
		"nested":  "json",
		"variant": "json",
		"dynamic": "json",
		"json":    "json",
		"object":  "json", // Object('json')

		// Aggregate-state types
		"aggregatefunction":       "json",
		"simpleaggregatefunction": "json",

		// Network
		"ipv4":  "text",
		"ipv6":  "text",
		"inet4": "text", // alias of IPv4
		"inet6": "text", // alias of IPv6

		// Geo
		"point":           "json",
		"ring":            "json",
		"polygon":         "json",
		"multipolygon":    "json",
		"linestring":      "json",
		"multilinestring": "json",

		// Intervals
		"intervalnanosecond":  "interval",
		"intervalmicrosecond": "interval",
		"intervalmillisecond": "interval",
		"intervalsecond":      "interval",
		"intervalminute":      "interval",
		"intervalhour":        "interval",
		"intervalday":         "interval",
		"intervalweek":        "interval",
		"intervalmonth":       "interval",
		"intervalquarter":     "interval",
		"intervalyear":        "interval",

		// Misc
		"nothing": "text",
	}
}

// IngestrTypeHints implements the optional connection capability used by
// pkg/python when building ingestr --columns hints for this destination.
func (c *Client) IngestrTypeHints() map[string]string {
	return IngestrTypeHints()
}
