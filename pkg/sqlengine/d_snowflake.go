package sqlengine

// Port of sqlglot/dialects/snowflake.py (sqlglot v30.13.0). The Snowflake JSONPathTokenizer lives
// in jsonpath.go; the parser and generator class bodies live in d_snowflake_parser.go and
// d_snowflake_generator.go. Data-only class attributes are in the generated zz_*_settings.go files.

func init() { registerCustomizer("snowflake", customizeSnowflake) }

func customizeSnowflake(d *Dialect) {
	// ---- Dialect class ----
	d.hooks.canQuote = snowflakeCanQuote

	// ---- SnowflakeParser ----
	customizeSnowflakeParser(d)

	// ---- SnowflakeGenerator ----
	customizeSnowflakeGenerator(d)
}

// can_quote.
func snowflakeCanQuote(d *Dialect, id *Expr, identify string) bool {
	// This disables quoting DUAL in SELECT ... FROM DUAL, because Snowflake treats an
	// unquoted DUAL keyword in a special way and does not map it to a user-defined table
	return d.baseCanQuote(id, identify) && !(id.Parent().IsA(KTable) &&
		!id.ArgB("quoted") &&
		pyLower(id.Name()) == "dual")
}
