package sqlengine

func init() { registerCustomizer("snowflake", customizeSnowflake) }

func customizeSnowflake(d *Dialect) {
	d.hooks.canQuote = snowflakeCanQuote

	customizeSnowflakeParser(d)

	customizeSnowflakeGenerator(d)
}

func snowflakeCanQuote(d *Dialect, id *Expr, identify string) bool {
	// This disables quoting DUAL in SELECT ... FROM DUAL, because Snowflake treats an
	// unquoted DUAL keyword in a special way and does not map it to a user-defined table
	return d.baseCanQuote(id, identify) && !(id.Parent().IsA(KTable) &&
		!id.ArgB("quoted") &&
		pyLower(id.Name()) == "dual")
}
