package duck

// IngestrTypeHints maps DuckDB type names whose ingestr defaults lose width or add a time zone.
func IngestrTypeHints() map[string]string {
	return map[string]string{
		"int":     "int32",
		"integer": "int32",
		"int4":    "int32",
		"int32":   "int32",
		"signed":  "int32",

		"timestamp":                   "timestamp_ntz",
		"datetime":                    "timestamp_ntz",
		"timestamp without time zone": "timestamp_ntz",
	}
}

func (c *Client) IngestrTypeHints() map[string]string {
	return IngestrTypeHints()
}
