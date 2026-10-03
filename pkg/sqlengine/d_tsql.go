package sqlengine

// Port of sqlglot/dialects/tsql.py. The TSQL Dialect class itself only defines data attributes
// (already generated in zz_*_settings.go); its Parser and Generator class bodies are ported in
// d_tsql_parser.go and d_tsql_generator.go.

func init() { registerCustomizer("tsql", customizeTSQL) }

func customizeTSQL(d *Dialect) {
	customizeTSQLParser(d)
	customizeTSQLGenerator(d)
}
