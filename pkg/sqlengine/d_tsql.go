package sqlengine

func init() { registerCustomizer("tsql", customizeTSQL) }

func customizeTSQL(d *Dialect) {
	customizeTSQLParser(d)
	customizeTSQLGenerator(d)
}
