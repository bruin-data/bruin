package sqlengine

// dialectCustomizers holds per-dialect customization functions (ports of the dialect's
// Parser/Generator/Dialect class bodies). They run after the parent's settings were cloned
// and the generated data tables for the dialect were installed.
var dialectCustomizers = map[string]func(d *Dialect){}

func registerCustomizer(name string, fn func(d *Dialect)) { dialectCustomizers[name] = fn }

type dialectSpec struct {
	name, className, parent string
	parents                 []string
	settings                func() *DialectSettings
	tokenizer               func() *TokenizerSettings
	parserData              func() *ParserData
	generatorData           func() *GeneratorData
}

func init() {
	specs := []dialectSpec{
		{"", "Dialect", "", nil, dialectSettings_base, tokenizerSettings_base, parserSettings_base, generatorSettings_base},
		{"athena", "Athena", "", nil, dialectSettings_athena, tokenizerSettings_athena, parserSettings_athena, generatorSettings_athena},
		{"bigquery", "BigQuery", "", nil, dialectSettings_bigquery, tokenizerSettings_bigquery, parserSettings_bigquery, generatorSettings_bigquery},
		{"clickhouse", "ClickHouse", "", nil, dialectSettings_clickhouse, tokenizerSettings_clickhouse, parserSettings_clickhouse, generatorSettings_clickhouse},
		{"hive", "Hive", "", nil, dialectSettings_hive, tokenizerSettings_hive, parserSettings_hive, generatorSettings_hive},
		{"spark2", "Spark2", "hive", []string{"hive"}, dialectSettings_spark2, tokenizerSettings_spark2, parserSettings_spark2, generatorSettings_spark2},
		{"spark", "Spark", "spark2", []string{"spark2", "hive"}, dialectSettings_spark, tokenizerSettings_spark, parserSettings_spark, generatorSettings_spark},
		{"databricks", "Databricks", "spark", []string{"spark", "spark2", "hive"}, dialectSettings_databricks, tokenizerSettings_databricks, parserSettings_databricks, generatorSettings_databricks},
		{"mysql", "MySQL", "", nil, dialectSettings_mysql, tokenizerSettings_mysql, parserSettings_mysql, generatorSettings_mysql},
		{"doris", "Doris", "mysql", []string{"mysql"}, dialectSettings_doris, tokenizerSettings_doris, parserSettings_doris, generatorSettings_doris},
		{"starrocks", "StarRocks", "mysql", []string{"mysql"}, dialectSettings_starrocks, tokenizerSettings_starrocks, parserSettings_starrocks, generatorSettings_starrocks},
		{"duckdb", "DuckDB", "", nil, dialectSettings_duckdb, tokenizerSettings_duckdb, parserSettings_duckdb, generatorSettings_duckdb},
		{"tsql", "TSQL", "", nil, dialectSettings_tsql, tokenizerSettings_tsql, parserSettings_tsql, generatorSettings_tsql},
		{"fabric", "Fabric", "tsql", []string{"tsql"}, dialectSettings_fabric, tokenizerSettings_fabric, parserSettings_fabric, generatorSettings_fabric},
		{"oracle", "Oracle", "", nil, dialectSettings_oracle, tokenizerSettings_oracle, parserSettings_oracle, generatorSettings_oracle},
		{"postgres", "Postgres", "", nil, dialectSettings_postgres, tokenizerSettings_postgres, parserSettings_postgres, generatorSettings_postgres},
		{"redshift", "Redshift", "postgres", []string{"postgres"}, dialectSettings_redshift, tokenizerSettings_redshift, parserSettings_redshift, generatorSettings_redshift},
		{"presto", "Presto", "", nil, dialectSettings_presto, tokenizerSettings_presto, parserSettings_presto, generatorSettings_presto},
		{"trino", "Trino", "presto", []string{"presto"}, dialectSettings_trino, tokenizerSettings_trino, parserSettings_trino, generatorSettings_trino},
		{"sqlite", "SQLite", "", nil, dialectSettings_sqlite, tokenizerSettings_sqlite, parserSettings_sqlite, generatorSettings_sqlite},
		{"snowflake", "Snowflake", "", nil, dialectSettings_snowflake, tokenizerSettings_snowflake, parserSettings_snowflake, generatorSettings_snowflake},
	}
	for _, sp := range specs {
		registerDialect(&dialectDef{
			name:      sp.name,
			className: sp.className,
			parents:   sp.parents,
			settings:  sp.settings,
			tokenizer: sp.tokenizer,
			setup: func(d *Dialect) {
				setupDialectSettings(d, sp)
			},
		})
	}
}

// setupDialectSettings builds the parser/generator settings of a dialect by cloning its
// parent's (Python class inheritance), installing the generated data tables and applying
// the dialect's customizer. Called with dialectMu held.
func setupDialectSettings(d *Dialect, sp dialectSpec) {
	if sp.name == "" {
		d.P = newBaseParserSettings()
		d.G = newBaseGeneratorSettings()
	} else {
		parentName := sp.parent
		parent := prototypeLocked(parentName)
		d.P = cloneParserSettings(parent.P)
		d.G = cloneGeneratorSettings(parent.G)
		h := *parent.hooks
		d.hooks = &h
		d.hooks.tokenize = nil
	}
	d.P.ParserData = sp.parserData()
	d.G.GeneratorData = sp.generatorData()
	if fn := dialectCustomizers[sp.name]; fn != nil {
		fn(d)
	}
	if sp.name == "athena" {
		setupAthenaTokenizer(d)
	}
	d.P.SHOW_TRIE = newTrieFromWords(splitKeys(d.P.SHOW_PARSERS)...)
	d.P.SET_TRIE = newTrieFromWords(splitKeys(d.P.SET_PARSERS)...)
	pruneJSONPathTransforms(d.G)
	d.G.buildDispatch()
}

func splitKeys[V any](m map[string]V) [][]string {
	var out [][]string
	for k := range m {
		out = append(out, splitSpaces(k))
	}
	return out
}

func splitSpaces(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == ' ' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	return append(out, cur)
}
