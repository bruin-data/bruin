package sqlengine

// Port of sqlglot/dialects/athena.py, sqlglot/parsers/athena.py and sqlglot/generators/athena.py
// (sqlglot v30.13.0). The Athena tokenizer routing lives in athena_tokenizer.go.
//
// Athena is a router: statements are tokenized, parsed and generated either by the Hive machinery
// or by (an Athena flavour of) the Trino machinery. The sub-dialects are the default Hive() and
// Trino() instances, so their prototypes are used.
//
// Parsing: AthenaParser.parse / parse_into delegate to a Hive parser (when the token stream starts
// with HIVE_TOKEN_STREAM) or to an AthenaTrinoParser. The Go Parser has no parse hook, so the
// routing is done in place on the first statement: the Athena ParserSettings only contain routing
// entries that switch the parser's dialect/settings (p.d, p.s) to the target before the first
// token-dependent decision is taken; all subsequent statements are parsed with the target settings.

func init() { registerCustomizer("athena", customizeAthena) }

func customizeAthena(d *Dialect) {
	trino := prototypeLocked("trino")
	hive := prototypeLocked("hive")

	customizeAthenaParser(d, trino, hive)
	customizeAthenaGenerator(d, trino, hive)
}

// ---------------------------------------------------------------------------------------------
// AthenaTrinoParser / AthenaParser
// ---------------------------------------------------------------------------------------------

func customizeAthenaParser(d *Dialect, trino, hive *Dialect) {
	// class AthenaTrinoParser(TrinoParser)
	atp := cloneParserSettings(trino.P)
	atp.STATEMENT_PARSERS[TK_USING] = func(p *Parser) *Expr { return p.parseAsCommand(p.prev) }

	P := d.P

	useTrino := func(p *Parser) {
		if p.s == P {
			p.d, p.s = trino, atp
		}
	}

	// Statements starting with a token the Trino parser handles in STATEMENT_PARSERS: the token was
	// consumed by _parse_statement exactly like AthenaTrinoParser._parse_statement would have done
	// (including the comment handling), so only the dispatch is redirected.
	statementParsers := make(map[TokenType]parseFn, len(atp.STATEMENT_PARSERS)+1)
	for tt := range atp.STATEMENT_PARSERS {
		statementParsers[tt] = func(p *Parser) *Expr {
			useTrino(p)
			return p.s.STATEMENT_PARSERS[p.prev.Type](p)
		}
	}
	// AthenaParser.parse: if raw_tokens[0].token_type == HIVE_TOKEN_STREAM -> hive parser (raw_tokens[1:])
	statementParsers[TK_HIVE_TOKEN_STREAM] = func(p *Parser) *Expr {
		if p.s == P {
			p.d, p.s = hive, hive.P
		}
		return p.parseStatement()
	}
	P.STATEMENT_PARSERS = statementParsers

	// Any other statement (expressions, queries) starts with _parse_expression, before which no
	// token has been consumed. (COMMANDS are identical for both tokenizers, and _parse_command
	// does not depend on the parser tables.)
	P.h.parseExpression = func(p *Parser) *Expr {
		useTrino(p)
		return p.parseExpression()
	}

	// AthenaParser.parse_into
	expressionParsers := make(map[Kind]parseFn, len(P.EXPRESSION_PARSERS))
	for k := range P.EXPRESSION_PARSERS {
		k := k
		expressionParsers[k] = func(p *Parser) *Expr {
			if p.s == P {
				if p.curr.Type == TK_HIVE_TOKEN_STREAM {
					p.advance(1)
					p.d, p.s = hive, hive.P
				} else {
					p.d, p.s = trino, atp
				}
			}
			return p.s.EXPRESSION_PARSERS[k](p)
		}
	}
	P.EXPRESSION_PARSERS = expressionParsers
}

// ---------------------------------------------------------------------------------------------
// Module-level helpers of sqlglot/generators/athena.py
// ---------------------------------------------------------------------------------------------

// athenaIsIcebergTable mirrors generators.athena._is_iceberg_table.
func athenaIsIcebergTable(properties *Expr) bool {
	for _, p := range properties.Expressions() {
		if p.IsA(KProperty) && p.Name() == "table_type" {
			return pyLower(p.Text("value")) == "iceberg"
		}
	}

	return false
}

// athenaLocationPropertySQL mirrors generators.athena._location_property_sql.
func athenaLocationPropertySQL(g *Generator, e *Expr) string {
	// If table_type='iceberg', the LocationProperty is called 'location'
	// Otherwise, it's called 'external_location'
	// ref: https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html

	propName := "external_location"

	if e.Parent().IsA(KProperties) {
		if athenaIsIcebergTable(e.Parent()) {
			propName = "location"
		}
	}

	return propName + "=" + g.sqlKey(e, "this")
}

// athenaPartitionedByPropertySQL mirrors generators.athena._partitioned_by_property_sql.
func athenaPartitionedByPropertySQL(g *Generator, e *Expr) string {
	// If table_type='iceberg' then the table property for partitioning is called 'partitioning'
	// If table_type='hive' it's called 'partitioned_by'
	// ref: https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html#ctas-table-properties

	propName := "partitioned_by"

	if e.Parent().IsA(KProperties) {
		if athenaIsIcebergTable(e.Parent()) {
			propName = "partitioning"
		}
	}

	return propName + "=" + g.sqlKey(e, "this")
}

// athenaGenerateAsHive mirrors generators.athena._generate_as_hive.
func athenaGenerateAsHive(e *Expr) bool {
	if e.IsA(KCreate) {
		if pyUpper(e.ArgS("kind")) == "TABLE" {
			properties := e.ArgE("properties")

			// CREATE EXTERNAL TABLE is Hive
			if properties != nil && properties.Find(KExternalProperty) != nil {
				return true
			}

			// Any CREATE TABLE other than CREATE TABLE ... As <query> is Hive
			if !e.Expression().IsA(KQuery) {
				return true
			}
		} else {
			// CREATE VIEW is Trino, but CREATE SCHEMA, CREATE DATABASE, etc, is Hive
			return pyUpper(e.ArgS("kind")) != "VIEW"
		}
	} else if e.IsA(KAlter, KDrop, KDescribe, KShow) {
		if e.IsA(KDrop) && pyUpper(e.ArgS("kind")) == "VIEW" {
			// DROP VIEW is Trino, because CREATE VIEW is as well
			return false
		}

		// Everything else, e.g., ALTER statements, is Hive
		return true
	}

	return false
}

// ---------------------------------------------------------------------------------------------
// _HiveGenerator / AthenaTrinoGenerator / AthenaGenerator
// ---------------------------------------------------------------------------------------------

func customizeAthenaGenerator(d *Dialect, trino, hive *Dialect) {
	// class _HiveGenerator(HiveGenerator)
	hiveG := cloneGeneratorSettings(hive.G)
	hiveAlterSQL := hiveG.h.alterSQL
	hiveG.h.alterSQL = func(g *Generator, e *Expr) string {
		if e.IsA(KAlter) && pyUpper(e.ArgS("kind")) == "TABLE" {
			if actions := e.ArgL("actions"); len(actions) > 0 && actions[0].IsA(KColumnDef) {
				newActions := New(KSchema, "expressions", actions)
				e.Set("actions", []*Expr{newActions})
			}
		}

		return hiveAlterSQL(g, e)
	}
	hiveG.buildDispatch()
	hiveDialect := *hive
	hiveDialect.G = hiveG

	// class AthenaTrinoGenerator(TrinoGenerator)
	trinoG := cloneGeneratorSettings(trino.G)
	trinoG.PROPERTIES_LOCATION[KLocationProperty] = Loc_POST_WITH
	// AthenaTrinoGenerator.TRANSFORMS copies TrinoGenerator.TRANSFORMS before the Trino dialect class
	// prunes its unsupported JSONPath parts, so all of PrestoGenerator's JSONPath transforms remain.
	presto := prototypeLocked("presto")
	for _, part := range allJSONPathPartKinds {
		if f, ok := presto.G.TRANSFORMS[part]; ok {
			trinoG.TRANSFORMS[part] = f
		}
	}
	trinoG.TRANSFORMS[KPartitionedByProperty] = athenaPartitionedByPropertySQL
	trinoG.TRANSFORMS[KLocationProperty] = athenaLocationPropertySQL
	trinoG.buildDispatch()
	trinoDialect := *trino
	trinoDialect.G = trinoG

	// class AthenaGenerator(generator.Generator)
	G := d.G

	// AFTER_HAVING_MODIFIER_TRANSFORMS = generator.AFTER_HAVING_MODIFIER_TRANSFORMS (module level)
	for _, k := range []string{"cluster", "distribute", "sort"} {
		delete(G.AFTER_HAVING_MODIFIER_TRANSFORMS, k)
	}
	keys := make([]string, 0, len(G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS))
	for _, k := range G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS {
		if k != "cluster" && k != "distribute" && k != "sort" {
			keys = append(keys, k)
		}
	}
	G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS = keys

	// AthenaGenerator.__init__ builds the two sub-generators with the same options and
	// AthenaGenerator.generate delegates to one of them.
	G.h.generate = func(g *Generator, e *Expr, copy bool) string {
		target := &trinoDialect
		if athenaGenerateAsHive(e) {
			target = &hiveDialect
		}
		return athenaSubGenerator(g, target).generate(e, copy)
	}
}

// athenaSubGenerator mirrors the construction of AthenaGenerator's sub-generators: same generator
// options (_generator_kwargs), but the sub-dialect's own settings.
func athenaSubGenerator(g *Generator, target *Dialect) *Generator {
	normalizeFunctions := g.normalizeFunctions
	unsupportedLevel := g.unsupportedLevel
	sub := target.NewGenerator(&GenerateOptions{
		Pretty:             g.pretty,
		Identify:           g.identify,
		Normalize:          g.normalize,
		Pad:                g.pad,
		Indent:             g.indentSize,
		NormalizeFunctions: &normalizeFunctions,
		UnsupportedLevel:   &unsupportedLevel,
		MaxUnsupported:     g.maxUnsupported,
		LeadingComma:       g.leadingComma,
		MaxTextWidth:       g.maxTextWidth,
		NoComments:         !g.comments,
	})
	return sub
}
