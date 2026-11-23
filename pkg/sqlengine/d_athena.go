package sqlengine

// Athena is a router: statements are tokenized, parsed and generated either by the Hive dialect
// or by an Athena flavor of the Trino dialect. The sub-dialects are the default hive and trino
// prototypes.
//
// Parsing: a token stream that starts with HIVE_TOKEN_STREAM goes to the Hive parser, anything
// else to the Athena Trino parser. The Parser has no top-level parse hook, so the routing is done
// in place on the first statement: the Athena ParserSettings only contain routing entries that
// switch the parser's dialect/settings (p.d, p.s) to the target before the first token-dependent
// decision is taken; all subsequent statements are parsed with the target settings.

func init() { registerCustomizer("athena", customizeAthena) }

func customizeAthena(d *Dialect) {
	trino := prototypeLocked("trino")
	hive := prototypeLocked("hive")

	customizeAthenaParser(d, trino, hive)
	customizeAthenaGenerator(d, trino, hive)
}

func customizeAthenaParser(d *Dialect, trino, hive *Dialect) {
	// The Athena Trino parser: Trino's settings, with USING statements parsed as commands. A
	// CREATE TABLE ... AS may also start its query with a USING EXTERNAL FUNCTION clause, which is
	// parsed as a command too.
	atp := cloneParserSettings(trino.P)
	atp.STATEMENT_PARSERS[TK_USING] = func(p *Parser) *Expr { return p.parseAsCommand(p.prev) }
	atp.DDL_SELECT_TOKENS = atp.DDL_SELECT_TOKENS.With(TK_USING)
	atp.h.parseDdlSelect = func(p *Parser) *Expr {
		if p.match(TK_USING) {
			return p.parseAsCommand(p.prev)
		}
		return p.baseParseDdlSelect()
	}

	P := d.P

	useTrino := func(p *Parser) {
		if p.s == P {
			p.d, p.s = trino, atp
		}
	}

	// Statements starting with a token in the Trino STATEMENT_PARSERS: parseStatement has already
	// consumed the token (and handled its comments) exactly as the Trino parser would, so only the
	// dispatch is redirected.
	statementParsers := make(map[TokenType]parseFn, len(atp.STATEMENT_PARSERS)+1)
	for tt := range atp.STATEMENT_PARSERS {
		statementParsers[tt] = func(p *Parser) *Expr {
			useTrino(p)
			return p.s.STATEMENT_PARSERS[p.prev.Type](p)
		}
	}
	// A leading HIVE_TOKEN_STREAM marker (already consumed here) sends the rest of the stream to
	// the Hive parser.
	statementParsers[TK_HIVE_TOKEN_STREAM] = func(p *Parser) *Expr {
		if p.s == P {
			p.d, p.s = hive, hive.P
		}
		return p.parseStatement()
	}
	P.STATEMENT_PARSERS = statementParsers

	// Any other statement (expressions, queries) starts with parseExpression, before which no
	// token has been consumed. (Both tokenizers have the same command set, and parseCommand does
	// not depend on the parser tables.)
	P.h.parseExpression = func(p *Parser) *Expr {
		useTrino(p)
		return p.parseExpression()
	}

	// Parsing into a specific kind routes the same way, skipping the HIVE_TOKEN_STREAM marker.
	expressionParsers := make(map[Kind]parseFn, len(P.EXPRESSION_PARSERS))
	for k := range P.EXPRESSION_PARSERS {
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

func athenaIsIcebergTable(properties *Expr) bool {
	for _, p := range properties.Expressions() {
		if p.IsA(KProperty) && p.Name() == "table_type" {
			return pyLower(p.Text("value")) == "iceberg"
		}
	}

	return false
}

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

// athenaGenerateAsHive reports whether e is generated with the Hive dialect (most DDL) rather
// than Trino (queries, CREATE TABLE ... AS <query>, CREATE VIEW and DROP VIEW).
func athenaGenerateAsHive(e *Expr) bool {
	if e.IsA(KCreate) {
		if pyUpper(e.ArgS("kind")) == "TABLE" {
			properties := e.ArgE("properties")

			if properties != nil && properties.Find(KExternalProperty) != nil {
				return true
			}

			// Any CREATE TABLE other than CREATE TABLE ... As <query> is Hive, except a CTAS with
			// a leading USING EXTERNAL FUNCTION clause
			if expression := e.Expression(); !expression.IsA(KQuery) {
				return !(expression.IsA(KCommand) && pyUpper(expression.ArgS("this")) == "USING")
			}
		} else {
			return pyUpper(e.ArgS("kind")) != "VIEW"
		}
	} else if e.IsA(KAlter, KDrop, KDescribe, KShow) {
		if e.IsA(KDrop) && pyUpper(e.ArgS("kind")) == "VIEW" {
			// DROP VIEW is Trino, because CREATE VIEW is as well
			return false
		}

		return true
	}

	return false
}

func customizeAthenaGenerator(d *Dialect, trino, hive *Dialect) {
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

	trinoG := cloneGeneratorSettings(trino.G)
	trinoG.PROPERTIES_LOCATION[KLocationProperty] = Loc_POST_WITH
	// Unlike Trino's own generator, which drops the JSONPath parts it does not support, the Athena
	// Trino generator keeps all of Presto's JSONPath part transforms.
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

	G := d.G

	// Only the base after-HAVING modifiers apply: the Hive CLUSTER BY, DISTRIBUTE BY and SORT BY
	// entries are dropped.
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

	G.h.generate = func(g *Generator, e *Expr, copy bool) string {
		target := &trinoDialect
		if athenaGenerateAsHive(e) {
			target = &hiveDialect
		}
		return athenaSubGenerator(g, target).generate(e, copy)
	}
}

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
