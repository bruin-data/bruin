package sqlparser

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/sqlengine"
)

// attrErr mirrors the AttributeError Python raises when calling a Query builder on a non-Query.
func attrErr(e *sqlengine.Expr, attr string) error {
	name := "NoneType"
	if e != nil {
		name = e.Kind().Name()
	}
	return &pyErr{fmt.Sprintf("'%s' object has no attribute '%s'", name, attr)}
}

// ---------------------------------------------------------------------------
// rename.py
// ---------------------------------------------------------------------------

type tablePath struct {
	catalog, schema, table *string
}

func splitTablePath(name string) tablePath {
	parts := strings.Split(name, ".")
	p := tablePath{}
	switch len(parts) {
	case 3:
		p.catalog, p.schema, p.table = &parts[0], &parts[1], &parts[2]
	case 2:
		p.schema, p.table = &parts[0], &parts[1]
	default:
		n := name
		p.table = &n
	}
	return p
}

// replaceTableReferences mirrors rename.replace_table_references.
func replaceTableReferences(query, dialectName string, mappingAny any) (any, error) {
	d, err := getDialect(dialectName)
	if err != nil {
		return nil, err
	}
	parsed, err := d.Parse(query, nil)
	if err != nil {
		return nil, err
	}
	var keys []string
	var mapping map[string]string
	mappingIsNil := false
	switch m := mappingAny.(type) {
	case nil:
		mappingIsNil = true
	case map[string]string:
		if m == nil {
			mappingIsNil = true
		}
		mapping = m
	case map[string]any:
		mapping = map[string]string{}
		for k, v := range m {
			mapping[k] = str(v)
		}
	}
	for k := range mapping {
		keys = append(keys, k)
	}
	// Go's JSON encoder sorts map keys; Python iterates the received dict in that order.
	sortStrings(keys)

	for _, pq := range parsed {
		if pq == nil {
			return nil, &pyErr{"'NoneType' object has no attribute 'find_all'"}
		}
		for tableNode := range pq.FindAll(sqlengine.KTable) {
			if mappingIsNil {
				return nil, &pyErr{"'NoneType' object has no attribute 'items'"}
			}
			for _, tableName := range keys {
				newTableName := mapping[tableName]
				src := splitTablePath(tableName)
				if tableNode.Name() != *src.table {
					continue
				}
				if src.schema != nil && *src.schema != tableNode.DbName() {
					continue
				}
				if src.catalog != nil && *src.catalog != tableNode.CatalogName() {
					continue
				}
				dst := splitTablePath(newTableName)
				thisNode, ok := tableNode.Arg("this").(*sqlengine.Expr)
				if !ok || thisNode == nil {
					// Python: table_node.this.set(...) on None / a raw string.
					return nil, &pyErr{fmt.Sprintf("'%s' object has no attribute 'set'", pyTypeName(tableNode.Arg("this")))}
				}
				thisNode.Set("this", *dst.table)
				if dst.schema != nil {
					tableNode.Set("db", *dst.schema)
				} else {
					tableNode.Set("db", nil)
				}
				if dst.catalog != nil {
					tableNode.Set("catalog", *dst.catalog)
				} else if dst.schema == nil {
					tableNode.Set("catalog", nil)
				}
				if tableNode.Alias() == "" && *src.table != *dst.table {
					tableNode.Set("alias", *src.table)
				}
			}
		}

		for columnNode := range pq.FindAll(sqlengine.KColumn) {
			colTable := columnNode.Text("table")
			if colTable == "" {
				continue
			}
			colSchema := columnNode.Text("db")
			colCatalog := columnNode.Text("catalog")
			if colSchema == "" && colCatalog == "" {
				continue
			}
			if mappingIsNil {
				return nil, &pyErr{"'NoneType' object is not iterable"}
			}
			for _, tableName := range keys {
				src := splitTablePath(tableName)
				if colTable != *src.table {
					continue
				}
				if src.schema != nil && colSchema != "" && colSchema != *src.schema {
					continue
				}
				if src.catalog != nil && colCatalog != "" && colCatalog != *src.catalog {
					continue
				}
				columnNode.Set("db", nil)
				columnNode.Set("catalog", nil)
				break
			}
		}

		if dialectName == "tsql" {
			preserveTSQLInferredAliasCase(pq)
		}
	}

	outs := make([]string, 0, len(parsed))
	for _, pq := range parsed {
		s, err := d.GenerateOwned(pq, nil)
		if err != nil {
			return nil, err
		}
		outs = append(outs, s)
	}
	return map[string]any{"query": strings.Join(outs, "; "), "error": nil}, nil
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// preserveTSQLInferredAliasCase mirrors rename._preserve_tsql_inferred_alias_case.
func preserveTSQLInferredAliasCase(q *sqlengine.Expr) {
	for _, node := range q.FindAllList(sqlengine.KCTE, sqlengine.KSubquery) {
		sqlengine.QualifyDerivedTableOutputsTSQL(node)
	}
	for alias := range q.FindAll(sqlengine.KAlias) {
		aliasIdent := alias.ArgE("alias")
		if !aliasIdent.IsA(sqlengine.KIdentifier) {
			continue
		}
		source := alias.This()
		var sourceIdent *sqlengine.Expr
		if source.IsA(sqlengine.KColumn) && source.This().IsA(sqlengine.KIdentifier) {
			sourceIdent = source.This()
		} else if source.IsA(sqlengine.KIdentifier) {
			sourceIdent = source
		}
		if sourceIdent == nil {
			continue
		}
		if pyLower(aliasIdent.Name()) != pyLower(sourceIdent.Name()) {
			continue
		}
		if aliasIdent.Name() == sourceIdent.Name() {
			continue
		}
		aliasIdent.Set("this", sourceIdent.Name())
		if sourceIdent.HasArgKey("quoted") {
			aliasIdent.Set("quoted", sourceIdent.Arg("quoted"))
		}
	}
}

// ---------------------------------------------------------------------------
// add_limit / is_single_select
// ---------------------------------------------------------------------------

func addLimit(query string, limit int, dialectName string) (any, error) {
	dialectName = normalizeDialect(dialectName)
	d, err := getDialect(dialectName)
	if err != nil {
		return map[string]any{"error": "cannot parse query"}, nil
	}
	parsed, err := d.ParseOne(query, nil)
	if err != nil || parsed == nil {
		return map[string]any{"error": "cannot parse query"}, nil
	}
	if !parsed.IsA(sqlengine.KQuery) {
		return nil, attrErr(parsed, "limit")
	}
	limited, err := sqlengine.QueryLimitBuildOwned(parsed, limit, d)
	if err != nil {
		return nil, err
	}
	out, err := d.GenerateOwned(limited, nil)
	if err != nil {
		return nil, err
	}
	return map[string]any{"query": out}, nil
}

func isSingleSelectQuery(query, dialectName string) map[string]any {
	dialectName = normalizeDialect(dialectName)
	if pyStrip(query) == "" {
		return map[string]any{"is_single_select": false, "error": "cannot parse query"}
	}
	d, err := getDialect(dialectName)
	if err != nil {
		return map[string]any{"is_single_select": false, "error": err.Error()}
	}
	stmts, err := d.Parse(query, nil)
	if err != nil {
		return map[string]any{"is_single_select": false, "error": err.Error()}
	}
	if len(stmts) == 0 {
		return map[string]any{"is_single_select": false, "error": "cannot parse query"}
	}
	if len(stmts) == 1 {
		return map[string]any{"is_single_select": stmts[0].IsA(sqlengine.KSelect, sqlengine.KQuery), "error": ""}
	}
	return map[string]any{"is_single_select": false, "error": ""}
}

// ---------------------------------------------------------------------------
// extract_select / select_cte / freeze_time / add_ctes
// ---------------------------------------------------------------------------

func parseOneForUnitTest(query, dialectName string) (*sqlengine.Dialect, *sqlengine.Expr, map[string]any) {
	dialectName = normalizeDialect(dialectName)
	if pyStrip(query) == "" {
		return nil, nil, map[string]any{"error": "cannot parse query"}
	}
	d, err := getDialect(dialectName)
	if err != nil {
		return nil, nil, map[string]any{"error": err.Error()}
	}
	parsed, err := d.ParseOne(query, nil)
	if err != nil {
		return nil, nil, map[string]any{"error": err.Error()}
	}
	if parsed == nil {
		return nil, nil, map[string]any{"error": "cannot parse query"}
	}
	return d, parsed, nil
}

var writeKinds = []sqlengine.Kind{sqlengine.KInsert, sqlengine.KUpdate, sqlengine.KDelete, sqlengine.KMerge}

// preserveFabricDerivedColumnCase mirrors _preserve_fabric_derived_column_case.
func preserveFabricDerivedColumnCase(e *sqlengine.Expr) {
	for derived := range e.FindAll(sqlengine.KCTE, sqlengine.KSubquery) {
		alias := derived.ArgE("alias")
		if alias.IsA(sqlengine.KTableAlias) && len(alias.ArgL("columns")) > 0 {
			continue
		}
		for _, projection := range derived.This().Selects() {
			if projection.IsA(sqlengine.KColumn) && projection.Alias() == "" {
				quoted := projection.This().ArgB("quoted")
				projection.Replace(sqlengine.AliasWithQuote(projection.Copy(), projection.Name(), quoted))
			}
		}
	}
}

func extractSelect(query, dialectName string) map[string]any {
	d, parsed, errResp := parseOneForUnitTest(query, dialectName)
	if errResp != nil {
		return errResp
	}
	inner := parsed
	if parsed.IsA(sqlengine.KCreate, sqlengine.KInsert) {
		inner = parsed.Expression()
		if inner == nil {
			return map[string]any{"error": "asset has no SELECT to unit test"}
		}
	}
	if !inner.IsA(sqlengine.KQuery) {
		return map[string]any{"error": "asset is not a SELECT and has no inner SELECT to unit test"}
	}
	if inner.Arg("into") != nil {
		inner.Set("into", nil)
	}
	if inner.Find(writeKinds...) != nil {
		return map[string]any{"error": "asset contains a write statement and cannot be unit tested read-only"}
	}
	if normalizeDialect(dialectName) == "fabric" {
		preserveFabricDerivedColumnCase(inner)
	}
	out, err := d.GenerateOwned(inner, nil)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	return map[string]any{"query": out}
}

func selectCTE(query, dialectName, cteName string) map[string]any {
	d, parsed, errResp := parseOneForUnitTest(query, dialectName)
	if errResp != nil {
		return errResp
	}
	with := parsed.ArgE("with_")
	if with == nil {
		return map[string]any{"error": "the query defines no CTEs to assert"}
	}
	target := ""
	found := false
	var available []string
	for _, cte := range with.Expressions() {
		name := cte.AliasOrName()
		available = append(available, name)
		if pyLower(name) == pyLower(cteName) {
			target = name
			found = true
		}
	}
	if !found {
		quoted := make([]string, len(available))
		for i, a := range available {
			quoted[i] = sqlengine.PyRepr(a)
		}
		return map[string]any{"error": fmt.Sprintf("no CTE named %s in the query (available: [%s])", sqlengine.PyRepr(cteName), strings.Join(quoted, ", "))}
	}
	withSQL, err := d.Generate(with, nil)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	combined := withSQL + " SELECT * FROM " + target
	reparsed, err := d.ParseOne(combined, nil)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	out, err := d.GenerateOwned(reparsed, nil)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	return map[string]any{"query": out}
}

func freezeTime(query, dialectName, executionTime string) map[string]any {
	if executionTime == "" {
		return map[string]any{"error": "execution_time is required"}
	}
	d, parsed, errResp := parseOneForUnitTest(query, dialectName)
	if errResp != nil {
		return errResp
	}
	pieces := strings.Split(strings.ReplaceAll(executionTime, "T", " "), " ")
	datePart := pieces[0]
	timePart := "00:00:00"
	if len(pieces) > 1 {
		timePart = pieces[1]
	}
	frozen := parsed.Transform(func(n *sqlengine.Expr) *sqlengine.Expr {
		switch {
		case n.IsA(sqlengine.KCurrentTimestamp):
			return sqlengine.CastToType(sqlengine.LiteralString(executionTime), "TIMESTAMP")
		case n.IsA(sqlengine.KCurrentDate):
			return sqlengine.CastToType(sqlengine.LiteralString(datePart), "DATE")
		case n.IsA(sqlengine.KCurrentTime):
			return sqlengine.CastToType(sqlengine.LiteralString(timePart), "TIME")
		}
		return n
	}, true)
	out, err := d.GenerateOwned(frozen, nil)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	return map[string]any{"query": out}
}

func addCTEs(query, dialectName string, ctesAny any) map[string]any {
	d, parsed, errResp := parseOneForUnitTest(query, dialectName)
	if errResp != nil {
		return errResp
	}
	type cte struct{ name, query string }
	var ctes []cte
	switch cs := ctesAny.(type) {
	case []CTE:
		for _, c := range cs {
			ctes = append(ctes, cte{c.Name, c.Query})
		}
	case []any:
		for _, c := range cs {
			m, _ := c.(map[string]any)
			ctes = append(ctes, cte{str(m["name"]), str(m["query"])})
		}
	}
	result, err := func() (out *sqlengine.Expr, err error) {
		existing := parsed.ArgE("with_")
		if existing != nil {
			var newNodes []*sqlengine.Expr
			for _, c := range ctes {
				body, err := d.ParseOne(c.query, nil)
				if err != nil {
					return nil, err
				}
				newNodes = append(newNodes, sqlengine.NewCTE(body, c.name))
			}
			existing.Set("expressions", append(newNodes, existing.Expressions()...))
			return parsed, nil
		}
		cur := parsed
		for _, c := range ctes {
			// with_ is defined on Query, Insert and Update.
			if !cur.IsA(sqlengine.KQuery, sqlengine.KInsert, sqlengine.KUpdate) {
				return nil, attrErr(cur, "with_")
			}
			next, err := sqlengine.QueryWithBuild(cur, c.name, c.query, d)
			if err != nil {
				return nil, err
			}
			cur = next
		}
		return cur, nil
	}()
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	out, err := d.GenerateOwned(result, nil)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	return map[string]any{"query": out}
}

// ---------------------------------------------------------------------------
// read-only classification
// ---------------------------------------------------------------------------

var readOnlyRootKinds = []sqlengine.Kind{
	sqlengine.KSelect, sqlengine.KUnion, sqlengine.KIntersect, sqlengine.KExcept, sqlengine.KSubquery, sqlengine.KShow, sqlengine.KDescribe,
}

var readOnlyForbiddenKinds = []sqlengine.Kind{
	sqlengine.KDDL, sqlengine.KDML, sqlengine.KDrop, sqlengine.KAlter, sqlengine.KTruncateTable, sqlengine.KGrant, sqlengine.KRevoke,
	sqlengine.KExecute, sqlengine.KTransaction, sqlengine.KCommit, sqlengine.KRollback, sqlengine.KUse, sqlengine.KPragma,
	sqlengine.KInto, sqlengine.KLock, sqlengine.KCommand, sqlengine.KNextValueFor,
}

func isReadOnlyQuery(query, dialectName string) map[string]any {
	if strings.Contains(query, "\x00") {
		return map[string]any{"is_read_only": false, "error": ""}
	}
	dialectName = normalizeDialect(dialectName)
	d, err := getDialect(dialectName)
	if err != nil {
		return map[string]any{"is_read_only": false, "error": err.Error()}
	}
	if dialectName == "snowflake" {
		toks, err := d.Tokenize(query)
		if err != nil {
			return map[string]any{"is_read_only": false, "error": err.Error()}
		}
		for _, t := range toks {
			if t.Type == sqlengine.TK_DARROW {
				return map[string]any{"is_read_only": false, "error": ""}
			}
		}
	}
	parsed, err := d.Parse(query, nil)
	if err != nil {
		return map[string]any{"is_read_only": false, "error": err.Error()}
	}
	var stmts []*sqlengine.Expr
	for _, s := range parsed {
		if s != nil && !s.IsA(sqlengine.KSemicolon) {
			stmts = append(stmts, s)
		}
	}
	if len(stmts) == 0 {
		return map[string]any{"is_read_only": false, "error": "cannot parse empty query"}
	}
	for _, s := range stmts {
		ok, err := isReadOnlyStatement(s, d, dialectName)
		if err != nil {
			return map[string]any{"is_read_only": false, "error": err.Error()}
		}
		if !ok {
			return map[string]any{"is_read_only": false, "error": ""}
		}
	}
	return map[string]any{"is_read_only": true, "error": ""}
}

func isReadOnlyStatement(stmt *sqlengine.Expr, d *sqlengine.Dialect, dialectName string) (bool, error) {
	if stmt.IsA(sqlengine.KCommand) && dialectName == "snowflake" {
		if pyUpper(stmt.Name()) != "EXPLAIN" || !stmt.Expression().IsA(sqlengine.KLiteral) {
			return false, nil
		}
		explained, err := d.Parse(stmt.Expression().ThisS(), nil)
		if err != nil {
			return false, err
		}
		if len(explained) != 1 || !explained[0].IsA(sqlengine.KQuery) {
			return false, nil
		}
		return isReadOnlyStatement(explained[0], d, dialectName)
	}
	if !stmt.IsA(readOnlyRootKinds...) {
		return false, nil
	}
	for node := range stmt.Walk(true, nil) {
		if node.IsA(readOnlyForbiddenKinds...) {
			return false, nil
		}
		if dialectName != "snowflake" && node.IsA(sqlengine.KAnonymous) {
			return false, nil
		}
		if node.IsA(sqlengine.KDynamicIdentifier) && node.Arg("expressions") != nil {
			return false, nil
		}
		if node.IsA(sqlengine.KColumn) && pyUpper(node.Name()) == "NEXTVAL" {
			return false, nil
		}
		if dialectName != "snowflake" && node.IsA(sqlengine.KFunc) && node.Parent().IsA(sqlengine.KDot) {
			return false, nil
		}
	}
	return true, nil
}

func pyUpper(s string) string { return sqlengine.PyUpper(s) }

// pyTypeName mirrors type(v).__name__ for raw argument values.
func pyTypeName(v any) string {
	switch x := v.(type) {
	case nil:
		return "NoneType"
	case string:
		return "str"
	case *sqlengine.Expr:
		return x.Kind().Name()
	}
	return fmt.Sprintf("%T", v)
}
