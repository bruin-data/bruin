package sqlparser

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ajitpratap0/GoSQLX/pkg/sqlglot"
)

// cmdLineage mirrors get_column_lineage(query, schema, dialect) including the error a
// missing (null) schema produces in Python.
func cmdLineage(c map[string]any) (any, error) {
	query := str(c["query"])
	dialect := str(c["dialect"])
	rawSchema := c["schema"]
	schema, err := toRawSchema(rawSchema)
	if err != nil {
		return nil, err
	}
	return getColumnLineage(query, schema, dialect)
}

// rawSchema is the {"table.path": {"col": "type"}} mapping Bruin passes, preserving order.
type rawSchema struct {
	keys   []string
	tables map[string]*sqlglot.SchemaMap
	// nested holds values that are not flat column maps (nested catalog/db dicts).
	nested map[string]any
}

func toRawSchema(v any) (*rawSchema, error) {
	switch s := v.(type) {
	case nil:
		// Python: dict(None) -> TypeError
		return nil, &pyErr{"'NoneType' object is not iterable"}
	case Schema:
		if s == nil {
			return nil, &pyErr{"'NoneType' object is not iterable"}
		}
		rs := &rawSchema{tables: map[string]*sqlglot.SchemaMap{}, nested: map[string]any{}}
		keys := make([]string, 0, len(s))
		for k := range s {
			keys = append(keys, k)
		}
		// Go's JSON encoder sorts map keys; Python receives them in that order.
		sort.Strings(keys)
		for _, k := range keys {
			rs.keys = append(rs.keys, k)
			if s[k] == nil {
				// A nil column map is sent as JSON null (Python None).
				rs.tables[k] = nil
				continue
			}
			cols := sqlglot.NewSchemaMap()
			colKeys := make([]string, 0, len(s[k]))
			for ck := range s[k] {
				colKeys = append(colKeys, ck)
			}
			sort.Strings(colKeys)
			for _, ck := range colKeys {
				cols.Set(ck, s[k][ck])
			}
			rs.tables[k] = cols
		}
		return rs, nil
	case map[string]any:
		rs := &rawSchema{tables: map[string]*sqlglot.SchemaMap{}, nested: map[string]any{}}
		keys := make([]string, 0, len(s))
		for k := range s {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			rs.keys = append(rs.keys, k)
			rs.tables[k] = anyToSchemaMap(s[k])
		}
		return rs, nil
	case map[string]map[string]string:
		return toRawSchema(Schema(s))
	}
	return nil, &pyErr{fmt.Sprintf("unsupported schema type %T", v)}
}

func anyToSchemaMap(v any) *sqlglot.SchemaMap {
	if v == nil {
		return nil
	}
	m := sqlglot.NewSchemaMap()
	obj, ok := v.(map[string]any)
	if !ok {
		if ms, ok := v.(map[string]string); ok {
			keys := make([]string, 0, len(ms))
			for k := range ms {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				m.Set(k, ms[k])
			}
		}
		return m
	}
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		switch x := obj[k].(type) {
		case map[string]any:
			m.Set(k, anyToSchemaMap(x))
		default:
			m.Set(k, x)
		}
	}
	return m
}

// alignSchemaCasing mirrors align_schema_casing.
func alignSchemaCasing(schema *rawSchema, parsed *sqlglot.Expr) *rawSchema {
	queryTables := map[string]bool{}
	var ordered []string
	for table := range parsed.FindAll(sqlglot.KTable) {
		var parts []string
		for _, p := range table.Parts() {
			parts = append(parts, p.Name())
		}
		if len(parts) > 0 {
			n := strings.Join(parts, ".")
			if !queryTables[n] {
				queryTables[n] = true
				ordered = append(ordered, n)
			}
		}
	}
	lowerToQuery := map[string]string{}
	for _, qt := range ordered {
		lowerToQuery[pyLower(qt)] = qt
	}
	out := &rawSchema{tables: map[string]*sqlglot.SchemaMap{}, nested: map[string]any{}}
	for _, k := range schema.keys {
		out.keys = append(out.keys, k)
		out.tables[k] = schema.tables[k]
	}
	for _, k := range schema.keys {
		if qk, ok := lowerToQuery[pyLower(k)]; ok {
			if _, exists := out.tables[qk]; !exists {
				out.keys = append(out.keys, qk)
				out.tables[qk] = schema.tables[k]
			}
		}
	}
	return out
}

// schemaDictToSchemaObject mirrors schema_dict_to_schema_object.
func schemaDictToSchemaObject(schema *rawSchema) *sqlglot.SchemaMap {
	result := sqlglot.NewSchemaMap()
	for _, path := range schema.keys {
		current := result
		parts := strings.Split(path, ".")
		for _, part := range parts[:len(parts)-1] {
			v, ok := current.Get(part)
			if !ok {
				nm := sqlglot.NewSchemaMap()
				current.Set(part, nm)
				current = nm
				continue
			}
			nm, isMap := v.(*sqlglot.SchemaMap)
			if !isMap {
				// Python would fail on item assignment into a non-dict.
				panic(&pyErr{"'str' object does not support item assignment"})
			}
			current = nm
		}
		current.Set(parts[len(parts)-1], schemaValue(schema.tables[path]))
	}
	return result
}

// schemaValue converts a table's column map to a SchemaMap value; a nil map is Python None.
func schemaValue(m *sqlglot.SchemaMap) any {
	if m == nil {
		return nil
	}
	return m
}

// flatSchemaMap returns the raw {"a.b": {...}} mapping as a SchemaMap (keys are not split).
func flatSchemaMap(schema *rawSchema) *sqlglot.SchemaMap {
	m := sqlglot.NewSchemaMap()
	for _, k := range schema.keys {
		m.Set(k, schemaValue(schema.tables[k]))
	}
	return m
}

type lineageCol struct {
	Name     string           `json:"name"`
	Upstream []UpstreamColumn `json:"upstream"`
	Type     string           `json:"type,omitempty"`
}

func lineageError(msg string) map[string]any {
	errs := []string{}
	if msg != "" {
		errs = append(errs, msg)
	}
	return map[string]any{"columns": []any{}, "non_selected_columns": []any{}, "errors": errs}
}

// getColumnLineage mirrors get_column_lineage.
func getColumnLineage(query string, schema *rawSchema, dialectName string) (out any, err error) {
	dialectName = normalizeDialect(dialectName)

	d, derr := getDialect(dialectName)
	if derr != nil {
		return lineageError("Parse error: " + derr.Error()), nil
	}
	parsed, perr := d.ParseOne(query, nil)
	if perr != nil {
		return lineageError("Parse error: " + perr.Error()), nil
	}
	if !parsed.IsA(sqlglot.KQuery) {
		return lineageError("Failed to parse query"), nil
	}

	aligned := alignSchemaCasing(schema, parsed)
	nested := schemaDictToSchemaObject(aligned)

	optimized, oerr := sqlglot.OptimizeSafe(parsed, nested, d, []sqlglot.OptimizerRule{
		sqlglot.RuleQualify, sqlglot.RuleUnnestSubqueries, sqlglot.RuleMergeSubqueries, sqlglot.RuleAnnotateTypes,
	})
	if oerr != nil {
		base, _ := getDialect("")
		optimized, oerr = sqlglot.OptimizeSafe(parsed, nested, base, nil)
		if oerr != nil {
			return lineageError("Schema Error: " + oerr.Error()), nil
		}
	}

	cols, cerr := extractColumns(optimized)
	if cerr != nil {
		return map[string]any{"columns": []any{}, "non_selected_columns": []any{}, "errors": []string{}}, nil
	}

	scope := sqlglot.BuildScope(optimized)
	// lineage() builds its MappingSchema from the raw schema; failures make every lineage call fail.
	var lineageSchema *sqlglot.MappingSchema
	schemaOK := func() (ok bool) {
		defer func() {
			if recover() != nil {
				ok = false
			}
		}()
		lineageSchema = sqlglot.NewMappingSchema(flatSchemaMap(schema), nil, d, true, nil)
		return true
	}()

	byColumn := map[string]*sqlglot.LineageNode{}
	names := map[string]bool{}
	unique := true
	for _, c := range cols {
		if names[c.name] {
			unique = false
		}
		names[c.name] = true
	}
	if unique && schemaOK {
		_, nodes, lerr := sqlglot.LineageAll(optimized, sqlglot.LineageOptions{
			Schema: lineageSchema, Dialect: d, Scope: scope, TrimSelects: false, Copy: false,
		})
		if lerr == nil {
			byColumn = nodes
		}
	}

	result := []lineageCol{}
	for _, col := range cols {
		func() {
			defer func() { _ = recover() }()
			ll := byColumn[col.name]
			if ll == nil {
				if !schemaOK {
					return
				}
				n, lerr := sqlglot.LineageColumn(col.name, optimized, sqlglot.LineageOptions{
					Schema: lineageSchema, Dialect: d, Scope: scope, TrimSelects: false, Copy: false,
				})
				if lerr != nil {
					return
				}
				ll = n
			}
			var leaves []*sqlglot.LineageNode
			findLeafNodes(ll, &leaves)
			type pair struct{ column, table string }
			seen := map[pair]bool{}
			var cl []pair
			for _, ds := range leaves {
				func() {
					defer func() { _ = recover() }()
					this := ds.Expression.This()
					if this.IsA(sqlglot.KLiteral) || this.IsA(sqlglot.KAnonymous) {
						return
					}
					if ds.Expression.IsA(sqlglot.KTable) {
						nameParts := strings.Split(ds.Name, ".")
						colName := strings.Trim(nameParts[len(nameParts)-1], `"`)
						p := pair{colName, mergeParts(ds.Expression)}
						if !seen[p] {
							seen[p] = true
							cl = append(cl, p)
						}
					}
				}()
			}
			sort.SliceStable(cl, func(i, j int) bool { return cl[i].table < cl[j].table })
			ups := make([]UpstreamColumn, 0, len(cl))
			for _, p := range cl {
				ups = append(ups, UpstreamColumn{Column: p.column, Table: p.table})
			}
			result = append(result, lineageCol{Name: col.name, Upstream: ups, Type: col.typ})
		}()
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].Name < result[j].Name })

	nonSelected := []lineageCol{}
	func() {
		defer func() { _ = recover() }()
		byName := map[string]int{}
		for _, c := range extractNonSelectedColumns(optimized) {
			idx, ok := byName[c.name]
			if !ok {
				idx = len(nonSelected)
				byName[c.name] = idx
				nonSelected = append(nonSelected, lineageCol{Name: c.name, Upstream: []UpstreamColumn{}})
			}
			nonSelected[idx].Upstream = append(nonSelected[idx].Upstream, UpstreamColumn{Column: c.name, Table: c.table})
		}
	}()

	for i := range result {
		sortUpstreamByColumnLower(result[i].Upstream)
	}
	for i := range nonSelected {
		sortUpstreamByColumnLower(nonSelected[i].Upstream)
	}

	// Columns always carry a type key (possibly ""); non-selected ones never do.
	columnsOut := make([]map[string]any, 0, len(result))
	for _, c := range result {
		columnsOut = append(columnsOut, map[string]any{"name": c.Name, "upstream": c.Upstream, "type": c.Type})
	}
	nonSelOut := make([]map[string]any, 0, len(nonSelected))
	for _, c := range nonSelected {
		nonSelOut = append(nonSelOut, map[string]any{"name": c.Name, "upstream": c.Upstream})
	}
	return map[string]any{"columns": columnsOut, "non_selected_columns": nonSelOut, "errors": []string{}}, nil
}

func sortUpstreamByColumnLower(u []UpstreamColumn) {
	sort.SliceStable(u, func(i, j int) bool { return pyLower(u[i].Column) < pyLower(u[j].Column) })
}

func findLeafNodes(n *sqlglot.LineageNode, out *[]*sqlglot.LineageNode) {
	if len(n.Downstream) == 0 {
		*out = append(*out, n)
		return
	}
	for _, c := range n.Downstream {
		findLeafNodes(c, out)
	}
}

type selectedCol struct {
	name, typ string
}

// extractColumns mirrors extract_columns.
func extractColumns(parsed *sqlglot.Expr) (cols []selectedCol, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	found := parsed.Find(sqlglot.KSelect)
	if found == nil {
		return nil, nil
	}
	for _, e := range found.Expressions() {
		if e.IsA(sqlglot.KCTE) {
			continue
		}
		cols = append(cols, selectedCol{name: e.AliasOrName(), typ: typeString(e.Type())})
	}
	return cols, nil
}

// typeString mirrors str(expression.type).
func typeString(t *sqlglot.Expr) string {
	if t == nil {
		return "None"
	}
	return sqlglot.ExprString(t)
}

type tableCol struct{ name, table string }

// extractNonSelectedColumns mirrors extract_non_selected_columns.
func extractNonSelectedColumns(parsed *sqlglot.Expr) []tableCol {
	tables := extractTables(parsed)
	tableAlias := map[string]string{}
	for _, t := range tables {
		if a := t.Alias(); a != "" {
			tableAlias[a] = mergeParts(t)
		}
	}
	tableNames := map[string]bool{}
	for _, t := range tables {
		tableNames[mergeParts(t)] = true
	}
	seen := map[tableCol]bool{}
	var cols []tableCol
	for _, kind := range []sqlglot.Kind{sqlglot.KWhere, sqlglot.KJoin, sqlglot.KGroup} {
		for scope := range parsed.FindAll(kind) {
			for expr := range sqlglot.FindAllInScope(scope, sqlglot.KColumn) {
				tableName := expr.TableName()
				if a, ok := tableAlias[tableName]; ok {
					tableName = a
				}
				if tableNames[tableName] {
					c := tableCol{expr.Name(), tableName}
					if !seen[c] {
						seen[c] = true
						cols = append(cols, c)
					}
				}
			}
		}
	}
	sort.SliceStable(cols, func(i, j int) bool { return cols[i].name+cols[i].table < cols[j].name+cols[j].table })
	return cols
}

func pyLower(s string) string { return sqlglot.PyLower(s) }
