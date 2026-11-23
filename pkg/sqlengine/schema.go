package sqlengine

import (
	"fmt"
	"strings"
	"sync"
)

// SchemaMap is a nested, insertion-ordered schema mapping: its values are either nested mappings
// (catalog/db/table levels) or column types (string, *Expr DataType, or nil).
type SchemaMap = omap[any]

// NewSchemaMap creates an empty schema mapping.
func NewSchemaMap() *SchemaMap { return newOMap[any]() }

// NewSchemaMapSize creates an empty schema mapping with room for n entries.
func NewSchemaMapSize(n int) *SchemaMap {
	m := newOMap[any]()
	m.reserve(n)
	return m
}

// MappingSchema resolves tables, columns, column types and UDF return types against a nested
// schema mapping, optionally normalizing names for its dialect.
type MappingSchema struct {
	mapping            *SchemaMap
	mappingTrie        *trie
	udfMapping         *SchemaMap
	udfTrie            *trie
	supportedTableArgs []string
	visible            *SchemaMap
	normalize          bool
	dialect            *Dialect

	typeMappingCache     map[string]*Expr
	normalizedTableCache map[tableCacheKey]*Expr
	normalizedNameCache  map[normNameKey]string
	findCache            map[findCacheKey]*findCacheEntry
	depthCache           int
}

// findCacheKey and tableCacheKey key the lookup caches by table hash plus the lookup options.
type findCacheKey struct {
	hash            uint64
	ensureDataTypes bool
}

type tableCacheKey struct {
	hash    uint64
	dialect *Dialect
	norm    bool
}

type findCacheEntry struct {
	schema *SchemaMap
}

// NewMappingSchema builds a schema from a table mapping, an optional visible-columns mapping and a
// UDF mapping. A nil dialect means the default dialect; normalize normalizes all names up front.
func NewMappingSchema(schema *SchemaMap, visible *SchemaMap, dialect *Dialect, normalize bool, udfMapping *SchemaMap) *MappingSchema {
	if dialect == nil {
		dialect = prototype("")
	}
	s := &MappingSchema{
		visible:              visible,
		normalize:            normalize,
		dialect:              dialect,
		typeMappingCache:     map[string]*Expr{},
		normalizedTableCache: map[tableCacheKey]*Expr{},
		normalizedNameCache:  map[normNameKey]string{},
		findCache:            map[findCacheKey]*findCacheEntry{},
	}
	if s.visible == nil {
		s.visible = NewSchemaMap()
	}
	if schema == nil {
		schema = NewSchemaMap()
	}
	if udfMapping == nil {
		udfMapping = NewSchemaMap()
	}
	if normalize {
		schema = s.normalizeSchema(schema)
		udfMapping = s.normalizeUDFs(udfMapping)
	}
	s.mapping = schema
	s.mappingTrie = newTrie()
	for _, keys := range flattenSchema(s.mapping, s.Depth(), nil) {
		s.mappingTrie.add(reversedStrings(keys))
	}
	s.udfMapping = udfMapping
	s.udfTrie = newTrie()
	for _, keys := range flattenSchema(s.udfMapping, dictDepth(s.udfMapping), nil) {
		s.udfTrie.add(reversedStrings(keys))
	}
	return s
}

func reversedStrings(in []string) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[len(in)-1-i] = v
	}
	return out
}

func (s *MappingSchema) Dialect() *Dialect { return s.dialect }

func (s *MappingSchema) Empty() bool { return s.mapping.Len() == 0 }

// Depth returns the number of table name levels (1: table, 2: db.table, 3: catalog.db.table),
// computed once.
func (s *MappingSchema) Depth() int {
	if !s.Empty() && s.depthCache == 0 {
		s.depthCache = dictDepth(s.mapping) - 1
	}
	return s.depthCache
}

// SupportedTableArgs returns the table parts the mapping is keyed by, innermost first (this, db,
// catalog), according to Depth. A depth above 3 panics.
func (s *MappingSchema) SupportedTableArgs() []string {
	if len(s.supportedTableArgs) == 0 && s.mapping.Len() > 0 {
		depth := s.Depth()
		if depth == 0 {
			s.supportedTableArgs = []string{}
		} else if depth >= 1 && depth <= 3 {
			s.supportedTableArgs = TABLE_PARTS[:depth]
		} else {
			panic(&SchemaError{Msg: fmt.Sprintf("Invalid mapping shape. Depth: %d", depth)})
		}
	}
	return s.supportedTableArgs
}

// tableParts returns the names of table's parts, innermost (the table name) first.
func (s *MappingSchema) tableParts(table *Expr) []string {
	parts := table.Parts()
	out := make([]string, len(parts))
	for i, p := range parts {
		out[len(parts)-1-i] = p.Name()
	}
	return out
}

func (s *MappingSchema) findInTrie(parts []string, tr *trie, raiseOnMissing bool) []string {
	value, sub := inTrie(tr, parts)
	if value == trieFailed {
		return nil
	}
	if value == triePrefix {
		possibilities := flattenTrie(sub)
		if len(possibilities) == 1 {
			parts = append(append([]string{}, parts...), possibilities[0]...)
		} else {
			if raiseOnMissing {
				var msgs []string
				for _, p := range possibilities {
					msgs = append(msgs, strings.Join(p, "."))
				}
				panic(&SchemaError{Msg: fmt.Sprintf("Ambiguous mapping for %s: %s.", strings.Join(parts, "."), strings.Join(msgs, ", "))})
			}
			return nil
		}
	}
	return parts
}

func flattenTrie(t *trie) [][]string {
	var out [][]string
	var walk func(n *trie, keys []string)
	walk = func(n *trie, keys []string) {
		for _, k := range n.order {
			c := n.children[k]
			if len(c.children) == 0 {
				out = append(out, append(append([]string{}, keys...), k))
			} else {
				walk(c, append(append([]string{}, keys...), k))
			}
		}
	}
	walk(t, nil)
	return out
}

// Find returns the column mapping of table. A partially qualified name matches when it is
// unambiguous; ambiguity panics when raiseOnMissing is set. With ensureDataTypes, string column
// types are converted to DataTypes. Results are cached.
func (s *MappingSchema) Find(table *Expr, raiseOnMissing bool, ensureDataTypes bool) *SchemaMap {
	key := findCacheKey{table.Hash(), ensureDataTypes}
	if e, ok := s.findCache[key]; ok && e.schema != nil {
		return e.schema
	}
	tp := s.tableParts(table)
	n := len(s.SupportedTableArgs())
	if n < len(tp) {
		tp = tp[:n]
	}
	var schema *SchemaMap
	resolved := s.findInTrie(tp, s.mappingTrie, raiseOnMissing)
	if resolved != nil {
		if v, ok := s.nestedGet(resolved, nil, raiseOnMissing).(*SchemaMap); ok {
			schema = v
		}
	}
	if ensureDataTypes && schema != nil {
		conv := NewSchemaMap()
		for _, col := range schema.Keys() {
			v, _ := schema.Get(col)
			if str, ok := v.(string); ok {
				conv.Set(col, s.toDataType(str, nil))
			} else {
				conv.Set(col, v)
			}
		}
		schema = conv
	}
	s.findCache[key] = &findCacheEntry{schema: schema}
	return schema
}

func (s *MappingSchema) nestedGet(parts []string, d *SchemaMap, raiseOnMissing bool) any {
	if d == nil || d.Len() == 0 {
		d = s.mapping
	}
	args := s.SupportedTableArgs()
	var path [][2]string
	for i := 0; i < len(args) && i < len(parts); i++ {
		path = append(path, [2]string{args[i], parts[len(parts)-1-i]})
	}
	return nestedGet(d, path, raiseOnMissing)
}

// AddTable registers table with its columns. With matchDepth, the table must have as many parts
// as the schema has levels. A table that is already known is only replaced when columns are given.
func (s *MappingSchema) AddTable(table *Expr, columnMapping *SchemaMap, dialect *Dialect, normalize *bool, matchDepth bool) {
	normalizedTable := s.normalizeTable(table, dialect, normalize)
	if matchDepth && !s.Empty() && len(normalizedTable.Parts()) != s.Depth() {
		sql, _ := normalizedTable.SQL(s.dialect.Name, nil)
		panic(&SchemaError{Msg: fmt.Sprintf("Table %s must match the schema's nesting level: %d.", sql, s.Depth())})
	}
	normalizedCols := NewSchemaMap()
	if columnMapping != nil {
		for _, k := range columnMapping.Keys() {
			v, _ := columnMapping.Get(k)
			normalizedCols.Set(s.normalizeName(k, dialect, false, normalize), v)
		}
	}
	if schema := s.Find(normalizedTable, false, false); schema != nil && normalizedCols.Len() == 0 {
		return
	}
	parts := s.tableParts(normalizedTable)
	nestedSet(s.mapping, reversedStrings(parts), normalizedCols)
	s.mappingTrie.add(parts)
	delete(s.findCache, findCacheKey{normalizedTable.Hash(), true})
	delete(s.findCache, findCacheKey{normalizedTable.Hash(), false})
}

// ColumnNames returns the column names of table, limited to the visible columns when onlyVisible
// is set and a visible mapping exists.
func (s *MappingSchema) ColumnNames(table *Expr, onlyVisible bool, dialect *Dialect, normalize *bool) []string {
	normalizedTable := s.normalizeTable(table, dialect, normalize)
	schema := s.Find(normalizedTable, true, false)
	if schema == nil {
		return []string{}
	}
	if !onlyVisible || s.visible.Len() == 0 {
		return append([]string{}, schema.Keys()...)
	}
	visible := map[string]bool{}
	switch v := s.nestedGet(s.tableParts(normalizedTable), s.visible, true).(type) {
	case []string:
		for _, x := range v {
			visible[x] = true
		}
	case *SchemaMap:
		for _, x := range v.Keys() {
			visible[x] = true
		}
	}
	var out []string
	for _, col := range schema.Keys() {
		if visible[col] {
			out = append(out, col)
		}
	}
	return out
}

// GetColumnType returns the type of a column of table (given as an expression or by name), or
// UNKNOWN.
func (s *MappingSchema) GetColumnType(table *Expr, column *Expr, columnName string, dialect *Dialect, normalize *bool) *Expr {
	normalizedTable := s.normalizeTable(table, dialect, normalize)
	var name string
	if column != nil {
		name = s.normalizeIdentName(column.This(), dialect, false, normalize)
	} else {
		name = s.normalizeName(columnName, dialect, false, normalize)
	}
	tableSchema := s.Find(normalizedTable, false, false)
	if tableSchema != nil && tableSchema.Len() > 0 {
		v, _ := tableSchema.Get(name)
		switch ct := v.(type) {
		case *Expr:
			if ct.IsA(KDataType) {
				return ct
			}
		case string:
			return s.toDataType(ct, dialect)
		}
	}
	return NewDataType(DT_UNKNOWN)
}

// HasColumn reports whether table has the column (given as an expression or by name).
func (s *MappingSchema) HasColumn(table *Expr, column *Expr, columnName string, dialect *Dialect, normalize *bool) bool {
	normalizedTable := s.normalizeTable(table, dialect, normalize)
	var name string
	if column != nil {
		name = s.normalizeIdentName(column.This(), dialect, false, normalize)
	} else {
		name = s.normalizeName(columnName, dialect, false, normalize)
	}
	tableSchema := s.Find(normalizedTable, false, false)
	if tableSchema == nil || tableSchema.Len() == 0 {
		return false
	}
	return tableSchema.Has(name)
}

// GetUDFType returns the return type of the UDF called by an Anonymous expression (qualified
// through its parent Dot), or UNKNOWN.
func (s *MappingSchema) GetUDFType(udf *Expr, dialect *Dialect, normalize *bool) *Expr {
	parts := s.normalizeUDF(udf, dialect, normalize)
	resolved := s.findInTrie(parts, s.udfTrie, false)
	if resolved == nil {
		return NewDataType(DT_UNKNOWN)
	}
	var path [][2]string
	for i := range resolved {
		path = append(path, [2]string{resolved[i], resolved[len(resolved)-1-i]})
	}
	switch t := nestedGet(s.udfMapping, path, false).(type) {
	case *Expr:
		if t.IsA(KDataType) {
			return t
		}
	case string:
		return s.toDataType(t, dialect)
	}
	return NewDataType(DT_UNKNOWN)
}

func (s *MappingSchema) udfParts(udf *Expr) []string {
	parent := udf.Parent()
	var parts []string
	if parent.IsA(KDot) {
		for _, p := range parent.Flatten(true) {
			parts = append(parts, p.Name())
		}
	} else {
		parts = []string{udf.Name()}
	}
	parts = reversedStrings(parts)
	d := dictDepth(s.udfMapping)
	if d < len(parts) {
		parts = parts[:d]
	}
	return parts
}

func (s *MappingSchema) normalizeUDF(udf *Expr, dialect *Dialect, normalize *bool) []string {
	if dialect == nil {
		dialect = s.dialect
	}
	norm := s.normalize
	if normalize != nil {
		norm = *normalize
	}
	parts := s.udfParts(udf)
	if norm {
		for i, p := range parts {
			parts[i] = s.normalizeName(p, dialect, true, nil)
		}
	}
	return parts
}

func (s *MappingSchema) normalizeSchema(schema *SchemaMap) *SchemaMap {
	normalized := NewSchemaMap()
	flattened := flattenSchema(schema, dictDepth(schema)-1, nil)
	if len(s.normalizedNameCache) == 0 {
		s.normalizedNameCache = make(map[normNameKey]string, countSchemaNames(schema))
	}
	var path [][2]string
	var normKeys []string
	for _, keys := range flattened {
		path = path[:0]
		for _, k := range keys {
			path = append(path, [2]string{k, k})
		}
		columns, ok := nestedGet(schema, path, true).(*SchemaMap)
		if !ok {
			panic(&SchemaError{Msg: fmt.Sprintf("Table %s must match the schema's nesting level: %d.", strings.Join(keys[:len(keys)-1], "."), len(flattened[0]))})
		}
		if columns.Len() == 0 {
			panic(&SchemaError{Msg: fmt.Sprintf("Table %s must have at least one column", strings.Join(keys[:len(keys)-1], "."))})
		}
		if firstVal, _ := columns.Get(columns.Keys()[0]); firstVal != nil {
			if _, ok := firstVal.(*SchemaMap); ok {
				inner := flattenSchema(columns, dictDepth(columns)-1, nil)
				var innerKeys []string
				if len(inner) > 0 {
					innerKeys = inner[0]
				}
				panic(&SchemaError{Msg: fmt.Sprintf("Table %s must match the schema's nesting level: %d.", strings.Join(append(append([]string{}, keys...), innerKeys...), "."), len(flattened[0]))})
			}
		}
		normKeys = normKeys[:0]
		for _, k := range keys {
			normKeys = append(normKeys, s.normalizeName(k, nil, true, nil))
		}
		// set normalized[normKeys...][column] = type for every column; the path is
		// walked (and created) once per table.
		table := nestedSubMap(normalized, normKeys)
		table.reserve(columns.Len())
		for i, col := range columns.keys {
			table.Set(s.normalizeName(col, nil, false, nil), columns.vals[i])
		}
	}
	return normalized
}

// countSchemaNames counts the keys of a nested schema mapping, for presizing name caches.
func countSchemaNames(m *SchemaMap) int {
	n := m.Len()
	if m != nil {
		for _, v := range m.vals {
			if sub, ok := v.(*SchemaMap); ok {
				n += countSchemaNames(sub)
			}
		}
	}
	return n
}

func (s *MappingSchema) normalizeUDFs(udfs *SchemaMap) *SchemaMap {
	normalized := NewSchemaMap()
	for _, keys := range flattenSchema(udfs, dictDepth(udfs), nil) {
		var path [][2]string
		for _, k := range keys {
			path = append(path, [2]string{k, k})
		}
		t := nestedGet(udfs, path, true)
		normKeys := make([]string, len(keys))
		for i, k := range keys {
			normKeys[i] = s.normalizeName(k, nil, true, nil)
		}
		nestedSet(normalized, normKeys, t)
	}
	return normalized
}

func (s *MappingSchema) normalizeTable(table *Expr, dialect *Dialect, normalize *bool) *Expr {
	if dialect == nil {
		dialect = s.dialect
	}
	norm := s.normalize
	if normalize != nil {
		norm = *normalize
	}
	key := tableCacheKey{table.Hash(), dialect, norm}
	if c, ok := s.normalizedTableCache[key]; ok && c != nil {
		return c
	}
	normalized := table
	if norm {
		normalized = table.Copy()
	}
	if norm {
		for _, part := range normalized.Parts() {
			if part.IsA(KIdentifier) {
				part.Replace(normalizeNameIdent(part, dialect, true, true))
			}
		}
	}
	s.normalizedTableCache[tableCacheKey{normalized.Hash(), dialect, norm}] = normalized
	return normalized
}

// TableFromString parses name as a table reference with dialect (nil: the schema's dialect);
// parse errors panic.
func (s *MappingSchema) TableFromString(name string, dialect *Dialect) *Expr {
	if dialect == nil {
		dialect = s.dialect
	}
	t, err := dialect.ParseOneInto(KTable, name, nil)
	if err != nil {
		panic(err)
	}
	return t
}

func (s *MappingSchema) normalizeName(name string, dialect *Dialect, isTable bool, normalize *bool) string {
	norm := s.normalize
	if normalize != nil {
		norm = *normalize
	}
	if dialect == nil {
		dialect = s.dialect
	}
	key := normNameKey{name, dialect, isTable, norm}
	if c, ok := s.normalizedNameCache[key]; ok && c != "" {
		return c
	}
	result := normalizedNameStr(key)
	s.normalizedNameCache[key] = result
	return result
}

type normNameKey struct {
	name    string
	dialect *Dialect
	isTable bool
	norm    bool
}

// globalNormNames memoizes normalized string names across schemas: for a string the result is a
// pure function of the key (parseIdentifier plus the dialect's NormalizeIdentifier on a fresh
// node), and MappingSchema construction would otherwise re-parse every table and column name.
var globalNormNames struct {
	sync.RWMutex
	m map[normNameKey]string
}

func normalizedNameStr(key normNameKey) string {
	globalNormNames.RLock()
	r, ok := globalNormNames.m[key]
	globalNormNames.RUnlock()
	if ok {
		return r
	}
	r = normalizeNameStr(key.name, key.dialect, key.isTable, key.norm).Name()
	globalNormNames.Lock()
	if globalNormNames.m == nil || len(globalNormNames.m) >= 1<<17 {
		globalNormNames.m = map[normNameKey]string{}
	}
	globalNormNames.m[key] = r
	globalNormNames.Unlock()
	return r
}

func (s *MappingSchema) normalizeIdentName(id *Expr, dialect *Dialect, isTable bool, normalize *bool) string {
	norm := s.normalize
	if normalize != nil {
		norm = *normalize
	}
	if dialect == nil {
		dialect = s.dialect
	}
	key := normNameKey{id.Name(), dialect, isTable, norm}
	if c, ok := s.normalizedNameCache[key]; ok && c != "" {
		return c
	}
	result := normalizeNameIdent(id, dialect, isTable, norm).Name()
	s.normalizedNameCache[key] = result
	return result
}

// toDataType parses a column type string with dialect (nil: the schema's dialect) and normalizes
// the identifiers in it; results are cached by type string.
func (s *MappingSchema) toDataType(schemaType string, dialect *Dialect) *Expr {
	if dt, ok := s.typeMappingCache[schemaType]; ok {
		return dt
	}
	if dialect == nil {
		dialect = s.dialect
	}
	udt := dialect.S.SUPPORTS_USER_DEFINED_TYPES
	expression := DataTypeFromStr(schemaType, dialect, udt)
	expression.Transform(func(n *Expr) *Expr { return dialect.NormalizeIdentifier(n) }, false)
	s.typeMappingCache[schemaType] = expression
	return expression
}

// DataTypeFromStr parses a type string with dialect. With udt, a string that does not parse
// becomes a user-defined type; other errors panic.
func DataTypeFromStr(dtype string, dialect *Dialect, udt bool) *Expr {
	if pyUpper(dtype) == "UNKNOWN" {
		return NewDataType(DT_UNKNOWN)
	}
	lvl := ErrorLevelIgnore
	e, err := dialect.ParseOneInto(KDataType, dtype, &ParseOptions{ErrorLevel: &lvl})
	if err != nil {
		if _, ok := err.(*ParseError); ok && udt {
			return New(KDataType, "this", DT_USERDEFINED, "kind", dtype)
		}
		panic(err)
	}
	return e
}

func normalizeNameStr(name string, dialect *Dialect, isTable bool, normalize bool) *Expr {
	return normalizeNameIdent(parseIdentifier(name, dialect), dialect, isTable, normalize)
}

// normalizeNameIdent returns a copy of id normalized for dialect when normalize is set, recording
// whether it names a table; id itself otherwise.
func normalizeNameIdent(id *Expr, dialect *Dialect, isTable bool, normalize bool) *Expr {
	if !normalize {
		return id
	}
	id = id.Copy()
	id.Meta()["is_table"] = isTable
	return dialect.NormalizeIdentifier(id)
}

// parseIdentifier parses name as an identifier with dialect, falling back to a plain Identifier
// when it does not tokenize or parse.
func parseIdentifier(name string, dialect *Dialect) *Expr {
	e, err := dialect.ParseOneInto(KIdentifier, name, nil)
	if err != nil {
		switch err.(type) {
		case *ParseError, *TokenError:
			return ToIdentifier(name, nil)
		}
		panic(err)
	}
	return e
}

// dictDepth returns the nesting depth of a schema mapping, following the first entry at each
// level: 0 for a non-mapping, 1 for an empty mapping.
func dictDepth(d any) int {
	m, ok := d.(*SchemaMap)
	if !ok || m == nil {
		return 0
	}
	if m.Len() == 0 {
		return 1
	}
	v, _ := m.Get(m.Keys()[0])
	return 1 + dictDepth(v)
}

// flattenSchema returns the key paths of schema, depth levels deep (or down to the first
// non-mapping value).
func flattenSchema(schema *SchemaMap, depth int, keys []string) [][]string {
	var tables [][]string
	for _, k := range schema.Keys() {
		v, _ := schema.Get(k)
		vm, isMap := v.(*SchemaMap)
		if depth == 1 || !isMap {
			tables = append(tables, append(append([]string{}, keys...), k))
		} else if depth >= 2 {
			tables = append(tables, flattenSchema(vm, depth-1, append(append([]string{}, keys...), k))...)
		}
	}
	return tables
}

// nestedGet follows path, a list of (part name, key) pairs, through d. A missing key returns nil,
// or panics with "Unknown <part>: <key>" when raiseOnMissing is set.
func nestedGet(d *SchemaMap, path [][2]string, raiseOnMissing bool) any {
	var result any = d
	for _, p := range path {
		name, key := p[0], p[1]
		m, ok := result.(*SchemaMap)
		var v any
		if ok {
			v, _ = m.Get(key)
		} else {
			panic(&ValueError{Msg: fmt.Sprintf("'%T' object has no attribute 'get'", result)})
		}
		result = v
		if result == nil {
			if raiseOnMissing {
				if name == "this" {
					name = "table"
				}
				panic(&ValueError{Msg: fmt.Sprintf("Unknown %s: %s", name, key)})
			}
			return nil
		}
	}
	return result
}

func nestedSubMap(d *SchemaMap, keys []string) *SchemaMap {
	sub := d
	for _, k := range keys {
		v, ok := sub.Get(k)
		if !ok {
			nm := NewSchemaMap()
			sub.Set(k, nm)
			sub = nm
		} else {
			sub = v.(*SchemaMap)
		}
	}
	return sub
}

func nestedSet(d *SchemaMap, keys []string, value any) *SchemaMap {
	if len(keys) == 0 {
		return d
	}
	if len(keys) == 1 {
		d.Set(keys[0], value)
		return d
	}
	sub := d
	for _, k := range keys[:len(keys)-1] {
		v, ok := sub.Get(k)
		if !ok {
			nm := NewSchemaMap()
			sub.Set(k, nm)
			sub = nm
		} else {
			sub = v.(*SchemaMap)
		}
	}
	sub.Set(keys[len(keys)-1], value)
	return d
}
