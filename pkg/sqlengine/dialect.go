package sqlengine

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

// Normalization strategies mirror sqlglot.dialects.dialect.NormalizationStrategy values.
const (
	NormLowercase            = "LOWERCASE"
	NormUppercase            = "UPPERCASE"
	NormCaseSensitive        = "CASE_SENSITIVE"
	NormCaseInsensitive      = "CASE_INSENSITIVE"
	NormCaseInsensitiveUpper = "CASE_INSENSITIVE_UPPERCASE"
)

// Dialect mirrors a sqlglot Dialect instance.
type Dialect struct {
	// Name is the registry key ("" for the base sqlglot dialect).
	Name string
	// ClassName is the sqlglot class name, e.g. "BigQuery".
	ClassName string
	// parents lists ancestor dialect names (closest first), excluding the base dialect.
	parents []string

	S *DialectSettings
	T *TokenizerSettings
	P *ParserSettings
	G *GeneratorSettings

	tok *tokenizerConfig

	NormalizationStrategy string
	Version               [3]int
	Settings              map[string]any

	hooks *dialectHooks

	timeTrie, formatTrie, inverseTimeTrie, inverseFormatTrie *trie
}

type dialectHooks struct {
	normalizeIdentifier   func(d *Dialect, e *Expr) *Expr
	canQuote              func(d *Dialect, id *Expr, identify string) bool
	toJSONPath            func(d *Dialect, path *Expr) *Expr
	generateValuesAliases func(d *Dialect, e *Expr) []*Expr
	tokenize              func(d *Dialect, sql string) ([]*Token, error)
}

// Is reports whether the dialect is (or inherits from) the given dialect name.
func (d *Dialect) Is(name string) bool {
	if d.Name == name {
		return true
	}
	for _, p := range d.parents {
		if p == name {
			return true
		}
	}
	return false
}

type dialectDef struct {
	name      string
	className string
	parents   []string
	settings  func() *DialectSettings
	tokenizer func() *TokenizerSettings
	setup     func(d *Dialect)
}

var (
	dialectDefs  = map[string]*dialectDef{}
	dialectCache = map[string]*Dialect{}
	dialectMu    sync.Mutex
)

// allSQLGlotDialectNames is used for "Did you mean" suggestions, mirroring DIALECT_MODULE_NAMES.
var allSQLGlotDialectNames = []string{
	"athena", "bigquery", "clickhouse", "databricks", "dax", "doris", "dremio", "drill", "druid",
	"duckdb", "dune", "exasol", "fabric", "hive", "materialize", "mysql", "oracle", "postgres",
	"presto", "prql", "redshift", "risingwave", "singlestore", "snowflake", "solr", "spark",
	"spark2", "sqlite", "starrocks", "tableau", "teradata", "trino", "tsql",
}

func registerDialect(def *dialectDef) { dialectDefs[def.name] = def }

// prototype returns the cached default instance for a registered dialect name.
func prototype(name string) *Dialect {
	dialectMu.Lock()
	defer dialectMu.Unlock()
	return prototypeLocked(name)
}

func prototypeLocked(name string) *Dialect {
	if d, ok := dialectCache[name]; ok {
		return d
	}
	def, ok := dialectDefs[name]
	if !ok {
		return nil
	}
	d := &Dialect{
		Name:      def.name,
		ClassName: def.className,
		parents:   def.parents,
		S:         def.settings(),
		T:         def.tokenizer(),
		hooks:     &dialectHooks{},
	}
	d.NormalizationStrategy = d.S.NORMALIZATION_STRATEGY
	d.Version = [3]int{math.MaxInt, 0, 0}
	d.tok = newTokenizerConfig(d.T, d.S)
	d.timeTrie = newTrieFromStrings(mapKeys(d.S.TIME_MAPPING)...)
	if len(d.S.FORMAT_MAPPING) > 0 {
		d.formatTrie = newTrieFromStrings(mapKeys(d.S.FORMAT_MAPPING)...)
	} else {
		d.formatTrie = d.timeTrie
	}
	d.inverseTimeTrie = newTrieFromStrings(mapKeys(d.S.INVERSE_TIME_MAPPING)...)
	d.inverseFormatTrie = newTrieFromStrings(mapKeys(d.S.INVERSE_FORMAT_MAPPING)...)
	dialectCache[name] = d
	if def.setup != nil {
		def.setup(d)
	}
	return d
}

func mapKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// GetDialect mirrors Dialect.get_or_raise for string inputs. Settings may follow the name,
// e.g. "mysql, normalization_strategy = case_sensitive".
func GetDialect(spec string) (*Dialect, error) {
	if spec == "" {
		return prototype(""), nil
	}
	parts := strings.Split(spec, ",")
	name := strings.TrimSpace(parts[0])
	kwargs := map[string]any{}
	for _, kv := range parts[1:] {
		pair := strings.Split(kv, "=")
		key := strings.TrimSpace(pair[0])
		switch len(pair) {
		case 1:
			kwargs[key] = true
		case 2:
			kwargs[key] = toBool(strings.TrimSpace(pair[1]))
		default:
			return nil, &ValueError{Msg: "Invalid dialect format: '" + spec + "'. Please use the correct format: 'dialect [, k1 = v2 [, ...]]'."}
		}
	}
	proto := prototype(name)
	if proto == nil {
		return nil, suggestClosestMatchAndFail("dialect", name, allSQLGlotDialectNames)
	}
	if len(kwargs) == 0 {
		return proto, nil
	}
	d := *proto
	if v, ok := kwargs["version"]; ok {
		delete(kwargs, "version")
		vs := strings.Split(toStr(v), ".")
		for len(vs) < 3 {
			vs = append(vs, "0")
		}
		for i := 0; i < 3; i++ {
			n, err := strconv.Atoi(vs[i])
			if err != nil {
				return nil, &ValueError{Msg: "invalid literal for int() with base 10: '" + vs[i] + "'"}
			}
			d.Version[i] = n
		}
	}
	if v, ok := kwargs["normalization_strategy"]; ok {
		delete(kwargs, "normalization_strategy")
		d.NormalizationStrategy = strings.ToUpper(toStr(v))
	}
	d.Settings = kwargs
	for k := range kwargs {
		return nil, suggestClosestMatchAndFail("setting", k, []string{"normalization_strategy", "version"})
	}
	return &d, nil
}

func toStr(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "True"
		}
		return "False"
	}
	return ""
}

// MustDialect returns the dialect or panics.
func MustDialect(name string) *Dialect {
	d, err := GetDialect(name)
	if err != nil {
		panic(err)
	}
	return d
}

// Tokenize mirrors Dialect.tokenize.
func (d *Dialect) Tokenize(sql string) ([]*Token, error) {
	if d.hooks.tokenize != nil {
		return d.hooks.tokenize(d, sql)
	}
	return newTokenizerCore(d.tok).tokenize(sql)
}

// NormalizeIdentifier mirrors Dialect.normalize_identifier.
func (d *Dialect) NormalizeIdentifier(e *Expr) *Expr {
	if d.hooks.normalizeIdentifier != nil {
		return d.hooks.normalizeIdentifier(d, e)
	}
	return d.baseNormalizeIdentifier(e)
}

func (d *Dialect) baseNormalizeIdentifier(e *Expr) *Expr {
	if e.IsA(KIdentifier) && d.NormalizationStrategy != NormCaseSensitive &&
		(!e.ArgB("quoted") || d.NormalizationStrategy == NormCaseInsensitive || d.NormalizationStrategy == NormCaseInsensitiveUpper) {
		var normalized string
		if d.NormalizationStrategy == NormUppercase || d.NormalizationStrategy == NormCaseInsensitiveUpper {
			normalized = pyUpper(e.ThisS())
		} else {
			normalized = pyLower(e.ThisS())
		}
		e.Set("this", normalized)
	}
	return e
}

// CaseSensitive mirrors Dialect.case_sensitive.
func (d *Dialect) CaseSensitive(text string) bool {
	if d.NormalizationStrategy == NormCaseInsensitive {
		return false
	}
	for _, r := range text {
		if d.NormalizationStrategy == NormUppercase {
			if unicode.IsLower(r) {
				return true
			}
		} else if unicode.IsUpper(r) {
			return true
		}
	}
	return false
}

// SAFE_IDENTIFIER_RE mirrors sqlglot.expressions.SAFE_IDENTIFIER_RE.
var SAFE_IDENTIFIER_RE = regexp.MustCompile(`^[_a-zA-Z][\p{L}\p{N}_]*\n?$`)

// CanQuote mirrors Dialect.can_quote. identify is "true", "false", "safe" or "unsafe".
func (d *Dialect) CanQuote(id *Expr, identify string) bool {
	if d.hooks.canQuote != nil {
		return d.hooks.canQuote(d, id, identify)
	}
	return d.baseCanQuote(id, identify)
}

func (d *Dialect) baseCanQuote(id *Expr, identify string) bool {
	if id.ArgB("quoted") {
		return true
	}
	if identify == "false" || identify == "" {
		return false
	}
	if id.Parent() != nil && id.Parent().IsA(KFunc) {
		return false
	}
	if identify == "true" {
		return true
	}
	isSafe := !d.CaseSensitive(id.ThisS()) && SAFE_IDENTIFIER_RE.MatchString(id.ThisS())
	if identify == "safe" {
		return isSafe
	}
	if identify == "unsafe" {
		return !isSafe
	}
	panic(&ValueError{Msg: "Unexpected argument for identify: '" + identify + "'"})
}

// QuoteIdentifier mirrors Dialect.quote_identifier.
func (d *Dialect) QuoteIdentifier(e *Expr, identify bool) *Expr {
	if e.IsA(KIdentifier) {
		mode := "unsafe"
		if identify {
			mode = "true"
		}
		e.Set("quoted", d.CanQuote(e, mode))
	}
	return e
}
