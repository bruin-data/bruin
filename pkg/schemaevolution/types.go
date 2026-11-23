package schemaevolution

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/sqlengine"
)

const (
	defaultBigDecimalPrecision = 76
	defaultBigDecimalScale     = 38
)

// TypeFormatConfig configures dialect-aware SQL type parsing and canonical formatting
// powered by pkg/sqlengine.
type TypeFormatConfig struct {
	// SQLDialect is the pkg/sqlengine dialect name (e.g. "duckdb", "bigquery").
	SQLDialect string
	// Aliases maps additional uppercase SQL type names not covered by the dialect's
	// tokenizer (such as DuckDB "UINT1" or BigQuery "BYTEINT") to canonical sqlengine DTypes.
	Aliases map[string]sqlengine.DType
	// DTypeFolds maps dialect-equivalent DTypes into a single canonical DType (for example,
	// folding DT_DATETIME -> DT_TIMESTAMP in DuckDB, or DT_INT/DT_SMALLINT -> DT_BIGINT in BigQuery).
	DTypeFolds map[sqlengine.DType]sqlengine.DType
	// TypeNames overrides the canonical display/DDL name for specific DTypes in this
	// destination (e.g. DT_INT -> "INTEGER", DT_BIGINT -> "INT64", DT_DECIMAL -> "NUMERIC").
	TypeNames map[sqlengine.DType]string
	// GenericArraySyntax renders DT_ARRAY as `ARRAY<T>` instead of `T[]` / `T[N]`.
	GenericArraySyntax bool
	// KeepTypeLength preserves length parameters on string/binary types (e.g. `STRING(50)`, `BYTES(16)`).
	KeepTypeLength bool
	// OmitDefaultDecimalParams renders bare `NUMERIC` / `BIGNUMERIC` without `(P,S)` when
	// Precision and Scale equal the engine's unparameterized defaults.
	OmitDefaultDecimalParams bool
	// DefaultDecimalPrecision and DefaultDecimalScale supply the engine's bare DECIMAL defaults
	// when a type is written without explicit (precision, scale) parameters.
	DefaultDecimalPrecision int
	DefaultDecimalScale     int
	// PreprocessTypeSQL optionally normalizes destination-specific type strings (for example,
	// runtime typeof() placeholders) before sqlengine parses them while preserving Raw.
	PreprocessTypeSQL func(sqlType string) string
}

// StructField describes a named field inside a SQL STRUCT or UNION type.
type StructField struct {
	Name string
	Type ParsedType
}

// ParsedType is the normalized, structured representation of a SQL data type parsed via pkg/sqlengine.
type ParsedType struct {
	Raw       string
	DType     sqlengine.DType
	Precision int
	Scale     int
	Length    int
	Elem      *ParsedType   // element type for DT_ARRAY
	ArraySize int           // > 0 for fixed-size arrays; 0 for variable-length lists
	Fields    []StructField // fields for DT_STRUCT / DT_UNION
	MapKey    *ParsedType   // key type for DT_MAP
	MapValue  *ParsedType   // value type for DT_MAP
	Complex   bool
}

// IsNull reports whether pt represents an untyped SQL NULL / SQLNULL type.
func (pt ParsedType) IsNull() bool {
	return pt.DType == sqlengine.DT_NULL
}

// HasNestedNull reports whether pt is a composite type (ARRAY, STRUCT, UNION, or MAP) containing
// at least one untyped NULL leaf.
func (pt ParsedType) HasNestedNull() bool {
	switch pt.DType { //nolint:exhaustive
	case sqlengine.DT_ARRAY:
		return pt.Elem != nil && (pt.Elem.IsNull() || pt.Elem.HasNestedNull())
	case sqlengine.DT_MAP:
		return (pt.MapKey != nil && (pt.MapKey.IsNull() || pt.MapKey.HasNestedNull())) ||
			(pt.MapValue != nil && (pt.MapValue.IsNull() || pt.MapValue.HasNestedNull()))
	case sqlengine.DT_STRUCT, sqlengine.DT_UNION:
		for _, f := range pt.Fields {
			if f.Type.IsNull() || f.Type.HasNestedNull() {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// ArrayLeaf returns the leaf element type inside any nested DT_ARRAY wrappers (or pt itself when non-array).
func (pt ParsedType) ArrayLeaf() ParsedType {
	cur := pt
	for cur.DType == sqlengine.DT_ARRAY && cur.Elem != nil {
		cur = *cur.Elem
	}
	return cur
}

// ParseType parses sqlType using cfg's sqlengine dialect into a normalized ParsedType.
func ParseType(cfg TypeFormatConfig, sqlType string) ParsedType {
	trimmed := strings.TrimSpace(sqlType)
	if trimmed == "" {
		return ParsedType{}
	}

	if elemSQL, arrSize, isBracketArr := peelTrailingBracketArray(cfg.SQLDialect, trimmed); isBracketArr {
		elem := ParseType(cfg, elemSQL)
		return ParsedType{
			Raw:       trimmed,
			DType:     sqlengine.DT_ARRAY,
			Elem:      &elem,
			ArraySize: arrSize,
		}
	}

	upper := strings.Join(strings.Fields(strings.ToUpper(trimmed)), " ")
	hasQuotes := strings.ContainsAny(trimmed, `'"`)

	if cfg.Aliases != nil {
		if aliased, ok := cfg.Aliases[upper]; ok {
			pt := ParsedType{
				Raw:   trimmed,
				DType: canonicalizeDType(cfg, aliased),
			}
			applyDefaultDecimalParams(cfg, &pt)
			return pt
		}
	}

	parseInput := trimmed
	if cfg.PreprocessTypeSQL != nil {
		parseInput = cfg.PreprocessTypeSQL(trimmed)
	}

	expr, ok := safeParseDataTypeExpr(cfg.SQLDialect, parseInput)
	if !ok || expr == nil {
		return ParsedType{
			Raw:     trimmed,
			DType:   sqlengine.DT_USERDEFINED,
			Complex: hasQuotes,
		}
	}

	pt := buildParsedTypeFromExpr(cfg, expr, trimmed)
	if hasQuotes {
		pt.Complex = true
	}
	return pt
}

func applyDefaultDecimalParams(cfg TypeFormatConfig, pt *ParsedType) {
	if pt.Precision > 0 {
		return
	}
	switch pt.DType { //nolint:exhaustive
	case sqlengine.DT_DECIMAL:
		if cfg.DefaultDecimalPrecision > 0 {
			pt.Precision = cfg.DefaultDecimalPrecision
			pt.Scale = cfg.DefaultDecimalScale
		}
	case sqlengine.DT_BIGDECIMAL:
		pt.Precision = defaultBigDecimalPrecision
		pt.Scale = defaultBigDecimalScale
	}
}

func peelTrailingBracketArray(dialectName, sqlType string) (string, int, bool) {
	if !strings.HasSuffix(sqlType, "]") {
		return "", 0, false
	}
	tokens, err := dialectOrDefault(dialectName).Tokenize(sqlType)
	if err != nil || len(tokens) < 3 {
		return "", 0, false
	}
	last := tokens[len(tokens)-1]
	if last.Type != sqlengine.TK_R_BRACKET {
		return "", 0, false
	}

	runes := []rune(sqlType)
	if tokens[len(tokens)-2].Type == sqlengine.TK_L_BRACKET {
		start := int(tokens[len(tokens)-2].Start)
		if start <= 0 || start > len(runes) {
			return "", 0, false
		}
		return strings.TrimSpace(string(runes[:start])), 0, true
	}

	if len(tokens) >= 4 &&
		tokens[len(tokens)-3].Type == sqlengine.TK_L_BRACKET &&
		tokens[len(tokens)-2].Type == sqlengine.TK_NUMBER {
		n, convErr := strconv.Atoi(strings.TrimSpace(tokens[len(tokens)-2].Text))
		if convErr != nil || n <= 0 {
			return "", 0, false
		}
		start := int(tokens[len(tokens)-3].Start)
		if start <= 0 || start > len(runes) {
			return "", 0, false
		}
		return strings.TrimSpace(string(runes[:start])), n, true
	}

	return "", 0, false
}

func safeParseDataTypeExpr(dialectName, sqlType string) (out *sqlengine.Expr, ok bool) {
	defer func() {
		if recover() != nil {
			out = nil
			ok = false
		}
	}()

	d := dialectOrDefault(dialectName)
	level := sqlengine.ErrorLevelRaise
	expr, err := d.ParseOneInto(sqlengine.KDataType, sqlType, &sqlengine.ParseOptions{ErrorLevel: &level})
	if err != nil || expr == nil || !expr.IsA(sqlengine.KDataType) {
		return nil, false
	}
	return expr, true
}

func buildParsedTypeFromExpr(cfg TypeFormatConfig, expr *sqlengine.Expr, raw string) ParsedType {
	dt := expr.DTypeOf()
	if dt == sqlengine.DT_NONE {
		if inner := expr.ArgE("this"); inner != nil && inner.IsA(sqlengine.KInterval) {
			dt = sqlengine.DT_INTERVAL
		} else {
			return ParsedType{
				Raw:     raw,
				DType:   sqlengine.DT_USERDEFINED,
				Complex: strings.ContainsAny(raw, `'"`),
			}
		}
	}

	switch dt { //nolint:exhaustive
	case sqlengine.DT_ARRAY, sqlengine.DT_LIST:
		var arrSize int
		if vals, ok := expr.Arg("values").([]*sqlengine.Expr); ok && len(vals) == 1 {
			if n, valid := parseDataTypeParamInt(vals[0]); valid && n > 0 {
				arrSize = n
			}
		}
		params := expr.Expressions()
		if len(params) == 1 && params[0] != nil && params[0].IsA(sqlengine.KDataType) {
			elemRaw := renderExprFallback(cfg.SQLDialect, params[0])
			elem := buildParsedTypeFromExpr(cfg, params[0], elemRaw)
			return ParsedType{
				Raw:       raw,
				DType:     sqlengine.DT_ARRAY,
				Elem:      &elem,
				ArraySize: arrSize,
			}
		}
		return ParsedType{
			Raw:       raw,
			DType:     sqlengine.DT_ARRAY,
			ArraySize: arrSize,
		}

	case sqlengine.DT_STRUCT, sqlengine.DT_UNION:
		fieldExprs := expr.Expressions()
		fields := make([]StructField, 0, len(fieldExprs))
		for _, fe := range fieldExprs {
			if fe == nil || !fe.IsA(sqlengine.KColumnDef) {
				fields = nil
				break
			}
			kindExpr := fe.ArgE("kind")
			if kindExpr == nil || !kindExpr.IsA(sqlengine.KDataType) {
				fields = nil
				break
			}
			fieldRaw := renderExprFallback(cfg.SQLDialect, kindExpr)
			fields = append(fields, StructField{
				Name: fe.Name(),
				Type: buildParsedTypeFromExpr(cfg, kindExpr, fieldRaw),
			})
		}
		return ParsedType{
			Raw:     raw,
			DType:   dt,
			Fields:  fields,
			Complex: true,
		}

	case sqlengine.DT_MAP:
		params := expr.Expressions()
		var mapKey, mapVal *ParsedType
		if len(params) == 2 && params[0] != nil && params[1] != nil &&
			params[0].IsA(sqlengine.KDataType) && params[1].IsA(sqlengine.KDataType) {
			k := buildParsedTypeFromExpr(cfg, params[0], renderExprFallback(cfg.SQLDialect, params[0]))
			v := buildParsedTypeFromExpr(cfg, params[1], renderExprFallback(cfg.SQLDialect, params[1]))
			mapKey, mapVal = &k, &v
		}
		return ParsedType{
			Raw:      raw,
			DType:    sqlengine.DT_MAP,
			MapKey:   mapKey,
			MapValue: mapVal,
			Complex:  true,
		}

	case sqlengine.DT_ENUM, sqlengine.DT_NESTED, sqlengine.DT_OBJECT, sqlengine.DT_RANGE:
		return ParsedType{
			Raw:     raw,
			DType:   dt,
			Complex: true,
		}

	case sqlengine.DT_USERDEFINED:
		upperRaw := strings.Join(strings.Fields(strings.ToUpper(strings.TrimSpace(raw))), " ")
		if cfg.Aliases != nil {
			if aliased, ok := cfg.Aliases[upperRaw]; ok {
				pt := ParsedType{
					Raw:   raw,
					DType: canonicalizeDType(cfg, aliased),
				}
				applyDefaultDecimalParams(cfg, &pt)
				return pt
			}
		}
		return ParsedType{
			Raw:     raw,
			DType:   sqlengine.DT_USERDEFINED,
			Complex: strings.ContainsAny(raw, `'"`),
		}
	}

	canon := canonicalizeDType(cfg, dt)
	pt := ParsedType{
		Raw:   raw,
		DType: canon,
	}

	params := expr.Expressions()
	switch canon { //nolint:exhaustive
	case sqlengine.DT_DECIMAL, sqlengine.DT_BIGDECIMAL:
		p, s, valid := extractDecimalPrecisionScale(cfg, canon, params)
		if !valid {
			return ParsedType{
				Raw:     raw,
				DType:   sqlengine.DT_USERDEFINED,
				Complex: strings.ContainsAny(raw, `'"`),
			}
		}
		if p > 0 {
			pt.Precision = p
			pt.Scale = s
		}

	case sqlengine.DT_VARCHAR, sqlengine.DT_BINARY:
		if len(params) > 0 {
			if len(params) != 1 {
				return ParsedType{
					Raw:     raw,
					DType:   sqlengine.DT_USERDEFINED,
					Complex: strings.ContainsAny(raw, `'"`),
				}
			}
			n, valid := parseDataTypeParamInt(params[0])
			if !valid || n <= 0 {
				return ParsedType{
					Raw:     raw,
					DType:   sqlengine.DT_USERDEFINED,
					Complex: strings.ContainsAny(raw, `'"`),
				}
			}
			if cfg.KeepTypeLength {
				pt.Length = n
			}
		}
	}

	return pt
}

func canonicalizeDType(cfg TypeFormatConfig, dt sqlengine.DType) sqlengine.DType {
	switch dt { //nolint:exhaustive
	case sqlengine.DT_UNKNOWN:
		dt = sqlengine.DT_NULL
	case sqlengine.DT_TEXT, sqlengine.DT_CHAR, sqlengine.DT_NCHAR, sqlengine.DT_BPCHAR,
		sqlengine.DT_NVARCHAR, sqlengine.DT_TINYTEXT, sqlengine.DT_MEDIUMTEXT, sqlengine.DT_LONGTEXT:
		dt = sqlengine.DT_VARCHAR
	case sqlengine.DT_BLOB, sqlengine.DT_VARBINARY, sqlengine.DT_BINARY,
		sqlengine.DT_TINYBLOB, sqlengine.DT_MEDIUMBLOB, sqlengine.DT_LONGBLOB:
		dt = sqlengine.DT_BINARY
	}
	if cfg.DTypeFolds != nil {
		if folded, ok := cfg.DTypeFolds[dt]; ok {
			dt = folded
		}
	}
	return dt
}

func extractDecimalPrecisionScale(cfg TypeFormatConfig, dt sqlengine.DType, params []*sqlengine.Expr) (int, int, bool) {
	if len(params) == 0 {
		if dt == sqlengine.DT_BIGDECIMAL {
			return defaultBigDecimalPrecision, defaultBigDecimalScale, true
		}
		if cfg.DefaultDecimalPrecision > 0 {
			return cfg.DefaultDecimalPrecision, cfg.DefaultDecimalScale, true
		}
		return 0, 0, true
	}
	if len(params) > 2 {
		return 0, 0, false
	}
	p, ok := parseDataTypeParamInt(params[0])
	if !ok || p <= 0 {
		return 0, 0, false
	}
	if len(params) == 1 {
		return p, 0, true
	}
	s, ok := parseDataTypeParamInt(params[1])
	if !ok || s < 0 || s > p {
		return 0, 0, false
	}
	return p, s, true
}

func parseDataTypeParamInt(param *sqlengine.Expr) (int, bool) {
	if param == nil {
		return 0, false
	}
	val := param
	if param.IsA(sqlengine.KDataTypeParam) {
		if param.ArgE("expression") != nil {
			return 0, false
		}
		val = param.This()
	}
	if val == nil || !val.IsA(sqlengine.KLiteral) || !val.IsNumber() {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(val.Name()))
	if err != nil {
		return 0, false
	}
	return n, true
}

func renderExprFallback(dialectName string, expr *sqlengine.Expr) string {
	if expr == nil {
		return ""
	}
	s, err := dialectOrDefault(dialectName).Generate(expr, nil)
	if err != nil {
		return sqlengine.ExprString(expr)
	}
	return s
}

// FormatType renders pt into its canonical SQL type representation according to cfg.
// Complex or quoted types preserve their original raw casing.
func FormatType(cfg TypeFormatConfig, pt ParsedType) string {
	if pt.DType == sqlengine.DT_NONE && strings.TrimSpace(pt.Raw) == "" {
		return ""
	}

	if pt.DType == sqlengine.DT_ARRAY && !pt.Complex {
		if pt.Elem == nil {
			return "ARRAY"
		}
		if cfg.GenericArraySyntax {
			return "ARRAY<" + FormatType(cfg, *pt.Elem) + ">"
		}
		if pt.ArraySize > 0 {
			return fmt.Sprintf("%s[%d]", FormatType(cfg, *pt.Elem), pt.ArraySize)
		}
		return FormatType(cfg, *pt.Elem) + "[]"
	}

	if pt.Complex {
		return strings.TrimSpace(pt.Raw)
	}

	if pt.DType == sqlengine.DT_USERDEFINED || pt.DType == sqlengine.DT_NONE {
		return strings.Join(strings.Fields(strings.ToUpper(strings.TrimSpace(pt.Raw))), " ")
	}

	baseName := canonicalDTypeName(cfg, pt.DType)
	switch pt.DType { //nolint:exhaustive
	case sqlengine.DT_DECIMAL:
		if pt.Precision > 0 {
			if cfg.OmitDefaultDecimalParams && pt.Precision == cfg.DefaultDecimalPrecision && pt.Scale == cfg.DefaultDecimalScale {
				return baseName
			}
			return fmt.Sprintf("%s(%d,%d)", baseName, pt.Precision, pt.Scale)
		}
	case sqlengine.DT_BIGDECIMAL:
		if pt.Precision > 0 {
			if cfg.OmitDefaultDecimalParams && pt.Precision == defaultBigDecimalPrecision && pt.Scale == defaultBigDecimalScale {
				return baseName
			}
			return fmt.Sprintf("%s(%d,%d)", baseName, pt.Precision, pt.Scale)
		}
	case sqlengine.DT_VARCHAR, sqlengine.DT_BINARY:
		if cfg.KeepTypeLength && pt.Length > 0 {
			return fmt.Sprintf("%s(%d)", baseName, pt.Length)
		}
	}
	return baseName
}

func canonicalDTypeName(cfg TypeFormatConfig, dt sqlengine.DType) string {
	if cfg.TypeNames != nil {
		if name, ok := cfg.TypeNames[dt]; ok && name != "" {
			return name
		}
	}
	return sqlengine.ExprString(sqlengine.New(sqlengine.KDataType, "this", dt))
}
