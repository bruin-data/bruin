package duck

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/schemaevolution"
	"github.com/bruin-data/bruin/pkg/sqlengine"
)

const (
	duckDBSQLDialect          = "duckdb"
	duckDBMaxDecimalPrecision = 38
)

var duckDBTypeFormatConfig = schemaevolution.TypeFormatConfig{
	SQLDialect: duckDBSQLDialect,
	Aliases: map[string]sqlengine.DType{
		"UINT1":   sqlengine.DT_UTINYINT,
		"UINT2":   sqlengine.DT_USMALLINT,
		"UINT4":   sqlengine.DT_UINT,
		"UINT8":   sqlengine.DT_UBIGINT,
		"SQLNULL": sqlengine.DT_NULL,
	},
	DTypeFolds: map[sqlengine.DType]sqlengine.DType{
		sqlengine.DT_DATETIME:     sqlengine.DT_TIMESTAMP,
		sqlengine.DT_TIMESTAMPNTZ: sqlengine.DT_TIMESTAMP,
	},
	TypeNames: map[sqlengine.DType]string{
		sqlengine.DT_INT:         "INTEGER",
		sqlengine.DT_INT128:      "HUGEINT",
		sqlengine.DT_UINT:        "UINTEGER",
		sqlengine.DT_UINT128:     "UHUGEINT",
		sqlengine.DT_FLOAT:       "FLOAT",
		sqlengine.DT_DOUBLE:      "DOUBLE",
		sqlengine.DT_VARCHAR:     "VARCHAR",
		sqlengine.DT_BINARY:      "BLOB",
		sqlengine.DT_TIMESTAMP:   "TIMESTAMP",
		sqlengine.DT_TIMESTAMPTZ: "TIMESTAMP WITH TIME ZONE",
		sqlengine.DT_TIMETZ:      "TIME WITH TIME ZONE",
		sqlengine.DT_TIME:        "TIME",
		sqlengine.DT_NULL:        "SQLNULL",
	},
	DefaultDecimalPrecision: 18,
	DefaultDecimalScale:     3,
	PreprocessTypeSQL:       rewriteDuckDBTypeofNullTokens,
}

var duckDBBasePolicy = schemaevolution.PolicyDialect{
	Policy: schemaevolution.TypePolicy{
		Name:                           "DuckDB",
		Format:                         duckDBTypeFormatConfig,
		MaxSignedIntDType:              sqlengine.DT_INT128,
		MaxUnsignedIntDType:            sqlengine.DT_UINT128,
		AllowUnsignedToSigned:          true,
		AllowDecimalWidening:           true,
		AllowIntToDecimal:              true,
		MaxDecimalPrecision:            duckDBMaxDecimalPrecision,
		UnboundedVarcharAbsorbsAll:     true,
		ImplicitVarcharTargetPredicate: acceptsDuckDBImplicitVarcharInsert,
		AllowDateToTimestamp:           true,
		AllowTimestampToTimestampTZ:    true,
	},
}

var duckLakeBasePolicy = schemaevolution.PolicyDialect{
	Policy: schemaevolution.TypePolicy{
		Name:                           "DuckLake",
		Format:                         duckDBTypeFormatConfig,
		NotNullAddedColumnReason:       "cannot add a new NOT NULL column in DuckLake",
		RequireLiteralDefault:          true,
		MaxSignedIntDType:              sqlengine.DT_BIGINT,
		MaxUnsignedIntDType:            sqlengine.DT_UBIGINT,
		MaxDecimalPrecision:            duckDBMaxDecimalPrecision,
		UnboundedVarcharAbsorbsAll:     true,
		ImplicitVarcharTargetPredicate: acceptsDuckDBImplicitVarcharInsert,
	},
}

func acceptsDuckDBImplicitVarcharInsert(cur schemaevolution.ParsedType) bool {
	switch cur.DType { //nolint:exhaustive
	case sqlengine.DT_ENUM,
		sqlengine.DT_DATE,
		sqlengine.DT_TIMESTAMP_S,
		sqlengine.DT_TIMESTAMP_MS,
		sqlengine.DT_TIMESTAMP,
		sqlengine.DT_TIMESTAMP_NS,
		sqlengine.DT_TIMESTAMPTZ,
		sqlengine.DT_TIME,
		sqlengine.DT_TIMETZ,
		sqlengine.DT_UUID,
		sqlengine.DT_JSON:
		return true
	default:
		return false
	}
}

// rewriteDuckDBTypeofNullTokens normalizes DuckDB typeof()'s `"NULL"` placeholder tokens in
// type positions (e.g. `STRUCT(x "NULL")` or `MAP("NULL", "NULL")`) into `UNKNOWN` so
// pkg/sqlengine parses them into DT_NULL leaves while preserving struct field names named `"NULL"`.
func rewriteDuckDBTypeofNullTokens(sqlType string) string {
	if !strings.Contains(sqlType, `"NULL"`) {
		return sqlType
	}
	tokens, err := sqlengine.MustDialect(duckDBSQLDialect).Tokenize(sqlType)
	if err != nil || len(tokens) == 0 {
		return sqlType
	}
	runes := []rune(sqlType)
	var b strings.Builder
	cursor := 0
	for i, tok := range tokens {
		if tok.Type != sqlengine.TK_IDENTIFIER || tok.Text != "NULL" {
			continue
		}
		isTypePos := i+1 == len(tokens)
		if !isTypePos {
			switch tokens[i+1].Type {
			case sqlengine.TK_COMMA, sqlengine.TK_R_PAREN, sqlengine.TK_GT, sqlengine.TK_L_BRACKET:
				isTypePos = true
			default:
			}
		}
		if !isTypePos {
			continue
		}
		start := int(tok.Start)
		end := int(tok.End) + 1
		if start < cursor || end > len(runes) {
			continue
		}
		b.WriteString(string(runes[cursor:start]))
		b.WriteString("UNKNOWN")
		cursor = end
	}
	if cursor == 0 {
		return sqlType
	}
	b.WriteString(string(runes[cursor:]))
	return b.String()
}

// DuckDBDialect implements schemaevolution.Dialect for DuckDB and DuckLake by embedding
// PolicyDialect and providing DuckDB's ALTER TABLE statement rendering.
type DuckDBDialect struct {
	schemaevolution.PolicyDialect
	DuckLake        bool
	RebuildsViaCTAS bool
}

func newDuckDBDialect(duckLake, rebuildsViaCTAS, dataVault bool) DuckDBDialect {
	base := duckDBBasePolicy
	if duckLake {
		base = duckLakeBasePolicy
	}
	if dataVault {
		base.Policy.NotNullAddedColumnReason = "cannot add a new NOT NULL column to a Data Vault table via ALTER TABLE; set nullable: true or rebuild the table"
	}
	return DuckDBDialect{
		PolicyDialect:   base,
		DuckLake:        duckLake,
		RebuildsViaCTAS: rebuildsViaCTAS,
	}
}

// CheckMissingColumn implements schemaevolution.MissingColumnChecker for DuckDB SCD2
// strategies, which rebuild the table via CREATE OR REPLACE TABLE and would otherwise
// drop any existing column removed from asset.Columns.
func (d DuckDBDialect) CheckMissingColumn(existing schemaevolution.Column) (string, bool) {
	if !d.RebuildsViaCTAS {
		return "", false
	}
	switch strings.ToLower(strings.TrimSpace(existing.Name)) {
	case "_valid_from", "_valid_until", "_is_current":
		return "", false
	}
	return "column exists in the table but is missing from asset `columns`; SCD2 rebuilds the table via CREATE OR REPLACE and would drop it", true
}

// AddColumnStatements builds the DuckDB ALTER TABLE statement(s) to add col.
// Constant defaults are included inline on ADD COLUMN; non-constant/volatile defaults
// (e.g. now(), uuid()) are split into ADD COLUMN + ALTER COLUMN SET DEFAULT because
// DuckDB fails at commit when a volatile-default ADD COLUMN is followed by another ALTER
// on the same table in the same transaction.
func (d DuckDBDialect) AddColumnStatements(tableName string, col schemaevolution.Column) []string {
	quotedCol := schemaevolution.QuoteDoubleQuotedIdentifier(col.Name)
	stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tableName, quotedCol, col.Type)
	if col.Collation != "" {
		stmt += " COLLATE " + col.Collation
	}
	if col.Default == "" {
		return []string{stmt}
	}
	if schemaevolution.IsConstantLiteralExpr(duckDBSQLDialect, col.Default) {
		return []string{stmt + " DEFAULT " + col.Default}
	}
	return []string{
		stmt,
		fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s", tableName, quotedCol, col.Default),
	}
}

// AlterColumnTypeSQL builds the DuckDB ALTER TABLE ALTER COLUMN SET DATA TYPE statement.
// When widening naive DATE/TIMESTAMP (or list/fixed-size array thereof) to TIMESTAMP WITH TIME ZONE,
// an explicit UTC USING clause keeps historical values independent of the session time zone.
func (d DuckDBDialect) AlterColumnTypeSQL(tableName string, action schemaevolution.Action) string {
	quotedCol := schemaevolution.QuoteDoubleQuotedIdentifier(action.ColumnName)
	stmt := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s", tableName, quotedCol, action.ToType)
	if !d.DuckLake {
		pt := schemaevolution.ParseType(duckDBTypeFormatConfig, action.ToType)
		if pt.ArrayLeaf().DType == sqlengine.DT_TIMESTAMPTZ {
			stmt += " USING " + buildUTCTimestampExprForType(quotedCol, pt, 1)
		}
	}
	return stmt
}

func buildUTCTimestampExprForType(expr string, pt schemaevolution.ParsedType, depth int) string {
	if pt.DType != sqlengine.DT_ARRAY || pt.Elem == nil {
		return fmt.Sprintf("timezone('UTC', CAST(%s AS TIMESTAMP))", expr)
	}
	param := fmt.Sprintf("__x%d", depth)
	inner := buildUTCTimestampExprForType(param, *pt.Elem, depth+1)
	if pt.ArraySize > 0 {
		targetType := schemaevolution.FormatType(duckDBTypeFormatConfig, pt)
		return fmt.Sprintf("CAST(list_transform((%s)[:], %s -> %s) AS %s)", expr, param, inner, targetType)
	}
	return fmt.Sprintf("list_transform(%s, %s -> %s)", expr, param, inner)
}

// RenderStatements renders all planned evolution actions into DuckDB ALTER TABLE statements,
// ordering type alterations before column additions.
func (d DuckDBDialect) RenderStatements(tableName string, plan *schemaevolution.Plan) []string {
	if plan.IsEmpty() {
		return nil
	}
	stmts := make([]string, 0, len(plan.Actions))
	for _, action := range plan.Actions {
		if action.Type == schemaevolution.ActionAlterColumnType {
			stmts = append(stmts, d.AlterColumnTypeSQL(tableName, action))
		}
	}
	for _, action := range plan.Actions {
		if action.Type == schemaevolution.ActionAddColumn {
			stmts = append(stmts, d.AddColumnStatements(tableName, action.Column)...)
		}
	}
	return stmts
}
