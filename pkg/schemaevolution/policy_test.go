package schemaevolution_test

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/schemaevolution"
	"github.com/bruin-data/bruin/pkg/sqlengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLUtil_PureGoParserHelpers(t *testing.T) {
	t.Parallel()

	t.Run("IsConstantLiteralExpr handles literals and rejects volatile/compound expressions", func(t *testing.T) {
		t.Parallel()
		for _, valid := range []string{
			"",
			"NULL",
			"null",
			"TRUE",
			"false",
			"0",
			"42",
			"-42",
			"+3.14",
			".5",
			"1.5e-3",
			"'hello'",
			"''",
			"'it''s fine'",
		} {
			assert.True(t, schemaevolution.IsConstantLiteralExpr("duckdb", valid), "expected constant literal: %q", valid)
		}

		for _, invalid := range []string{
			"now()",
			"uuid()",
			"current_timestamp",
			"1 + 2",
			"'a' || 'b'",
			"'a' 'b'",
			"'unterminated",
			"'bad'quote'",
			"1.2.3",
		} {
			assert.False(t, schemaevolution.IsConstantLiteralExpr("duckdb", invalid), "expected non-constant: %q", invalid)
		}
	})

	t.Run("SpliceStatementsInTransaction handles leading comments and BEGIN/START variants", func(t *testing.T) {
		t.Parallel()
		alters := []string{`ALTER TABLE t ADD COLUMN "x" BIGINT`}

		withComment := "-- pre-comment\n/* block */\nBEGIN TRANSACTION;\nINSERT INTO t BY NAME SELECT 1 AS x;\nCOMMIT;"
		spliced := schemaevolution.SpliceStatementsInTransaction("duckdb", withComment, alters)
		assert.Equal(
			t,
			"-- pre-comment\n/* block */\nBEGIN TRANSACTION;\nALTER TABLE t ADD COLUMN \"x\" BIGINT;\nINSERT INTO t BY NAME SELECT 1 AS x;\nCOMMIT;",
			spliced,
		)

		plain := "INSERT INTO t BY NAME SELECT 1 AS x; -- trailing comment"
		splicedPlain := schemaevolution.SpliceStatementsInTransaction("duckdb", plain, alters)
		assert.Equal(
			t,
			"BEGIN TRANSACTION;\nALTER TABLE t ADD COLUMN \"x\" BIGINT;\nINSERT INTO t BY NAME SELECT 1 AS x;\nCOMMIT;",
			splicedPlain,
		)
	})

	t.Run("SplitTableIdentifier handles quoted identifiers with dots, spaces, and escaped quotes", func(t *testing.T) {
		t.Parallel()

		cat, sch, tbl, n := schemaevolution.SplitTableIdentifier("duckdb", `"my.schema"."my ""quoted"" table"`, "main")
		assert.Empty(t, cat)
		assert.Equal(t, "my.schema", sch)
		assert.Equal(t, `my "quoted" table`, tbl)
		assert.Equal(t, 2, n)

		cat, sch, tbl, n = schemaevolution.SplitTableIdentifier("duckdb", `"cat"."my.schema"."my.table"`, "main")
		assert.Equal(t, "cat", cat)
		assert.Equal(t, "my.schema", sch)
		assert.Equal(t, "my.table", tbl)
		assert.Equal(t, 3, n)
	})

	t.Run("QuoteDoubleQuotedIdentifier preserves dots and escapes embedded double quotes", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, `"a.b"`, schemaevolution.QuoteDoubleQuotedIdentifier("a.b"))
		assert.Equal(t, `"x""y"`, schemaevolution.QuoteDoubleQuotedIdentifier(`x"y`))
		assert.Equal(t, `"order"`, schemaevolution.QuoteDoubleQuotedIdentifier("order"))
	})
}

func TestPolicyDialect_Capabilities(t *testing.T) {
	t.Parallel()

	strictPolicy := schemaevolution.PolicyDialect{
		Policy: schemaevolution.TypePolicy{
			Name: "StrictEngine",
			Format: schemaevolution.TypeFormatConfig{
				SQLDialect:              "duckdb",
				DefaultDecimalPrecision: 18,
				DefaultDecimalScale:     3,
			},
			NotNullAddedColumnReason: "cannot add NOT NULL column in StrictEngine",
			RequireLiteralDefault:    true,
			MaxSignedIntDType:        sqlengine.DT_BIGINT,
			MaxUnsignedIntDType:      sqlengine.DT_UBIGINT,
			MaxDecimalPrecision:      38,
		},
	}

	res, target, _ := strictPolicy.ResolveTypeChange(
		schemaevolution.Column{Type: "INT"},
		schemaevolution.Column{Type: "BIGINT"},
	)
	assert.Equal(t, schemaevolution.TypeChangeWiden, res)
	assert.Equal(t, "BIGINT", target)

	res, _, reason := strictPolicy.ResolveTypeChange(
		schemaevolution.Column{Type: "BIGINT"},
		schemaevolution.Column{Type: "INT128"},
	)
	assert.Equal(t, schemaevolution.TypeChangeIncompatible, res)
	assert.Contains(t, reason, "StrictEngine")

	_, reason, ok := strictPolicy.ValidateNewColumn(schemaevolution.Column{
		Name:    "created_at",
		Type:    "TIMESTAMP",
		Default: "now()",
	})
	require.False(t, ok)
	assert.Contains(t, reason, "StrictEngine")

	_, reason, ok = strictPolicy.ValidateNewColumn(schemaevolution.Column{
		Name:    "required_col",
		Type:    "VARCHAR",
		NotNull: true,
	})
	require.False(t, ok)
	assert.Contains(t, reason, "NOT NULL")
}
