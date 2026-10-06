package sqlparser

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestHoistDeclaresContractAcrossMappedDialects(t *testing.T) {
	t.Parallel()
	for asset := range assetTypeDialectMap {
		t.Run(string(asset), func(t *testing.T) {
			t.Parallel()
			in := "SELECT 'é;DECLARE fake INT' AS café;\n/* setup ; */ DECLARE first_value INT;\nDECLARE second_value STRING;\nSELECT 2;"
			got, err := sharedSQLParser.HoistDeclares(in, asset)
			require.NoError(t, err)
			require.Equal(t, "/* setup ; */ DECLARE first_value INT;\nDECLARE second_value STRING;\nSELECT 'é;DECLARE fake INT' AS café;\nSELECT 2;", got)
		})
	}
}

func TestHoistDeclaresContractBlocksAndPreservation(t *testing.T) {
	t.Parallel()
	cases := []struct{ name, in, want string }{
		{"nested block", "SELECT 0;\nBEGIN\n BEGIN\n  DECLARE nested INT;\n END;\nEND;\nDECLARE top_level INT;", "DECLARE top_level INT;\nSELECT 0;\nBEGIN\n BEGIN\n  DECLARE nested INT;\n END;\nEND;"}, //nolint:dupword // nested END; END; is the point of the case
		{"already ordered is byte stable", "DECLARE x INT64;\n-- π;\nSELECT ';' AS semi;", "DECLARE x INT64;\n-- π;\nSELECT ';' AS semi;"},
		{"nil text", "", ""},
		{"declaration remains attached to initial IF slice", "SELECT 1; IF TRUE THEN DECLARE x INT64; END IF; DECLARE y INT64;", "DECLARE y INT64;\nSELECT 1;\nIF TRUE THEN DECLARE x INT64;\nEND IF;"},
		{"declaration remains attached to initial LOOP slice", "SELECT 1; LOOP DECLARE x INT64; END LOOP; DECLARE y INT64;", "DECLARE y INT64;\nSELECT 1;\nLOOP DECLARE x INT64;\nEND LOOP;"},
		{"dollar quotes are not native bigquery strings", "SELECT $$DECLARE fake INT;$$; DECLARE real INT;", "DECLARE real INT;\nSELECT $$DECLARE fake INT;\n$$;"},
		// END IF decrements BEGIN nesting even though IF did not increment it.
		{"end if incorrectly exposes inner declaration", "SELECT 1; BEGIN IF TRUE THEN SELECT 2; END IF; DECLARE inner_x INT64; END; DECLARE outer_x INT64;", "DECLARE inner_x INT64;\nDECLARE outer_x INT64;\nSELECT 1;\nBEGIN IF TRUE THEN SELECT 2; END IF;\nEND;"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := sharedSQLParser.HoistDeclares(tc.in, pipeline.AssetTypeBigqueryQuery)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	bad := "SELECT 1; DECLARE x STRING; SELECT 'unterminated"
	got, err := sharedSQLParser.HoistDeclares(bad, pipeline.AssetTypeBigqueryQuery)
	require.EqualError(t, err, "Error tokenizing 'SELECT 1; DECLARE x STRING; SELECT 'unterminate'")
	require.Equal(t, bad, got)
	got, err = sharedSQLParser.HoistDeclares("SELECT 1; DECLARE x INT", pipeline.AssetTypePython)
	require.EqualError(t, err, "unsupported asset type python")
	require.Equal(t, "SELECT 1; DECLARE x INT", got)
}

func TestHoistDeclaresContractNativeDialectTokens(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, query, want string
		assetType         pipeline.AssetType
	}{
		{"postgres dollar quoted body", "SELECT $$DECLARE fake INT;$$; DECLARE real_x INT;", "DECLARE real_x INT;\nSELECT $$DECLARE fake INT;$$;", pipeline.AssetTypePostgresQuery},
		{"tsql table variable", "SELECT 1; DECLARE @t TABLE(id INT); SELECT * FROM @t; DECLARE @x INT;", "DECLARE @t TABLE(id INT);\nDECLARE @x INT;\nSELECT 1;\nSELECT * FROM @t;", pipeline.AssetTypeMsSQLQuery},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := sharedSQLParser.HoistDeclares(tc.query, tc.assetType)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestHoistDeclaresListContractAcrossMappedDialects(t *testing.T) {
	t.Parallel()
	for asset := range assetTypeDialectMap {
		t.Run(string(asset), func(t *testing.T) {
			t.Parallel()
			// An entry containing a whole script is intentionally kept whole and
			// classified as non-DECLARE; only declaration-only entries move.
			in := []string{"SELECT 1; DECLARE embedded INT", "  DECLARE α STRING  ", "-- lead\nDECLARE beta INT", "SELECT ';'"}
			original := append([]string(nil), in...)
			got, err := sharedSQLParser.HoistDeclaresList(in, asset)
			require.NoError(t, err)
			require.Equal(t, []string{"  DECLARE α STRING  ", "-- lead\nDECLARE beta INT", "SELECT 1; DECLARE embedded INT", "SELECT ';'"}, got)
			require.Equal(t, original, in)
		})
	}
}

func TestHoistDeclaresListContractInputsAndErrors(t *testing.T) {
	t.Parallel()
	for _, in := range [][]string{nil, {}} {
		got, err := sharedSQLParser.HoistDeclaresList(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, in, got)
	}
	in := []string{"SELECT 1", "DECLARE x INT"}
	got, err := sharedSQLParser.HoistDeclaresList(in, pipeline.AssetTypePython)
	require.EqualError(t, err, "unsupported asset type python")
	require.Equal(t, in, got)
	// Unlike the string operation, list classification swallows parse errors and
	// preserves malformed whole entries in the non-declaration partition.
	malformed := []string{"SELECT 'unterminated", "DECLARE x INT"}
	got, err = sharedSQLParser.HoistDeclaresList(malformed, pipeline.AssetTypeBigqueryQuery)
	require.NoError(t, err)
	require.Equal(t, []string{"DECLARE x INT", "SELECT 'unterminated"}, got)
	mixed := []string{"SELECT 1", "DECLARE a INT, b STRING", "DECLARE c INT; SELECT 2"}
	got, err = sharedSQLParser.HoistDeclaresList(mixed, pipeline.AssetTypeBigqueryQuery)
	require.NoError(t, err)
	require.Equal(t, []string{"DECLARE a INT, b STRING", "SELECT 1", "DECLARE c INT; SELECT 2"}, got)
}

// The expected values below were recorded from the Python reference (codegen/pythonsrc).
func TestHoistDeclaresContractScripts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		assetType pipeline.AssetType
		in, want  string
	}{
		{"declarations keep their relative order", pipeline.AssetTypeBigqueryQuery, "SELECT 1;\nDECLARE b INT64;\nSELECT 2;\nDECLARE a INT64;", "DECLARE b INT64;\nDECLARE a INT64;\nSELECT 1;\nSELECT 2;"},
		{"empty statements are dropped", pipeline.AssetTypeBigqueryQuery, "SELECT 1;;\nDECLARE x INT64;;", "DECLARE x INT64;\nSELECT 1;"},
		{"CRLF separators are normalized", pipeline.AssetTypeBigqueryQuery, "SELECT 1;\r\nDECLARE x INT64;\r\n", "DECLARE x INT64;\nSELECT 1;"},
		{"whitespace-only input is returned verbatim", pipeline.AssetTypeBigqueryQuery, "  \n ", "  \n "},
		{"subquery default moves with its declaration", pipeline.AssetTypeBigqueryQuery, "SELECT 1;\nDECLARE x INT64 DEFAULT (SELECT MAX(ts) FROM ds.events);", "DECLARE x INT64 DEFAULT (SELECT MAX(ts) FROM ds.events);\nSELECT 1;"},
		{"semicolon inside parentheses does not split", pipeline.AssetTypeBigqueryQuery, "DECLARE x INT64 DEFAULT (SELECT 1; );\nSELECT 2;", "DECLARE x INT64 DEFAULT (SELECT 1; );\nSELECT 2;"},
		{"unbalanced closing parenthesis is ignored", pipeline.AssetTypeBigqueryQuery, "SELECT 1);\nDECLARE x INT64;", "DECLARE x INT64;\nSELECT 1);"},
		{"unbalanced opening parenthesis swallows the rest", pipeline.AssetTypeBigqueryQuery, "SELECT (1;\nDECLARE x INT64;", "SELECT (1;\nDECLARE x INT64;"},
		{"exception handler stays inside its block", pipeline.AssetTypeBigqueryQuery, "SELECT 1;\nBEGIN\n  DECLARE y INT64;\n  SELECT y;\nEXCEPTION WHEN ERROR THEN\n  SELECT @@error.message;\nEND;\nDECLARE x INT64;", "DECLARE x INT64;\nSELECT 1;\nBEGIN\n  DECLARE y INT64;\n  SELECT y;\nEXCEPTION WHEN ERROR THEN\n  SELECT @@error.message;\nEND;"},
		{"snowflake dollar-quoted block is one statement", pipeline.AssetTypeSnowflakeQuery, "SELECT 1;\nEXECUTE IMMEDIATE $$ DECLARE y INT; BEGIN RETURN y; END $$;\nDECLARE x INT;", "DECLARE x INT;\nSELECT 1;\nEXECUTE IMMEDIATE $$ DECLARE y INT; BEGIN RETURN y; END $$;"},
		{"postgres DO body is one statement", pipeline.AssetTypePostgresQuery, "SELECT 1;\nDO $$ BEGIN DECLARE y INT; END $$;\nDECLARE x INT;", "DECLARE x INT;\nSELECT 1;\nDO $$ BEGIN DECLARE y INT; END $$;"},
		{"tsql TRY and CATCH blocks keep their declarations", pipeline.AssetTypeMsSQLQuery, "SELECT 1;\nBEGIN TRY\n  DECLARE @y INT;\nEND TRY\nBEGIN CATCH\n  SELECT 2;\nEND CATCH;\nDECLARE @x INT;", "DECLARE @x INT;\nSELECT 1;\nBEGIN TRY\n  DECLARE @y INT;\nEND TRY\nBEGIN CATCH\n  SELECT 2;\nEND CATCH;"},
		{"tsql IF BEGIN block keeps its declaration", pipeline.AssetTypeMsSQLQuery, "SET @x = 1;\nIF @x > 0 BEGIN DECLARE @y INT; END;\nDECLARE @z INT;", "DECLARE @z INT;\nSET @x = 1;\nIF @x > 0 BEGIN DECLARE @y INT; END;"},
		{"databricks DECLARE VARIABLE", pipeline.AssetTypeDatabricksQuery, "SELECT 1;\nDECLARE VARIABLE x INT DEFAULT 5;", "DECLARE VARIABLE x INT DEFAULT 5;\nSELECT 1;"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := sharedSQLParser.HoistDeclares(tc.in, tc.assetType)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestHoistDeclaresListContractPartition(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		assetType pipeline.AssetType
		in, want  []string
	}{
		{"padding is preserved", pipeline.AssetTypeBigqueryQuery, []string{"SELECT 1", "  DECLARE x INT64  "}, []string{"  DECLARE x INT64  ", "SELECT 1"}},
		{"trailing semicolon still classifies as a declaration", pipeline.AssetTypeBigqueryQuery, []string{"SELECT 1", "DECLARE x INT64;"}, []string{"DECLARE x INT64;", "SELECT 1"}},
		{"empty entries stay with the statements", pipeline.AssetTypeBigqueryQuery, []string{"", "SELECT 1", "DECLARE x INT64"}, []string{"DECLARE x INT64", "", "SELECT 1"}},
		{"stable partition of interleaved entries", pipeline.AssetTypeBigqueryQuery, []string{"DECLARE a INT64", "SELECT 1", "DECLARE b INT64", "SELECT 2", "DECLARE c INT64"}, []string{"DECLARE a INT64", "DECLARE b INT64", "DECLARE c INT64", "SELECT 1", "SELECT 2"}},
		{"tsql declaration with initializer", pipeline.AssetTypeMsSQLQuery, []string{"SELECT 1", "DECLARE @x INT = 5"}, []string{"DECLARE @x INT = 5", "SELECT 1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := sharedSQLParser.HoistDeclaresList(tc.in, tc.assetType)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestHoistDeclaresContractDialectNames(t *testing.T) {
	t.Parallel()
	// Bruin's asset types never map to an unknown dialect, but the engine must still fail softly.
	require.Equal(t, map[string]any{"query": "SELECT 1; DECLARE x INT;", "error": "Unknown dialect 'not_a_dialect'."}, hoistDeclares("SELECT 1; DECLARE x INT;", "not_a_dialect"))
	// The list operation falls back to BigQuery's DECLARE grammar for each entry.
	require.Equal(t, map[string]any{"queries": []string{"DECLARE x INT64", "SELECT 1"}, "error": ""}, hoistDeclaresList([]string{"SELECT 1", "DECLARE x INT64"}, "not_a_dialect"))
	// vertica is read as postgres, so dollar quotes are strings; bigquery has no dollar quotes.
	require.Equal(t, "DECLARE x INT;\nSELECT $$a;b$$;", hoistDeclares("SELECT $$a;b$$;\nDECLARE x INT;", "vertica")["query"])
	require.Equal(t, "DECLARE x INT;\nSELECT $$a;\nb$$;", hoistDeclares("SELECT $$a;b$$;\nDECLARE x INT;", "bigquery")["query"])
}
