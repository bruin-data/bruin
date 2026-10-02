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
		{"nested block", "SELECT 0;\nBEGIN\n BEGIN\n  DECLARE nested INT;\n END;\nEND;\nDECLARE top_level INT;", "DECLARE top_level INT;\nSELECT 0;\nBEGIN\n BEGIN\n  DECLARE nested INT;\n END;\nEND;"},
		{"already ordered is byte stable", "DECLARE x INT64;\n-- π;\nSELECT ';' AS semi;", "DECLARE x INT64;\n-- π;\nSELECT ';' AS semi;"},
		{"nil text", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sharedSQLParser.HoistDeclares(tc.in, pipeline.AssetTypeBigqueryQuery)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	bad := "SELECT 1; DECLARE x STRING; SELECT 'unterminated"
	got, err := sharedSQLParser.HoistDeclares(bad, pipeline.AssetTypeBigqueryQuery)
	require.Error(t, err)
	require.Equal(t, bad, got)
	got, err = sharedSQLParser.HoistDeclares("SELECT 1; DECLARE x INT", pipeline.AssetTypePython)
	require.Error(t, err)
	require.Equal(t, "SELECT 1; DECLARE x INT", got)
}

func TestHoistDeclaresListContractAcrossMappedDialects(t *testing.T) {
	t.Parallel()
	for asset := range assetTypeDialectMap {
		t.Run(string(asset), func(t *testing.T) {
			t.Parallel()
			// An entry containing a whole script is intentionally kept whole and
			// classified as non-DECLARE; only declaration-only entries move.
			in := []string{"SELECT 1; DECLARE embedded INT", "  DECLARE α STRING  ", "-- lead\nDECLARE beta INT", "SELECT ';'"}
			got, err := sharedSQLParser.HoistDeclaresList(in, asset)
			require.NoError(t, err)
			require.Equal(t, []string{"  DECLARE α STRING  ", "-- lead\nDECLARE beta INT", "SELECT 1; DECLARE embedded INT", "SELECT ';'"}, got)
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
	require.Error(t, err)
	require.Equal(t, in, got)
	// Unlike the string operation, list classification swallows parse errors and
	// preserves malformed whole entries in the non-declaration partition.
	malformed := []string{"SELECT 'unterminated", "DECLARE x INT"}
	got, err = sharedSQLParser.HoistDeclaresList(malformed, pipeline.AssetTypeBigqueryQuery)
	require.NoError(t, err)
	require.Equal(t, []string{"DECLARE x INT", "SELECT 'unterminated"}, got)
}
