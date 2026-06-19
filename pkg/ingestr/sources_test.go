package ingestr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSharePointSourceTables(t *testing.T) {
	t.Parallel()

	source, err := GetSourceTables("sharepoint")
	require.NoError(t, err)
	require.Equal(t, "sharepoint", source.Name)
	require.NotEmpty(t, source.Tables)

	var hasExcelSheetExample, hasCSVExample bool
	for _, table := range source.Tables {
		switch table.Name {
		case "<path/to/file.xlsx>#sheet=<sheet_name>":
			hasExcelSheetExample = true
			require.Equal(t, "replace", table.IncStrategy)
		case "<path/to/file.csv>#csv,encoding=utf-16le,sep=tab":
			hasCSVExample = true
			require.Equal(t, "replace", table.IncStrategy)
		}
	}

	require.True(t, hasExcelSheetExample)
	require.True(t, hasCSVExample)
}
