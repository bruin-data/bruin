package sqlparser

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidUTF8ReplacesEachInvalidByte(t *testing.T) {
	t.Parallel()
	for _, s := range []string{"", "SELECT 1", "SELECT 'é'", "SELECT '\xff'", "SELECT '\xe2\x82'", "\xc3\x28 FROM t", "a\xff\xfeb", "\xf0\x9f\x98"} {
		// encoding/json replaces each invalid byte with U+FFFD, which is what Bruin's parser used to
		// receive.
		b, err := json.Marshal(s)
		require.NoError(t, err)
		var want string
		require.NoError(t, json.Unmarshal(b, &want))
		require.Equal(t, want, validUTF8(s), "%q", s)
	}
}

func TestPublicMethodsReplaceInvalidUTF8(t *testing.T) {
	t.Parallel()
	p, err := NewSQLParser()
	require.NoError(t, err)
	bad := "SELECT a AS \"x\xff\" FROM \"t\xfe\""
	clean := validUTF8(bad)

	gotTables, gotErr := p.UsedTables(bad, "postgres")
	wantTables, wantErr := p.UsedTables(clean, "postgres")
	require.Equal(t, wantTables, gotTables)
	require.Equal(t, wantErr, gotErr)

	gotLimited, gotErr := p.AddLimit(bad, 5, "postgres")
	wantLimited, wantErr := p.AddLimit(clean, 5, "postgres")
	require.Equal(t, wantLimited, gotLimited)
	require.Equal(t, wantErr, gotErr)

	schema := Schema{"t\xfe": {"a": "int", "b\xff": "text"}}
	gotLineage, err := p.ColumnLineage(bad, "postgres", schema)
	require.NoError(t, err)
	wantLineage, err := p.ColumnLineage(clean, "postgres", validUTF8Schema(schema))
	require.NoError(t, err)
	require.Equal(t, wantLineage, gotLineage)
	require.NotEmpty(t, gotLineage.Columns)
	require.Equal(t, Schema{"t\uFFFD": {"a": "int", "b\uFFFD": "text"}}, validUTF8Schema(schema))
}
