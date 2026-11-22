package postgres

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_StreamWithSchemaConvertsTypeDependentValues(t *testing.T) {
	t.Parallel()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRowsWithColumnDefinition(
		pgconn.FieldDescription{Name: "doc", DataTypeOID: pgtype.XMLOID},
		pgconn.FieldDescription{Name: "payload", DataTypeOID: pgtype.ByteaOID},
		pgconn.FieldDescription{Name: "kind", DataTypeOID: pgtype.QCharOID},
		pgconn.FieldDescription{Name: "kinds", DataTypeOID: pgtype.QCharArrayOID},
		pgconn.FieldDescription{Name: "id", DataTypeOID: pgtype.Int4OID},
	).
		AddRow([]byte("<a>1</a>"), []byte{0xde, 0xad, 0xbe, 0xef}, int32('x'), []any{int32('r'), nil}, int32(120)).
		AddRow(nil, nil, nil, nil, nil)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	client := Client{connection: mock}
	h := &recordingRowHandler{}
	_, err = client.StreamWithSchema(t.Context(), &query.Query{Query: "SELECT"}, h)
	require.NoError(t, err)

	assert.Equal(t, [][]interface{}{
		// The int4 column holds the same int32 as the "char" one and stays a number.
		{"<a>1</a>", `\xdeadbeef`, "x", []any{"r", nil}, int32(120)},
		{nil, nil, nil, nil, nil},
	}, h.rows)
}

func TestFormatQChar(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "a", formatQChar('a'))
	assert.Empty(t, formatQChar(0))
	assert.Equal(t, `\351`, formatQChar(0xe9))
}

func TestNestArrayElements(t *testing.T) {
	t.Parallel()

	typeMap := pgtype.NewMap()
	encode := func(oid uint32, value any) []byte {
		raw, err := typeMap.Encode(oid, pgtype.BinaryFormatCode, value, nil)
		require.NoError(t, err)
		return raw
	}

	tests := []struct {
		name     string
		raw      []byte
		elements []any
		expected []any
	}{
		{
			name:     "two dimensions",
			raw:      encode(pgtype.Int4ArrayOID, [][]int32{{1, 2}, {3, 4}}),
			elements: []any{int32(1), int32(2), int32(3), int32(4)},
			expected: []any{[]any{int32(1), int32(2)}, []any{int32(3), int32(4)}},
		},
		{
			name:     "three dimensions",
			raw:      encode(pgtype.TextArrayOID, [][][]string{{{"a"}, {"b"}}, {{"c"}, {"d"}}}),
			elements: []any{"a", "b", "c", "d"},
			expected: []any{[]any{[]any{"a"}, []any{"b"}}, []any{[]any{"c"}, []any{"d"}}},
		},
		{
			name:     "one dimension stays flat",
			raw:      encode(pgtype.Int4ArrayOID, []int32{1, 2, 3}),
			elements: []any{int32(1), int32(2), int32(3)},
			expected: []any{int32(1), int32(2), int32(3)},
		},
		{
			name:     "element count mismatch stays flat",
			raw:      encode(pgtype.Int4ArrayOID, [][]int32{{1, 2}, {3, 4}}),
			elements: []any{int32(1), int32(2)},
			expected: []any{int32(1), int32(2)},
		},
		{
			name:     "out of range dimension count stays flat",
			raw:      append([]byte{0xff, 0xff, 0xff, 0xff}, make([]byte, 60)...),
			elements: []any{int32(1)},
			expected: []any{int32(1)},
		},
		{
			name:     "truncated header stays flat",
			raw:      []byte{0, 0, 0, 2},
			elements: []any{int32(1)},
			expected: []any{int32(1)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, nestArrayElements(tt.raw, tt.elements))
		})
	}
}

func TestResultValueConvertersSkipsPlainColumns(t *testing.T) {
	t.Parallel()

	fields := []pgconn.FieldDescription{
		{Name: "id", DataTypeOID: pgtype.Int8OID},
		{Name: "name", DataTypeOID: pgtype.TextOID},
	}
	assert.Nil(t, resultValueConverters(fields, pgtype.NewMap()))
}
