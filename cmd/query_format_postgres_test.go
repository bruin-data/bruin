package cmd

import (
	"database/sql"
	"math"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

// scanPgText decodes Postgres text output into a pgx value, as a query result would hold it.
func scanPgText[T any, PT interface {
	*T
	sql.Scanner
}](t *testing.T, text string) T {
	t.Helper()
	var value T
	require.NoError(t, PT(&value).Scan(text))
	return value
}

func TestFormatPostgresTypeCells(t *testing.T) {
	t.Parallel()

	uuid := [16]byte{0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a, 0x11}
	mac, err := net.ParseMAC("08:00:2b:01:02:03")
	require.NoError(t, err)

	tests := []struct {
		name         string
		cell         interface{}
		expectedText string
		expectedJSON string
	}{
		{name: "interval", cell: pgtype.Interval{Microseconds: 7_203_500_000, Days: 1, Valid: true}, expectedText: "1 day 02:00:03.5", expectedJSON: `"1 day 02:00:03.5"`},
		{
			name:         "negative interval",
			cell:         pgtype.Interval{Microseconds: -14_706_000_000, Days: 3, Months: -14, Valid: true},
			expectedText: "-1 years -2 mons +3 days -04:05:06",
			expectedJSON: `"-1 years -2 mons +3 days -04:05:06"`,
		},
		{name: "interval with singular units", cell: pgtype.Interval{Microseconds: 1_000_000, Days: 1, Months: 13, Valid: true}, expectedText: "1 year 1 mon 1 day 00:00:01", expectedJSON: `"1 year 1 mon 1 day 00:00:01"`},
		{name: "interval with positive time after negative days", cell: pgtype.Interval{Microseconds: 7_200_000_000, Days: -1, Valid: true}, expectedText: "-1 days +02:00:00", expectedJSON: `"-1 days +02:00:00"`},
		{name: "interval over a day of hours", cell: pgtype.Interval{Microseconds: 360_000_000_000, Valid: true}, expectedText: "100:00:00", expectedJSON: `"100:00:00"`},
		{name: "zero interval", cell: pgtype.Interval{Valid: true}, expectedText: "00:00:00", expectedJSON: `"00:00:00"`},
		{name: "infinite interval", cell: pgtype.Interval{Microseconds: math.MaxInt64, Days: math.MaxInt32, Months: math.MaxInt32, Valid: true}, expectedText: "infinity", expectedJSON: `"infinity"`},
		{name: "null interval", cell: pgtype.Interval{}, expectedText: "", expectedJSON: "null"},
		{name: "time", cell: pgtype.Time{Microseconds: 49_506_123_456, Valid: true}, expectedText: "13:45:06.123456", expectedJSON: `"13:45:06.123456"`},
		{name: "end of day time", cell: pgtype.Time{Microseconds: 86_400_000_000, Valid: true}, expectedText: "24:00:00", expectedJSON: `"24:00:00"`},
		{name: "uuid", cell: uuid, expectedText: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", expectedJSON: `"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`},
		{
			name:         "integer range",
			cell:         pgtype.Range[interface{}]{Lower: int32(1), Upper: int32(10), LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true},
			expectedText: "[1,10)",
			expectedJSON: `"[1,10)"`,
		},
		{
			name: "numeric range",
			cell: pgtype.Range[interface{}]{
				Lower: pgNumeric(t, "1.5"), Upper: pgNumeric(t, "2.25"),
				LowerType: pgtype.Exclusive, UpperType: pgtype.Inclusive, Valid: true,
			},
			expectedText: "(1.5,2.25]",
			expectedJSON: `"(1.5,2.25]"`,
		},
		{
			name:         "unbounded range",
			cell:         pgtype.Range[interface{}]{Lower: int64(5), LowerType: pgtype.Inclusive, UpperType: pgtype.Unbounded, Valid: true},
			expectedText: "[5,)",
			expectedJSON: `"[5,)"`,
		},
		{name: "empty range", cell: pgtype.Range[interface{}]{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Valid: true}, expectedText: "empty", expectedJSON: `"empty"`},
		{
			name:         "range bounds that need quoting",
			cell:         pgtype.Range[interface{}]{Lower: "a b", Upper: `x"y`, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true},
			expectedText: `["a b","x""y")`,
			expectedJSON: `"[\"a b\",\"x\"\"y\")"`,
		},
		{
			name: "multirange",
			cell: pgtype.Multirange[pgtype.Range[interface{}]]{
				{Lower: int32(1), Upper: int32(3), LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true},
				{Lower: int32(5), Upper: int32(8), LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true},
			},
			expectedText: "{[1,3),[5,8)}",
			expectedJSON: `"{[1,3),[5,8)}"`,
		},
		{name: "infinite date", cell: pgtype.Infinity, expectedText: "infinity", expectedJSON: `"infinity"`},
		{name: "negative infinite timestamp", cell: pgtype.NegativeInfinity, expectedText: "-infinity", expectedJSON: `"-infinity"`},
		{name: "point", cell: scanPgText[pgtype.Point](t, "(1.5,-2)"), expectedText: "(1.5,-2)", expectedJSON: `"(1.5,-2)"`},
		{name: "line", cell: scanPgText[pgtype.Line](t, "{1,-1,0}"), expectedText: "{1,-1,0}", expectedJSON: `"{1,-1,0}"`},
		{name: "line segment", cell: scanPgText[pgtype.Lseg](t, "[(0,0),(1,1)]"), expectedText: "[(0,0),(1,1)]", expectedJSON: `"[(0,0),(1,1)]"`},
		{name: "box", cell: scanPgText[pgtype.Box](t, "(1,1),(0,0)"), expectedText: "(1,1),(0,0)", expectedJSON: `"(1,1),(0,0)"`},
		{name: "path", cell: scanPgText[pgtype.Path](t, "[(0,0),(1,1),(2,0)]"), expectedText: "[(0,0),(1,1),(2,0)]", expectedJSON: `"[(0,0),(1,1),(2,0)]"`},
		{name: "polygon", cell: scanPgText[pgtype.Polygon](t, "((0,0),(1,1),(2,0))"), expectedText: "((0,0),(1,1),(2,0))", expectedJSON: `"((0,0),(1,1),(2,0))"`},
		{name: "circle", cell: scanPgText[pgtype.Circle](t, "<(1,2),3>"), expectedText: "<(1,2),3>", expectedJSON: `"\u003c(1,2),3\u003e"`},
		{name: "bit string", cell: scanPgText[pgtype.Bits](t, "10110"), expectedText: "10110", expectedJSON: `"10110"`},
		{name: "tuple id", cell: scanPgText[pgtype.TID](t, "(1,2)"), expectedText: "(1,2)", expectedJSON: `"(1,2)"`},
		{name: "text search vector", cell: scanPgText[pgtype.TSVector](t, "'brown':3 'fox':4"), expectedText: "'brown':3 'fox':4", expectedJSON: `"'brown':3 'fox':4"`},
		{name: "mac address", cell: mac, expectedText: "08:00:2b:01:02:03", expectedJSON: `"08:00:2b:01:02:03"`},
		{name: "json object", cell: map[string]interface{}{"a": 1.0, "b": []interface{}{true, nil}}, expectedText: `{"a":1,"b":[true,null]}`, expectedJSON: `{"a":1,"b":[true,null]}`},
		{name: "json object with markup", cell: map[string]interface{}{"html": "<a>"}, expectedText: `{"html":"<a>"}`, expectedJSON: `{"html":"\u003ca\u003e"}`},
		{name: "float NaN", cell: math.NaN(), expectedText: "NaN", expectedJSON: `"NaN"`},
		{name: "float infinity", cell: math.Inf(1), expectedText: "Infinity", expectedJSON: `"Infinity"`},
		{name: "float32 negative infinity", cell: float32(math.Inf(-1)), expectedText: "-Infinity", expectedJSON: `"-Infinity"`},
		{name: "finite float", cell: 1.25, expectedText: "1.25", expectedJSON: "1.25"},
		{
			name:         "array of uuids and intervals",
			cell:         []interface{}{uuid, pgtype.Interval{Days: 1, Valid: true}},
			expectedText: "[a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 1 day]",
			expectedJSON: `["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","1 day"]`,
		},
		{
			name:         "two-dimensional array",
			cell:         []interface{}{[]interface{}{int32(1), int32(2)}, []interface{}{int32(3), int32(4)}},
			expectedText: "[[1 2] [3 4]]",
			expectedJSON: "[[1,2],[3,4]]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assertQueryCellOutputs(t, tt.cell, tt.expectedText, tt.expectedJSON)
		})
	}
}
