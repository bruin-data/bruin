package inference

import (
	"os"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/require"
)

func roundTripArrow(t *testing.T, input arrow.RecordBatch, results []string) arrow.RecordBatch { //nolint:ireturn
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "inference-*.arrow")
	require.NoError(t, err)
	values := make([]any, len(results))
	for i, result := range results {
		values[i] = result
	}
	require.NoError(t, writeInferenceArrow(f, input, []outputColumn{{Name: "result", Type: "string"}}, [][]any{values}))
	require.NoError(t, f.Close())

	f, err = os.Open(f.Name())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	reader, err := ipc.NewFileReader(f)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })
	require.Equal(t, 1, reader.NumRecords())
	record, err := reader.RecordBatchAt(0)
	require.NoError(t, err)
	t.Cleanup(record.Release)
	return record
}

func TestRecordFromQueryWriteArrowRoundTripPreservesValues(t *testing.T) {
	t.Parallel()
	stamp := time.Date(2042, 7, 8, 9, 10, 11, 987654321, time.FixedZone("adversarial", 5*60*60+30*60))
	input, err := recordFromQuery(&query.QueryResult{
		Columns:     []string{"big", "amount", "at", "payload", "nil_text", "literal_text"},
		ColumnTypes: []string{"bigint", "decimal(38,18)", "timestamp with time zone", "binary", "string", "string"},
		Rows: [][]interface{}{{
			int64(9007199254740993), "12345678901234567890.123456789012345678", stamp,
			[]byte{0x00, 0xff, 0x01},
			nil, "(null)",
		}},
	}, nil, "")
	require.NoError(t, err)
	defer input.Release()

	record := roundTripArrow(t, input, []string{"ok"})
	require.Equal(t, int64(9007199254740993), record.Column(0).(*array.Int64).Value(0))
	require.Equal(t, "12345678901234567890.123456789012345678", record.Column(1).(*array.Decimal128).Value(0).ToString(18))
	timestamps := record.Column(2).(*array.Timestamp)
	require.Equal(t, arrow.Timestamp(stamp.UnixNano()), timestamps.Value(0))
	require.Equal(t, "UTC", timestamps.DataType().(*arrow.TimestampType).TimeZone)
	require.Equal(t, []byte{0x00, 0xff, 0x01}, record.Column(3).(*array.Binary).Value(0))
	require.True(t, record.Column(4).IsNull(0))
	require.Equal(t, "(null)", record.Column(5).(*array.String).Value(0))
	require.Equal(t, "ok", record.Column(6).(*array.String).Value(0))
}

func TestRecordFromQueryWriteArrowRoundTripPreservesSchemaWithZeroRows(t *testing.T) {
	t.Parallel()
	input, err := recordFromQuery(&query.QueryResult{
		Columns:     []string{"id", "amount"},
		ColumnTypes: []string{"bigint", "decimal(38,18)"},
	}, nil, "")
	require.NoError(t, err)
	defer input.Release()

	record := roundTripArrow(t, input, nil)
	require.Zero(t, record.NumRows())
	require.Equal(t, []string{"id", "amount", "result"}, []string{
		record.Schema().Field(0).Name, record.Schema().Field(1).Name, record.Schema().Field(2).Name,
	})
	require.Equal(t, &arrow.Decimal128Type{Precision: 38, Scale: 18}, record.Schema().Field(1).Type)
}

func TestRecordFromQueryRejectsLossyValuesAndUnknownTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		columnType string
		value      interface{}
	}{
		{name: "unknown type", columnType: "mystery", value: "value"},
		{name: "floating point decimal", columnType: "decimal(38,18)", value: 1.25},
		{name: "decimal fractional scale", columnType: "decimal(5,2)", value: "1.234"},
		{name: "float narrowing", columnType: "float32", value: float64(16777217)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := recordFromQuery(&query.QueryResult{
				Columns: []string{"value"}, ColumnTypes: []string{tt.columnType}, Rows: [][]interface{}{{tt.value}},
			}, nil, "")
			require.Error(t, err)
		})
	}
}

func TestQueryArrowTypeMapsOracleNativeTypes(t *testing.T) {
	t.Parallel()
	dt, err := queryArrowType("VARCHAR2")
	require.NoError(t, err)
	require.Equal(t, arrow.BinaryTypes.String, dt)
	dt, err = queryArrowType("VARCHAR2(100)")
	require.NoError(t, err)
	require.Equal(t, arrow.BinaryTypes.String, dt)
	dt, err = queryArrowType("NUMBER")
	require.NoError(t, err)
	require.Equal(t, arrow.PrimitiveTypes.Int64, dt)
	input, err := recordFromQuery(&query.QueryResult{
		Columns:     []string{"name", "age"},
		ColumnTypes: []string{"VARCHAR2", "NUMBER"},
		Rows:        [][]any{{"jane", int64(30)}},
	}, nil, "")
	require.NoError(t, err)
	defer input.Release()
	require.Equal(t, arrow.BinaryTypes.String, input.Schema().Field(0).Type)
	require.Equal(t, arrow.PrimitiveTypes.Int64, input.Schema().Field(1).Type)
}

func TestBigQueryDriverTypes(t *testing.T) {
	t.Parallel()
	stamp := time.Date(2500, 2, 3, 4, 5, 6, 123456000, time.UTC)
	result := &query.QueryResult{
		Columns:     []string{"id", "score", "at", "local"},
		ColumnTypes: []string{"INTEGER", "FLOAT", "TIMESTAMP", "DATETIME"},
		Rows: [][]any{
			{int64(3000000000), float64(16777217), stamp, civil.DateTimeOf(stamp)},
			{int64(9007199254740993), nil, nil, nil},
		},
	}
	input, err := recordFromQuery(result, nil, "google_cloud_platform")
	require.NoError(t, err)
	defer input.Release()
	record := roundTripArrow(t, input, []string{"a", "b"})
	require.Equal(t, int64(3000000000), record.Column(0).(*array.Int64).Value(0))
	require.Equal(t, int64(9007199254740993), record.Column(0).(*array.Int64).Value(1))
	require.Equal(t, float64(16777217), record.Column(1).(*array.Float64).Value(0)) //nolint:testifylint // Exact preservation, not approximate numerical equality.
	require.Equal(t, arrow.Timestamp(stamp.UnixMicro()), record.Column(2).(*array.Timestamp).Value(0))
	require.Equal(t, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, record.Schema().Field(2).Type)
	require.Equal(t, &arrow.TimestampType{Unit: arrow.Microsecond}, record.Schema().Field(3).Type)
	require.Equal(t, arrow.Timestamp(stamp.UnixMicro()), record.Column(3).(*array.Timestamp).Value(0))
	require.True(t, record.Column(2).IsNull(1))
	result.Rows = nil
	empty, err := recordFromQuery(result, nil, "google_cloud_platform")
	require.NoError(t, err)
	defer empty.Release()
	require.True(t, input.Schema().Equal(empty.Schema()))
	explicit, err := recordFromQuery(result, []pipeline.Column{{Name: "id", Type: "integer"}, {Name: "score", Type: "float32"}}, "google_cloud_platform")
	require.NoError(t, err)
	defer explicit.Release()
	require.Equal(t, arrow.PrimitiveTypes.Int32, explicit.Schema().Field(0).Type)
	require.Equal(t, arrow.PrimitiveTypes.Float32, explicit.Schema().Field(1).Type)
}
