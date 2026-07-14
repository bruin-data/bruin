package clickhouse

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngestrTypeHints_ClientImplementsProvider(t *testing.T) {
	t.Parallel()

	var provider python.IngestrTypeHintProvider = &Client{}
	hints := provider.IngestrTypeHints()
	require.NotEmpty(t, hints)
	assert.Equal(t, "timestamp", hints["datetime64"])
	assert.Equal(t, "bigint", hints["uint64"])
	assert.Equal(t, "tinyint", hints["int8"], "ClickHouse Int8 must override PostgreSQL int8→bigint")
	assert.Equal(t, "bigint", hints["time"], "ClickHouse TIME is an Int64 alias")
}

func TestIngestrTypeHints_CoversClickHouseTypes(t *testing.T) {
	t.Parallel()

	hints := IngestrTypeHints()
	cases := []struct {
		declared string
		want     string
	}{
		// integers
		{"Int8", "tinyint"},
		{"Int16", "smallint"},
		{"Int32", "int"},
		{"Int64", "bigint"},
		{"Int128", "bigint"},
		{"Int256", "bigint"},
		{"UInt8", "tinyint"},
		{"UInt16", "int"},
		{"UInt32", "bigint"},
		{"UInt64", "bigint"},
		{"UInt128", "bigint"},
		{"UInt256", "bigint"},
		// floats / decimals
		{"Float32", "double"},
		{"Float64", "double"},
		{"BFloat16", "double"},
		{"Decimal", "decimal"},
		{"Decimal32", "decimal"},
		{"Decimal64", "decimal"},
		{"Decimal128", "decimal"},
		{"Decimal256", "decimal"},
		{"Decimal64(2)", "decimal"},
		{"Decimal(18, 4)", "decimal"},
		// bool / string / uuid
		{"Bool", "bool"},
		{"String", "text"},
		{"FixedString(16)", "text(16)"},
		{"UUID", "text"},
		// date / time
		{"Date", "date"},
		{"Date32", "date"},
		{"DateTime", "timestamp"},
		{"DateTime('UTC')", "timestamp"},
		{"DateTime64", "timestamp"},
		{"DateTime64(3)", "timestamp"},
		{"DateTime64(3, 'UTC')", "timestamp"},
		{"TIME", "bigint"},
		// enums / nested
		{"Enum8('a' = 1, 'b' = 2)", "text"},
		{"Enum16", "text"},
		{"Array(UInt64)", "json"},
		{"Tuple(UInt32, String)", "json"},
		{"Map(String, UInt64)", "json"},
		{"Nested(x UInt32, y String)", "json"},
		{"JSON", "json"},
		{"Dynamic", "json"},
		{"Variant(String, UInt64)", "json"},
		{"Object('json')", "json"},
		{"AggregateFunction(sum, UInt64)", "json"},
		{"SimpleAggregateFunction(sum, UInt64)", "json"},
		// network / geo / intervals
		{"IPv4", "text"},
		{"IPv6", "text"},
		{"Point", "json"},
		{"Ring", "json"},
		{"Polygon", "json"},
		{"MultiPolygon", "json"},
		{"LineString", "json"},
		{"MultiLineString", "json"},
		{"IntervalDay", "interval"},
		{"IntervalSecond", "interval"},
		{"Nothing", "text"},
		// wrappers peel to the inner type
		{"Nullable(UInt64)", "bigint"},
		{"Nullable(DateTime64(3))", "timestamp"},
		{"LowCardinality(String)", "text"},
		{"LowCardinality(Nullable(Int32))", "int"},
		{"Nullable(LowCardinality(UUID))", "text"},
	}

	for _, tc := range cases {
		t.Run(tc.declared, func(t *testing.T) {
			t.Parallel()
			got := python.ColumnHints([]pipeline.Column{{Name: "c", Type: tc.declared}}, false, hints)
			assert.Equal(t, "c:"+tc.want, got)
		})
	}
}

func TestIngestrTypeHints_UsedAsColumnHintOverlay(t *testing.T) {
	t.Parallel()

	cols := []pipeline.Column{
		{Name: "id", Type: "UInt64"},
		{Name: "created_at", Type: "DateTime64(3)"},
		{Name: "payload", Type: "Tuple(String, UInt32)"},
		{Name: "name", Type: "String"},
		{Name: "flag", Type: "Nullable(Bool)"},
	}

	got := python.ColumnHints(cols, false, IngestrTypeHints())
	assert.Equal(t, "id:bigint,created_at:timestamp,payload:json,name:text,flag:bool", got)
}

func TestTypeHintOverlayForConnection_FromClickHouseClient(t *testing.T) {
	t.Parallel()

	overlay := python.TypeHintOverlayForConnection(&Client{})
	require.NotNil(t, overlay)
	assert.Equal(t, IngestrTypeHints(), overlay)

	assert.Nil(t, python.TypeHintOverlayForConnection(nil))
	assert.Nil(t, python.TypeHintOverlayForConnection(struct{}{}))
}
