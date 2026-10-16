package duck_test

import (
	"testing"

	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/stretchr/testify/assert"
)

func TestIngestrTypeHints_ColumnHints(t *testing.T) {
	t.Parallel()

	var provider python.IngestrTypeHintProvider = &duck.Client{}
	overlay := python.TypeHintOverlayForConnection(provider)

	cases := []struct {
		declared string
		want     string
	}{
		{"integer", "int32"},
		{"INT", "int32"},
		{"int4", "int32"},
		{"INT32", "int32"},
		{"signed", "int32"},
		{"smallint", "smallint"},
		{"bigint", "bigint"},
		{"int8", "bigint"},
		{"timestamp", "timestamp_ntz"},
		{"DATETIME", "timestamp_ntz"},
		{"timestamp without time zone", "timestamp_ntz"},
		{"timestamptz", "timestamp"},
		{"timestamp with time zone", "timestamp"},
		{"date", "date"},
		{"varchar", "text"},
	}

	for _, tc := range cases {
		t.Run(tc.declared, func(t *testing.T) {
			t.Parallel()

			cols := []pipeline.Column{{Name: "c", Type: tc.declared}}
			assert.Equal(t, "c:"+tc.want, python.ColumnHints(cols, true, overlay, nil))
		})
	}
}
