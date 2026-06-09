package sail

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchemaForAsset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		asset string
		want  string
	}{
		{name: "schema.table", asset: "public.messages", want: "public"},
		{name: "deeper paths are not supported (flat only)", asset: "catalog.public.messages", want: ""},
		{name: "bare table uses default database", asset: "messages", want: ""},
		{name: "empty name", asset: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, schemaForAsset(tt.asset))
		})
	}
}
