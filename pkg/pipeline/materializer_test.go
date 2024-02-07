package pipeline

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMaterializer_Render(t *testing.T) {
	materializer := Materializer{
		MaterializationMap: make(map[MaterializationType]map[MaterializationStrategy]MaterializerFunc),
	}

	asset := &Asset{
		Materialization: Materialization{
			Type: MaterializationTypeNone,
		},
	}

	query := "SELECT * FROM table"
	expected := "SELECT * FROM table"

	result, err := materializer.Render(asset, query)
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}
