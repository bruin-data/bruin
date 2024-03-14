package synapse

import (
	"fmt"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

// The other packages all use a materializer that renders the query to a single string. Due to the quirks of synapse
// we need to create a different materializer that returns a slice of strings, since synapse server requires us to send separate batches
// for certain things
type Materializer struct {
	MaterializationMap AssetMaterializationMap
	fullRefresh        bool
}

func (m *Materializer) Render(asset *pipeline.Asset, query string) ([]string, error) {
	mat := asset.Materialization
	if mat.Type == pipeline.MaterializationTypeNone {
		return []string{query}, nil
	}

	strategy := mat.Strategy
	if m.fullRefresh && mat.Type == pipeline.MaterializationTypeTable && mat.Strategy != pipeline.MaterializationStrategyNone {
		strategy = pipeline.MaterializationStrategyCreateReplace
	}

	if matFunc, ok := m.MaterializationMap[mat.Type][strategy]; ok {
		return matFunc(asset, query)
	}

	return []string{}, fmt.Errorf("unsupported materialization type - strategy combination: (`%s` - `%s`)", mat.Type, mat.Strategy)
}

func NewMaterializer() *Materializer {
	return &Materializer{
		MaterializationMap: matMap,
	}
}
