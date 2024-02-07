package pipeline

import "fmt"

type (
	MaterializerFunc        func(task *Asset, query string) (string, error)
	AssetMaterializationMap map[MaterializationType]map[MaterializationStrategy]MaterializerFunc
)

type Materializer struct {
	MaterializationMap AssetMaterializationMap
}

func (m *Materializer) Render(asset *Asset, query string) (string, error) {
	mat := asset.Materialization
	if mat.Type == MaterializationTypeNone {
		return query, nil
	}

	if matFunc, ok := m.MaterializationMap[mat.Type][mat.Strategy]; ok {
		return matFunc(asset, query)
	}

	return "", fmt.Errorf("unsupported materialization type - strategy combination: (`%s` - `%s`)", mat.Type, mat.Strategy)
}
