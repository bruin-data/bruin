package pipeline

import (
	"fmt"
	"regexp"
)

type (
	MaterializerFunc        func(task *Asset, query string) (string, error)
	AssetMaterializationMap map[MaterializationType]map[MaterializationStrategy]MaterializerFunc
)

type Materializer struct {
	MaterializationMap AssetMaterializationMap
	FullRefresh        bool
}

func (m *Materializer) Render(asset *Asset, query string) (string, error) {
	mat := asset.Materialization
	if mat.Type == MaterializationTypeNone {
		return removeComments(query), nil
	}

	strategy := mat.Strategy
	if m.FullRefresh && mat.Type == MaterializationTypeTable && mat.Strategy != MaterializationStrategyNone {
		strategy = MaterializationStrategyCreateReplace
	}

	if matFunc, ok := m.MaterializationMap[mat.Type][strategy]; ok {
		materializedQuery, err := matFunc(asset, query)
		if err != nil {
			return "", err
		}

		return removeComments(materializedQuery), nil
	}

	return "", fmt.Errorf("unsupported materialization type - strategy combination: (`%s` - `%s`)", mat.Type, mat.Strategy)
}

func removeComments(query string) string {
	bytes := []byte(query)
	re := regexp.MustCompile(`/\* *@bruin[\s\w\S]*@bruin *\*/`)
	newBytes := re.ReplaceAll(bytes, []byte(""))
	return string(newBytes)
}
